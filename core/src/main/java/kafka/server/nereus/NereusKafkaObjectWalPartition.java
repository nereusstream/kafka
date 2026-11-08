/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server.nereus;

import org.apache.kafka.common.errors.FencedLeaderEpochException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1;
import com.nereusstream.kafka.bookkeeper.broker.KafkaObjectNativeAppendV1;
import com.nereusstream.kafka.bookkeeper.broker.KafkaObjectNativeExtentV1;
import com.nereusstream.kafka.bookkeeper.broker.KafkaObjectNativeFetchV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentCommitCoordinatorV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectCoherentProtocolSnapshotV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectExtentLocatorV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1;
import com.nereusstream.storage.object.control.WalRunHistoricalObjectReaderV1;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.nwg1.Nwg1ObjectReaderV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/** A selected native Object WAL partition and the exact retained sources of its coherent read cut. */
public final class NereusKafkaObjectWalPartition {
    public interface NativeAssignment {
        boolean current();

        <T> T whileCurrent(Callable<T> action) throws Exception;
    }

    @FunctionalInterface
    interface ReadExtent {
        void read(KafkaObjectExtentLocatorV1 locator, Nwg1ObjectReaderV1.VerifiedFrameConsumer consume)
                throws IOException;
    }

    record Source(ReadExtent read, Runnable close) {
        static Source live(WalRunObjectSession run, Nwg1VerificationContextV1 verification) {
            return new Source((locator, consume) -> KafkaObjectNativeExtentV1.read(run, verification, locator, consume),
                    run::close);
        }

        static Source historical(WalRunHistoricalObjectReaderV1 run, Nwg1VerificationContextV1 verification) {
            return new Source((locator, consume) -> KafkaObjectNativeExtentV1.read(run, verification, locator, consume),
                    run::close);
        }
    }

    private final KafkaCoherentCommitCoordinatorV1 coordinator;
    private final KafkaObjectNativeAppendV1 writer;
    private final Map<Sha256Digest, Source> sources;
    private final Executor reads;
    private final NativeAssignment assignment;
    private final LongSupplier clock;
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private CompletableFuture<Void> plannedSeal;
    private CompletableFuture<Void> closing;
    private boolean fenced;
    private int actualOperations;

    NereusKafkaObjectWalPartition(KafkaCoherentCommitCoordinatorV1 coordinator, KafkaObjectNativeAppendV1 writer,
            Map<Sha256Digest, Source> sources, Executor reads, NativeAssignment assignment, LongSupplier clock) {
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.writer = Objects.requireNonNull(writer, "writer");
        this.sources = Map.copyOf(sources);
        this.reads = reads;
        this.assignment = assignment;
        this.clock = clock;
    }

    public KafkaObjectCoherentProtocolSnapshotV1 capture() {
        return coordinator.captureObject();
    }

    public int maximumFrameBytes() {
        return writer.maximumFrameBytes();
    }

    public synchronized KafkaObjectNativeAppendV1.Admission admit(List<Integer> frameBounds) {
        requireCurrent();
        return writer.admit(frameBounds);
    }

    public void append(KafkaObjectNativeAppendV1.Admission admitted, List<KafkaNativeAssignedRecordBatchV1> batches,
            Consumer<KafkaObjectCoherentProtocolSnapshotV1> accept) {
        begin();
        try {
            writer.append(admitted, batches, clock.getAsLong(), accept);
        } finally {
            finished();
        }
    }

    public CompletionStage<KafkaObjectNativeFetchV1.Result> read(
            KafkaObjectCoherentProtocolSnapshotV1 snapshot, long offset, int maximumBytes, int hardMaximumBytes,
            boolean minOneMessage, KafkaReadIsolationV1 isolation) {
        begin();
        var result = new CompletableFuture<KafkaObjectNativeFetchV1.Result>();
        try {
            reads.execute(() -> {
                try {
                    result.complete(KafkaObjectNativeFetchV1.read(snapshot, this::pinExtent,
                            offset, maximumBytes, hardMaximumBytes, minOneMessage, isolation));
                } catch (IOException failure) {
                    result.completeExceptionally(new KafkaStorageException("native Object WAL Fetch failed", failure));
                } catch (RuntimeException | Error failure) {
                    result.completeExceptionally(failure);
                } finally {
                    finished();
                }
            });
        } catch (RejectedExecutionException exhausted) {
            finished();
            result.completeExceptionally(new ThrottlingQuotaExceededException("native Object WAL Fetch capacity exhausted"));
        }
        return result.copy();
    }

    private KafkaObjectNativeFetchV1.PinnedExtent pinExtent(KafkaObjectExtentLocatorV1 locator) {
        var source = sources.get(locator.extent().walRunRootSha());
        if (source == null) throw new KafkaStorageException("native Object WAL extent has no retained Root source");
        // A plan already holds the complete source set until actual task termination. This per-extent reference
        // cannot escape its synchronous read callback or close the backing session underneath another plan.
        return new KafkaObjectNativeFetchV1.PinnedExtent() {
            private boolean closed;

            @Override
            public void read(Nwg1ObjectReaderV1.VerifiedFrameConsumer consume) throws IOException {
                if (closed) throw new IOException("native Object WAL extent pin is closed");
                source.read().read(locator, consume);
            }

            @Override
            public void close() {
                closed = true;
            }
        };
    }

    public void fence() {
        boolean done;
        synchronized (this) {
            fenced = true;
            writer.fence();
            done = actualOperations == 0;
        }
        if (done) drained.complete(null);
    }

    private synchronized void begin() {
        requireCurrent();
        actualOperations = Math.incrementExact(actualOperations);
    }

    private void requireCurrent() {
        if (fenced || !assignment.current()) {
            throw new FencedLeaderEpochException("native Object WAL partition is fenced");
        }
    }

    private void finished() {
        boolean done;
        synchronized (this) {
            actualOperations = Math.decrementExact(actualOperations);
            done = fenced && actualOperations == 0;
        }
        if (done) drained.complete(null);
    }

    synchronized CompletableFuture<Void> sealBeforeHandoff(Executor recoveryExecutor) {
        if (plannedSeal == null) {
            if (closing != null) {
                return CompletableFuture.failedFuture(new FencedLeaderEpochException("Object WAL is already closing"));
            }
            fence();
            plannedSeal = drained.thenRunAsync(() -> {
                try {
                    assignment.whileCurrent(writer::sealDrained);
                } catch (Exception failure) {
                    throw new CompletionException(failure);
                }
            }, recoveryExecutor);
        }
        return plannedSeal.copy();
    }

    public synchronized CompletableFuture<Void> closeAsync() {
        if (closing == null || closing.isCompletedExceptionally()) {
            fence();
            // A timed-out caller still leaves the actual seal attempt owning its Provider and KMS leases.
            var sealFinished = plannedSeal == null ? drained : plannedSeal.handle((ignored, failure) -> null);
            closing = sealFinished.thenRun(this::closeSources);
        }
        return closing.copy();
    }

    private void closeSources() {
        writer.closeCheckpointAfterDrain();
        RuntimeException failure = null;
        for (var source : sources.values()) {
            try {
                source.close().run();
            } catch (RuntimeException closeFailure) {
                if (failure == null) failure = closeFailure;
                else failure.addSuppressed(closeFailure);
            }
        }
        if (failure != null) throw failure;
    }
}
