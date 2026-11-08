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
package kafka.log.nereus;

import kafka.server.nereus.NereusKafkaObjectWalPartition;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.message.FetchResponseData.AbortedTransaction;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.AsyncOffsetReader;
import org.apache.kafka.storage.internals.log.BrokerStorageManagedLog;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.LogReadInfo;
import org.apache.kafka.storage.internals.log.LogSegments;
import org.apache.kafka.storage.internals.log.OffsetResultHolder;
import org.apache.kafka.storage.internals.log.PartitionLeaderAuthority;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.RequiredAcksAwareAppend;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.internals.log.VerificationGuard;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaRawAssignedRecordBatchFactsV1;
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1;
import com.nereusstream.kafka.bookkeeper.broker.KafkaObjectNativeAppendV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaBatchDuplicateIdentityV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCommittedProducerStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaLeaderEpochIndexV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedAppendOutcomeV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionProtocolStateV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadBatchV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadCursorV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadOutcomeV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadResultV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperSequentialReadRequestV1;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/** Native validation and producer state over the one shared BK commit and closed-history recovery path. */
public final class NereusUnifiedLog extends UnifiedLog implements RequiredAcksAwareAppend, BrokerStorageManagedLog {
    private final Object guard = new Object();
    private final TopicIdPartition identity;
    private final Duration appendTimeout;
    private final Duration fetchTimeout;
    private final int hardMaxFetchBytes;
    private final NereusListOffsetsScanConfig timestampScan;
    private final NereusProducerStateManager producers;
    private final ThreadLocal<KafkaBookKeeperPartitionV1.AdmittedAppend> admission = new ThreadLocal<>();
    private final ThreadLocal<KafkaObjectNativeAppendV1.Admission> objectAdmission = new ThreadLocal<>();
    private KafkaBookKeeperPartitionV1 storage;
    private NereusKafkaObjectWalPartition objectStorage;
    // One disposable position per log. The reader checks it against a fresh snapshot on every use.
    private final AtomicReference<Optional<KafkaBookKeeperReadCursorV1>> nextFetchCursor =
            new AtomicReference<>(Optional.empty());
    private int publishedEpoch = -1;
    private java.util.function.BooleanSupplier currentOwner = () -> false;
    private long configMetadataOffset = -1;
    private long m5PolicyMetadataOffset = -1;
    private java.util.function.BiConsumer<LogConfig, Long> m5PolicyWriter;
    private final Object m5PolicyGuard = new Object();
    private final ThreadLocal<Boolean> physicallyDispatched = new ThreadLocal<>();

    private NereusUnifiedLog(
            Parts parts,
            org.apache.kafka.storage.log.metrics.BrokerTopicStats statistics,
            int expirationInterval,
            Uuid topicId,
            TopicIdPartition identity,
            Duration appendTimeout,
            Duration fetchTimeout,
            int hardMaxFetchBytes,
            NereusListOffsetsScanConfig timestampScan,
            LogOffsetsListener listener)
            throws IOException {
        super(
                0,
                parts.local(),
                statistics,
                expirationInterval,
                parts.epochs(),
                parts.producers(),
                Optional.of(topicId),
                false,
                listener);
        this.identity = identity;
        this.appendTimeout = appendTimeout;
        this.fetchTimeout = fetchTimeout;
        this.hardMaxFetchBytes = hardMaxFetchBytes;
        this.timestampScan = java.util.Objects.requireNonNull(timestampScan, "timestampScan");
        producers = parts.producers();
        parts.local().bindStableAppend(this::appendStable);
    }

    public static NereusUnifiedLog create(
            File dir,
            LogConfig config,
            Scheduler scheduler,
            org.apache.kafka.storage.log.metrics.BrokerTopicStats statistics,
            Time time,
            int maximumTransactionTimeout,
            ProducerStateManagerConfig producerConfig,
            int expirationInterval,
            LogDirFailureChannel failures,
            Uuid topicId,
            TopicIdPartition identity,
            Duration appendTimeout,
            Duration fetchTimeout,
            int hardMaxFetchBytes,
            NereusListOffsetsScanConfig timestampScan,
            LogOffsetsListener listener)
            throws IOException {
        if (topicId.equals(Uuid.ZERO_UUID) || !identity.topicId().equals(topicId)) {
            throw new IllegalArgumentException("shared BK log requires the exact nonzero native topic ID");
        }
        if (appendTimeout.isNegative()
                || appendTimeout.isZero()
                || fetchTimeout.isNegative()
                || fetchTimeout.isZero()
                || hardMaxFetchBytes <= 0) {
            throw new IllegalArgumentException("shared BK log bounds must be positive");
        }
        return new NereusUnifiedLog(
                parts(dir, config, scheduler, time, maximumTransactionTimeout, producerConfig, failures),
                statistics,
                expirationInterval,
                topicId,
                identity,
                appendTimeout,
                fetchTimeout,
                hardMaxFetchBytes,
                timestampScan,
                listener);
    }

    /** Called under the current lifecycle slot guard after asynchronous checkpoint and tail recovery, before request readiness. */
    public NereusKafkaRecoveredState installRecoveredState(
            int epoch, KafkaBookKeeperPartitionV1 candidate, java.util.function.BooleanSupplier currentOwner)
            throws IOException {
        synchronized (guard) {
            requireUnowned();
            var snapshot = candidate.capture();
            var state = installRecoveredProtocol(epoch, snapshot.root(), snapshot.committedProducerState(),
                    snapshot.transactionState(), snapshot.leaderEpochIndex());
            this.currentOwner = currentOwner;
            storage = candidate;
            publishedEpoch = epoch;
            return state;
        }
    }

    public NereusKafkaRecoveredState installRecoveredObjectState(
            int epoch, NereusKafkaObjectWalPartition candidate, java.util.function.BooleanSupplier currentOwner)
            throws IOException {
        synchronized (guard) {
            requireUnowned();
            var snapshot = candidate.capture();
            var state = installRecoveredProtocol(epoch, snapshot.root(), snapshot.committedProducerState(),
                    snapshot.transactionState(), snapshot.leaderEpochIndex());
            this.currentOwner = currentOwner;
            objectStorage = candidate;
            publishedEpoch = epoch;
            return state;
        }
    }

    private void requireUnowned() {
        if (storage != null || objectStorage != null) {
            throw new KafkaStorageException("shared Kafka log already has a published owner");
        }
    }

    private NereusKafkaRecoveredState installRecoveredProtocol(int epoch, KafkaPartitionProtocolStateV1 root,
            KafkaCommittedProducerStateV1 producerState, KafkaTransactionStateV1 transactionState,
            KafkaLeaderEpochIndexV1 leaderEpochs) throws IOException {
        if (root.fence().kafkaLeaderEpoch() != epoch || root.frontiers().trimStartOffset() != 0) {
            throw new KafkaStorageException("native recovered partition has a foreign epoch or unsupported prefix");
        }
        var state = new NereusKafkaRecoveredState(identity.topicPartition(), identity.topicId(), epoch, root);
        long end = root.frontiers().durableEndOffset();
        super.truncateFullyAndStartAt(end, Optional.of(0L));
        producers.loadSharedState(root, producerState, transactionState);
        if (producers.mapEndOffset() != end) {
            throw new KafkaStorageException("native producer replay does not cover the shared durable prefix");
        }
        leaderEpochs.startOffsets().forEach(super::assignEpochStartOffset);
        super.assignEpochStartOffset(epoch, end);
        super.updateHighWatermark(end);
        if (lastStableOffset() != root.frontiers().lastStableOffset()) {
            throw new KafkaStorageException("native transaction replay differs from the shared LSO");
        }
        return state;
    }

    public void removeStorage(int epoch, KafkaBookKeeperPartitionV1 expected) {
        synchronized (guard) {
            if (publishedEpoch == epoch && storage != null && (expected == null || storage == expected)) {
                storage = null;
                nextFetchCursor.set(Optional.empty());
                publishedEpoch = -1;
                m5PolicyWriter = null;
            }
        }
    }

    public void removeObjectStorage(int epoch, NereusKafkaObjectWalPartition expected) {
        synchronized (guard) {
            if (publishedEpoch == epoch && objectStorage == expected) {
                objectStorage = null;
                publishedEpoch = -1;
            }
        }
    }

    public boolean nereusWritable(int epoch) {
        synchronized (guard) {
            return (storage != null || objectStorage != null) && publishedEpoch == epoch;
        }
    }

    public TopicIdPartition nereusIdentity() {
        return identity;
    }

    @Override
    public LogAppendInfo appendAsLeader(
            MemoryRecords records,
            int epoch,
            AppendOrigin origin,
            RequestLocal requestLocal,
            VerificationGuard verificationGuard,
            short transactionVersion) {
        return appendAsLeader(records, epoch, origin, requestLocal, verificationGuard, transactionVersion, (short) 1);
    }

    @Override
    public LogAppendInfo appendAsLeader(
            MemoryRecords records,
            int epoch,
            AppendOrigin origin,
            RequestLocal requestLocal,
            VerificationGuard verificationGuard,
            short transactionVersion,
            short requiredAcks) {
        if (origin != AppendOrigin.CLIENT && origin != AppendOrigin.COORDINATOR) {
            throw new KafkaStorageException("shared BK accepts only native client/coordinator appends");
        }
        if (requiredAcks != 0 && requiredAcks != 1 && requiredAcks != -1)
            throw new IllegalArgumentException("invalid acks");
        synchronized (guard) {
            if (objectStorage != null) {
                return appendObjectAsLeader(records, epoch, origin, requestLocal, verificationGuard, transactionVersion);
            }
            var exact = requireStorage(epoch);
            var lengths = new ArrayList<Integer>();
            boolean duplicate = origin == AppendOrigin.CLIENT;
            for (var batch : records.batches()) {
                batch.ensureValid();
                if (batch.magic() != RecordBatch.MAGIC_VALUE_V2) {
                    throw new org.apache.kafka.common.errors.UnsupportedForMessageFormatException(
                            "shared BK requires magic-v2");
                }
                lengths.add(config().maxMessageSize());
                duplicate = duplicate && isDurableDuplicate(exact.capture().committedProducerState(), batch);
            }
            if (lengths.isEmpty())
                return super.appendAsLeader(
                        records, epoch, origin, requestLocal, verificationGuard, transactionVersion);
            if (admission.get() != null) throw new IllegalStateException("nested shared BK append");
            try (var reserved = duplicate ? null : admitStorage(exact, lengths)) {
                if (reserved != null) admission.set(reserved);
                LogAppendInfo result = super.appendAsLeader(
                        records, epoch, origin, requestLocal, verificationGuard, transactionVersion);
                if (reserved != null) {
                    var published = exact.capture();
                    long end = published.root().frontiers().durableEndOffset();
                    super.updateHighWatermark(end);
                    if (end != logEndOffset()
                            || lastStableOffset()
                                    != published.root().frontiers().lastStableOffset()) {
                        throw new KafkaStorageException("native append differs from coherent BK publication");
                    }
                }
                return result;
            } catch (IOException failure) {
                exact.fence();
                throw new KafkaStorageException("shared BK native frontier publication failed", failure);
            } catch (RuntimeException | Error failure) {
                // Native validation before any stable append leaves the old published prefix intact. A physical
                // failure fences inside the pipeline; an uncertain wait must also prevent further native allocation.
                if (Boolean.TRUE.equals(physicallyDispatched.get())) exact.fence();
                throw failure;
            } finally {
                admission.remove();
                physicallyDispatched.remove();
            }
        }
    }

    private boolean isDurableDuplicate(KafkaCommittedProducerStateV1 selected, RecordBatch batch) {
        return batch.hasProducerId() && !batch.isControlBatch() && batch.baseSequence() >= 0
                && producers.activeProducers().containsKey(batch.producerId())
                && selected.findDuplicate(new KafkaBatchDuplicateIdentityV1(
                        batch.producerId(), batch.producerEpoch(), batch.baseSequence(), batch.lastSequence()))
                        .isPresent();
    }

    private LogAppendInfo appendObjectAsLeader(MemoryRecords records, int epoch, AppendOrigin origin,
            RequestLocal requestLocal, VerificationGuard verificationGuard, short transactionVersion) {
        var exact = requireObjectStorage(epoch);
        var selected = exact.capture();
        var lengths = new ArrayList<Integer>();
        boolean duplicate = origin == AppendOrigin.CLIENT;
        boolean exceedsFrameBound = false;
        int frameBound = Math.min(config().maxMessageSize(), exact.maximumFrameBytes());
        for (var batch : records.batches()) {
            batch.ensureValid();
            if (batch.magic() != RecordBatch.MAGIC_VALUE_V2) {
                throw new org.apache.kafka.common.errors.UnsupportedForMessageFormatException(
                        "native Object WAL requires magic-v2");
            }
            exceedsFrameBound |= batch.sizeInBytes() > frameBound;
            lengths.add(frameBound);
            duplicate = duplicate && isDurableDuplicate(selected.committedProducerState(), batch);
        }
        if (lengths.isEmpty() || duplicate) {
            return super.appendAsLeader(records, epoch, origin, requestLocal, verificationGuard, transactionVersion);
        }
        if (exceedsFrameBound) {
            throw new RecordTooLargeException("native Object WAL frame exceeds its admitted byte limit");
        }
        if (objectAdmission.get() != null) throw new IllegalStateException("nested native Object WAL append");
        try (var reserved = exact.admit(lengths)) {
            objectAdmission.set(reserved);
            var result = super.appendAsLeader(records, epoch, origin, requestLocal, verificationGuard, transactionVersion);
            var root = exact.capture().root();
            long end = root.frontiers().durableEndOffset();
            super.updateHighWatermark(end);
            if (end != logEndOffset() || lastStableOffset() != root.frontiers().lastStableOffset()) {
                throw new KafkaStorageException("native append differs from the coherent Object WAL publication");
            }
            return result;
        } catch (IOException failure) {
            exact.fence();
            throw new KafkaStorageException("native Object WAL frontier publication failed", failure);
        } catch (RuntimeException | Error failure) {
            if (Boolean.TRUE.equals(physicallyDispatched.get())) exact.fence();
            throw failure;
        } finally {
            objectAdmission.remove();
            physicallyDispatched.remove();
        }
    }

    private static KafkaBookKeeperPartitionV1.AdmittedAppend admitStorage(
            KafkaBookKeeperPartitionV1 exact, List<Integer> lengths) {
        try {
            return exact.admit(lengths);
        } catch (IllegalStateException failure) {
            // A fenced run (including an unresolved write) or an in-progress rollover cannot admit offsets.
            throw NereusKafkaExceptionMapper.map(failure);
        }
    }

    private void appendStable(long lastOffset, MemoryRecords records) {
        if (objectAdmission.get() != null) {
            var batches = assignedBatches(records);
            physicallyDispatched.set(true);
            requireObjectStorage(publishedEpoch).append(objectAdmission.get(), batches, snapshot -> {
                if (snapshot.root().frontiers().durableEndOffset() != lastOffset + 1) {
                    throw new KafkaStorageException("Object WAL append did not publish the exact native range");
                }
            });
            return;
        }
        var reserved = admission.get();
        if (reserved == null) throw new KafkaStorageException("native append has no pre-offset BK admission");
        var batches = assignedBatches(records);
        physicallyDispatched.set(true);
        var result = await(storage.appendAssigned(reserved, batches), appendTimeout);
        if (result.outcome() != KafkaOrderedAppendOutcomeV1.COMMITTED_ORDERED
                || result.endOffsetExclusive().orElseThrow() != lastOffset + 1) {
            storage.fence();
            throw new KafkaStorageException("shared BK append did not commit the exact native batch range");
        }
    }

    private static List<KafkaNativeAssignedRecordBatchV1> assignedBatches(MemoryRecords records) {
        var batches = new ArrayList<KafkaNativeAssignedRecordBatchV1>();
        for (var batch : records.batches()) {
            var bytes = ByteBuffer.allocate(batch.sizeInBytes());
            batch.writeTo(bytes);
            var raw = CanonicalBytes.copyOf(bytes.array());
            batches.add(KafkaNativeAssignedRecordBatchV1.validate(KafkaRawAssignedRecordBatchFactsV1.parse(raw)));
        }
        return batches;
    }

    @Override
    public LogAppendInfo appendAsFollower(MemoryRecords records, int epoch) {
        throw new KafkaStorageException("RF=1 shared BK has no native follower append path");
    }

    private static KafkaReadIsolationV1 readIsolation(FetchIsolation isolation) {
        return switch (isolation) {
            case LOG_END -> KafkaReadIsolationV1.REPLICA;
            case HIGH_WATERMARK -> KafkaReadIsolationV1.READ_UNCOMMITTED;
            case TXN_COMMITTED -> KafkaReadIsolationV1.READ_COMMITTED;
        };
    }

    @Override
    public LogReadInfo withFetchState(Supplier<CompletionStage<LogReadInfo>> read) {
        final CompletionStage<LogReadInfo> planned;
        synchronized (guard) {
            planned = read.get();
        }
        return awaitFetch(planned);
    }

    @Override
    public FetchDataInfo read(long offset, int maximumBytes, FetchIsolation isolation, boolean minOneMessage) {
        return awaitFetch(planFetch(offset, maximumBytes, isolation, minOneMessage));
    }

    @Override
    public CompletionStage<FetchDataInfo> planFetch(
            long offset, int maximumBytes, FetchIsolation isolation, boolean minOneMessage) {
        long start = System.nanoTime();
        synchronized (guard) {
            if (objectStorage != null) {
                return planObjectFetch(offset, maximumBytes, isolation, minOneMessage);
            }
            var exact = requireStorage(publishedEpoch);
            var cursor = nextFetchCursor.getAndSet(Optional.empty());
            var mode = readIsolation(isolation);
            var snapshot = exact.capture();
            if (offset < snapshot.root().frontiers().trimStartOffset()
                    || offset > snapshot.root().frontiers().readableEndOffset()) {
                throw new OffsetOutOfRangeException("shared BK Fetch offset is outside the captured range");
            }
            if (maximumBytes <= 0 || offset >= snapshot.root().readUpperBound(mode)) {
                return CompletableFuture.completedFuture(new FetchDataInfo(
                        new LogOffsetMetadata(offset),
                        MemoryRecords.EMPTY,
                        false,
                        isolation == FetchIsolation.TXN_COMMITTED ? Optional.of(List.of()) : Optional.empty()));
            }
            KafkaBookKeeperPartitionV1.PlannedRead planned;
            try {
                planned = exact.planRead(new KafkaBookKeeperSequentialReadRequestV1(
                        offset, mode, Math.min(maximumBytes, hardMaxFetchBytes), cursor));
                var pinned = awaitFetch(planned.pinned());
                if (!pinned.root().frontiers().equals(snapshot.root().frontiers())
                        || !pinned.root().fence().equals(snapshot.root().fence())) {
                    throw new KafkaStorageException("native Fetch pinned a different frontier or Owner");
                }
            } catch (IllegalStateException failure) {
                throw NereusKafkaExceptionMapper.map(failure);
            }
            long remaining = Math.max(0, fetchTimeout.toNanos() - (System.nanoTime() - start));
            // Timeout applies only to this observer. The accepted read still owns its source and pin until drain.
            return planned.result().thenApply(result -> fetchData(result, offset, maximumBytes, isolation, minOneMessage))
                    .toCompletableFuture().orTimeout(remaining, TimeUnit.NANOSECONDS);
        }
    }

    private CompletionStage<FetchDataInfo> planObjectFetch(
            long offset, int maximumBytes, FetchIsolation isolation, boolean minOneMessage) {
        var exact = requireObjectStorage(publishedEpoch);
        var snapshot = exact.capture();
        if (offset < snapshot.root().frontiers().trimStartOffset()
                || offset > snapshot.root().frontiers().readableEndOffset()) {
            throw new OffsetOutOfRangeException("Object WAL Fetch is outside its captured range");
        }
        return exact.read(snapshot, offset, Math.max(0, maximumBytes), hardMaxFetchBytes, minOneMessage,
                        readIsolation(isolation))
                .thenApply(read -> {
                    var buffer = ByteBuffer.allocate(read.bytes());
                    read.batches().forEach(batch -> buffer.put(batch.rawAssignedBytes().toByteArray()));
                    buffer.flip();
                    var aborted = isolation == FetchIsolation.TXN_COMMITTED
                            ? Optional.of(read.abortedTransactions().stream()
                                    .map(txn -> new AbortedTransaction().setProducerId(txn.producerId())
                                            .setFirstOffset(txn.firstOffset())).toList())
                            : Optional.<List<AbortedTransaction>>empty();
                    long base = read.batches().isEmpty() ? offset : read.batches().get(0).startOffset();
                    return new FetchDataInfo(new LogOffsetMetadata(base, 0, 0), MemoryRecords.readableRecords(buffer),
                            false, aborted);
                }).toCompletableFuture().orTimeout(fetchTimeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    private FetchDataInfo fetchData(KafkaBookKeeperReadResultV1 result, long offset, int maximumBytes,
            FetchIsolation isolation, boolean minOneMessage) {
        if (result.outcome() != KafkaBookKeeperReadOutcomeV1.FOUND) {
            if (result.outcome() == KafkaBookKeeperReadOutcomeV1.END_OF_SNAPSHOT) {
                return new FetchDataInfo(new LogOffsetMetadata(offset), MemoryRecords.EMPTY);
            }
            throw new KafkaStorageException("shared BK Fetch failed: " + result.outcome());
        }
        int bytes = result.batches().stream()
                .mapToInt(batch -> batch.rawAssignedRecordBatch().length())
                .sum();
        if (bytes > hardMaxFetchBytes)
            throw new KafkaStorageException("shared BK Fetch exceeds the hard response cap");
        if (bytes > maximumBytes && !minOneMessage)
            return new FetchDataInfo(new LogOffsetMetadata(offset), MemoryRecords.EMPTY);
        var buffer = ByteBuffer.allocate(bytes);
        result.batches().forEach(batch -> buffer.put(batch.rawAssignedRecordBatch().toByteArray()));
        buffer.flip();
        var aborted = isolation == FetchIsolation.TXN_COMMITTED
                ? Optional.of(result.abortedTransactions().stream()
                        .map(transaction -> new org.apache.kafka.common.message.FetchResponseData.AbortedTransaction()
                                .setProducerId(transaction.producerId()).setFirstOffset(transaction.firstOffset()))
                        .toList())
                : Optional.<List<org.apache.kafka.common.message.FetchResponseData.AbortedTransaction>>empty();
        // Completion runs on the read owner's event loop. A concurrent Fetch may be pinning under the log guard.
        nextFetchCursor.set(result.nextCursor());
        return new FetchDataInfo(new LogOffsetMetadata(result.batches().get(0).startOffset(), 0, 0),
                MemoryRecords.readableRecords(buffer), false, aborted);
    }

    private <T> T awaitFetch(CompletionStage<T> stage) {
        try {
            return await(stage, fetchTimeout);
        } catch (IllegalStateException failure) {
            throw NereusKafkaExceptionMapper.map(failure);
        }
    }

    @Override
    public LogOffsetMetadata maybeConvertToOffsetMetadata(long offset) {
        return new LogOffsetMetadata(offset, 0, 0);
    }

    public CompletionStage<Optional<FileRecords.TimestampAndOffset>> lookupTimestamp(long timestamp, int epoch) {
        synchronized (guard) {
            if (objectStorage != null) {
                return lookupObjectTimestamp(timestamp, epoch);
            }
            var exact = requireStorage(epoch);
            var snapshot = exact.capture();
            if (timestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP
                    || timestamp == ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP) {
                return java.util.concurrent.CompletableFuture.completedFuture(
                        Optional.of(new FileRecords.TimestampAndOffset(
                                RecordBatch.NO_TIMESTAMP, logStartOffset(), Optional.empty())));
            }
            if (timestamp == ListOffsetsRequest.LATEST_TIMESTAMP) {
                return java.util.concurrent.CompletableFuture.completedFuture(
                        Optional.of(new FileRecords.TimestampAndOffset(
                                RecordBatch.NO_TIMESTAMP,
                                snapshot.root().frontiers().highWatermark(),
                                Optional.of(epoch))));
            }
            if (timestamp < 0 && timestamp != ListOffsetsRequest.MAX_TIMESTAMP) {
                return java.util.concurrent.CompletableFuture.completedFuture(Optional.empty());
            }
            return exact.read(new KafkaBookKeeperSequentialReadRequestV1(
                            snapshot.root().frontiers().trimStartOffset(),
                            KafkaReadIsolationV1.READ_UNCOMMITTED,
                            timestampScan.maxScanBytes(),
                            Optional.empty()))
                    .toCompletableFuture()
                    .orTimeout(timestampScan.timeout().toMillis(), TimeUnit.MILLISECONDS)
                    .thenApply(result -> {
                        synchronized (guard) {
                            if (requireStorage(epoch) != exact)
                                throw new KafkaStorageException("timestamp scan owner changed");
                        }
                        if (result.outcome() == KafkaBookKeeperReadOutcomeV1.END_OF_SNAPSHOT) return Optional.empty();
                        if (result.outcome() != KafkaBookKeeperReadOutcomeV1.FOUND)
                            throw new KafkaStorageException("shared BK timestamp scan failed");
                        return scanTimestamp(
                                result.batches(),
                                snapshot.root().frontiers().highWatermark(),
                                timestamp,
                                timestampScan);
                    });
        }
    }

    private CompletionStage<Optional<FileRecords.TimestampAndOffset>> lookupObjectTimestamp(long timestamp, int epoch) {
        var exact = requireObjectStorage(epoch);
        var snapshot = exact.capture();
        var frontiers = snapshot.root().frontiers();
        if (timestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP || timestamp == ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP) {
            return CompletableFuture.completedFuture(Optional.of(new FileRecords.TimestampAndOffset(
                    RecordBatch.NO_TIMESTAMP, frontiers.trimStartOffset(), Optional.empty())));
        }
        if (timestamp == ListOffsetsRequest.LATEST_TIMESTAMP) {
            return CompletableFuture.completedFuture(Optional.of(new FileRecords.TimestampAndOffset(
                    RecordBatch.NO_TIMESTAMP, frontiers.highWatermark(), Optional.of(epoch))));
        }
        if (timestamp < 0 && timestamp != ListOffsetsRequest.MAX_TIMESTAMP) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        int limit = (int) Math.min(timestampScan.maxScanBytes(), Integer.MAX_VALUE);
        return exact.read(snapshot, frontiers.trimStartOffset(), limit, limit, false, KafkaReadIsolationV1.READ_UNCOMMITTED)
                .toCompletableFuture().orTimeout(timestampScan.timeout().toMillis(), TimeUnit.MILLISECONDS)
                .thenApply(result -> {
                    synchronized (guard) {
                        if (requireObjectStorage(epoch) != exact) {
                            throw new KafkaStorageException("Object WAL timestamp scan owner changed");
                        }
                    }
                    if (frontiers.trimStartOffset() == frontiers.highWatermark()) return Optional.empty();
                    var batches = result.batches().stream().map(batch -> new KafkaBookKeeperReadBatchV1(
                            batch.startOffset(), batch.endOffsetExclusive(), OptionalLong.empty(), batch.rawAssignedBytes()))
                            .toList();
                    return scanTimestamp(batches, frontiers.highWatermark(), timestamp, timestampScan);
                });
    }

    static Optional<FileRecords.TimestampAndOffset> scanTimestamp(
            List<KafkaBookKeeperReadBatchV1> batches,
            long upperBound,
            long timestamp,
            NereusListOffsetsScanConfig limits) {
        FileRecords.TimestampAndOffset found = null;
        long scannedRecords = 0;
        long scannedBytes = 0;
        long scannedThrough = 0;
        for (var raw : batches) {
            if (raw.startOffset() >= upperBound) break;
            scannedBytes =
                    Math.addExact(scannedBytes, raw.rawAssignedRecordBatch().length());
            if (scannedBytes > limits.maxScanBytes()) throw scanLimitExceeded();
            var records = MemoryRecords.readableRecords(
                    ByteBuffer.wrap(raw.rawAssignedRecordBatch().toByteArray()));
            for (var batch : records.batches()) {
                for (var record : batch) {
                    if (record.offset() >= upperBound) break;
                    if (++scannedRecords > limits.maxScanRecords()) throw scanLimitExceeded();
                    if (timestamp != ListOffsetsRequest.MAX_TIMESTAMP && record.timestamp() >= timestamp) {
                        return Optional.of(new FileRecords.TimestampAndOffset(
                                record.timestamp(), record.offset(), Optional.of(batch.partitionLeaderEpoch())));
                    }
                    if (timestamp == ListOffsetsRequest.MAX_TIMESTAMP
                            && (found == null || record.timestamp() > found.timestamp)) {
                        found = new FileRecords.TimestampAndOffset(
                                record.timestamp(), record.offset(), Optional.of(batch.partitionLeaderEpoch()));
                    }
                }
            }
            scannedThrough = raw.endOffsetExclusive();
        }
        if (scannedThrough < upperBound) throw scanLimitExceeded();
        return Optional.ofNullable(found);
    }

    private static org.apache.kafka.common.errors.ThrottlingQuotaExceededException scanLimitExceeded() {
        return new org.apache.kafka.common.errors.ThrottlingQuotaExceededException(
                "shared BK timestamp scan exceeds its bounded prefix");
    }

    @Override
    public OffsetResultHolder fetchOffsetByTimestamp(long timestamp, Optional<AsyncOffsetReader> remote) {
        return new OffsetResultHolder(await(lookupTimestamp(timestamp, publishedEpoch), fetchTimeout));
    }

    /** Policy and source state captured under the same native append/configuration guard. */
    public record NativeM5InputCut(
            KafkaBookKeeperPartitionV1.NativeInputSources inputs, LogConfig config, long policyMetadataOffset) { }

    public NativeM5InputCut captureM5InputCut(
            int epoch, KafkaBookKeeperPartitionV1 expected, LogConfig imageConfig, long metadataOffset) {
        synchronized (guard) {
            if (requireStorage(epoch) != expected || metadataOffset < 0)
                throw new KafkaStorageException("native M5 policy belongs to a foreign owner or metadata image");
            var applied = config();
            if (!applied.values().equals(imageConfig.values()))
                throw new KafkaStorageException("native M5 topic policy has not reached the current metadata image");
            // Initial shells and unversioned default-config updates have no topic-config callback offset.
            // Their effective values must first match this actual KRaft image and broker defaults.
            if (configMetadataOffset > metadataOffset)
                throw new KafkaStorageException("native M5 configuration is newer than its metadata image");
            if (m5PolicyMetadataOffset < 0) m5PolicyMetadataOffset = metadataOffset;
            return new NativeM5InputCut(expected.nativeInputSources(), applied, m5PolicyMetadataOffset);
        }
    }

    /** Installs the original native owner's policy writer before request readiness. */
    public void installM5PolicyWriter(int epoch, KafkaBookKeeperPartitionV1 expected,
            java.util.function.BiConsumer<LogConfig, Long> writer) {
        synchronized (m5PolicyGuard) {
            final LogConfig current;
            synchronized (guard) {
                if (requireStorage(epoch) != expected) throw new KafkaStorageException("native M5 policy writer has another owner");
                current = config();
            }
            writer.accept(current, -1L);
            synchronized (guard) {
                if (requireStorage(epoch) != expected) throw new KafkaStorageException("native M5 policy writer owner changed");
                m5PolicyWriter = java.util.Objects.requireNonNull(writer);
            }
        }
    }

    @Override
    public LogConfig updateConfig(LogConfig config) {
        return updateNativeM5Config(config, -1);
    }

    @Override
    public LogConfig updateConfigAtMetadataOffset(LogConfig config, long metadataOffset) {
        if (metadataOffset < 0) throw new KafkaStorageException("negative metadata config update");
        return updateNativeM5Config(config, metadataOffset);
    }

    private LogConfig updateNativeM5Config(LogConfig config, long metadataOffset) {
        synchronized (m5PolicyGuard) {
            final java.util.function.BiConsumer<LogConfig, Long> writer;
            synchronized (guard) {
                if (metadataOffset >= 0 && metadataOffset < configMetadataOffset)
                    throw new KafkaStorageException("stale metadata config update");
                writer = m5PolicyWriter;
            }
            // Accepted scans may still need the log guard while this callback drains them.
            if (writer != null) writer.accept(config, metadataOffset);
            synchronized (guard) {
                if (writer != m5PolicyWriter) throw new KafkaStorageException("native M5 policy writer owner changed");
                var previous = super.updateConfig(config);
                configMetadataOffset = metadataOffset;
                m5PolicyMetadataOffset = -1;
                return previous;
            }
        }
    }

    @Override
    public long deleteRecords(int epoch, long offset, PartitionLeaderAuthority authority) {
        throw new org.apache.kafka.common.errors.UnsupportedVersionException(
                "durable BK prefix trim requires the M4/M5 lifecycle gate");
    }

    private KafkaBookKeeperPartitionV1 requireStorage(int epoch) {
        if (storage != null && !currentOwner.getAsBoolean()) {
            storage.fence();
            throw new org.apache.kafka.common.errors.FencedLeaderEpochException("native BK assignment was revoked");
        }
        if (storage == null || publishedEpoch != epoch)
            throw new KafkaStorageException("shared BK partition is not recovered for this epoch");
        return storage;
    }

    private NereusKafkaObjectWalPartition requireObjectStorage(int epoch) {
        if (objectStorage != null && !currentOwner.getAsBoolean()) {
            objectStorage.fence();
            throw new org.apache.kafka.common.errors.FencedLeaderEpochException("native Object WAL assignment was revoked");
        }
        if (objectStorage == null || publishedEpoch != epoch) {
            throw new KafkaStorageException("Object WAL partition is not recovered for this epoch");
        }
        return objectStorage;
    }

    private <T> T await(CompletionStage<T> stage, Duration timeout) {
        try {
            return stage.toCompletableFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            if (storage != null && Boolean.TRUE.equals(physicallyDispatched.get())) storage.fence();
            throw new KafkaStorageException("shared BK operation interrupted", failure);
        } catch (TimeoutException failure) {
            if (storage != null && Boolean.TRUE.equals(physicallyDispatched.get())) storage.fence();
            throw new KafkaStorageException("shared BK operation timed out with unresolved outcome", failure);
        } catch (ExecutionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof RuntimeException runtime) throw runtime;
            throw new KafkaStorageException("shared BK operation failed", cause);
        }
    }

    private static Parts parts(
            File dir,
            LogConfig config,
            Scheduler scheduler,
            Time time,
            int transactionTimeout,
            ProducerStateManagerConfig producerConfig,
            LogDirFailureChannel failures)
            throws IOException {
        Files.createDirectories(dir.toPath());
        var identity = UnifiedLog.parseTopicPartitionName(dir);
        var segments = new LogSegments(identity);
        var transactions = new NereusTransactionIndex(
                0, org.apache.kafka.storage.internals.log.LogFileUtils.transactionIndexFile(dir, 0));
        segments.add(NereusLogSegment.open(dir, 0, config, time, transactions));
        var local = new NereusLocalLog(dir, config, segments, scheduler, time, identity, failures, transactions);
        var epochs = UnifiedLog.createLeaderEpochCache(dir, identity, failures, Optional.empty(), scheduler);
        var producers =
                new NereusProducerStateManager(identity, dir, transactionTimeout, producerConfig, time, transactions);
        return new Parts(local, epochs, producers);
    }

    private record Parts(
            NereusLocalLog local,
            org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache epochs,
            NereusProducerStateManager producers) {
        // The local shell and native protocol state have one owner.
    }
}
