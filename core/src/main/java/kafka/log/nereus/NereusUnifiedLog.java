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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
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
import com.nereusstream.kafka.bookkeeper.commit.KafkaBatchDuplicateIdentityV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedAppendOutcomeV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadBatchV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadOutcomeV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperSequentialReadRequestV1;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    private KafkaBookKeeperPartitionV1 storage;
    private int publishedEpoch = -1;
    private java.util.function.BooleanSupplier currentOwner = () -> false;
    private long configMetadataOffset = -1;
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
            if (storage != null) throw new KafkaStorageException("shared BK log already has a published owner");
            var snapshot = candidate.capture();
            if (snapshot.root().fence().kafkaLeaderEpoch() != epoch
                    || snapshot.root().frontiers().trimStartOffset() != 0) {
                throw new KafkaStorageException(
                        "shared BK recovered partition has a foreign epoch or unsupported prefix");
            }
            long end = snapshot.root().frontiers().durableEndOffset();
            super.truncateFullyAndStartAt(end, Optional.of(0L));
            producers.loadSharedState(snapshot);
            if (producers.mapEndOffset() != end) {
                throw new KafkaStorageException("native producer replay does not cover the shared BK prefix");
            }
            snapshot.leaderEpochIndex().startOffsets().forEach(super::assignEpochStartOffset);
            super.assignEpochStartOffset(epoch, end);
            super.updateHighWatermark(end);
            if (lastStableOffset() != snapshot.root().frontiers().lastStableOffset()) {
                throw new KafkaStorageException("native transaction replay differs from the shared BK LSO");
            }
            var state = new NereusKafkaRecoveredState(identity.topicPartition(), identity.topicId(), epoch, snapshot);
            this.currentOwner = currentOwner;
            storage = candidate;
            publishedEpoch = epoch;
            return state;
        }
    }

    public void removeStorage(int epoch, KafkaBookKeeperPartitionV1 expected) {
        synchronized (guard) {
            if (publishedEpoch == epoch && (expected == null || storage == expected)) {
                storage = null;
                publishedEpoch = -1;
            }
        }
    }

    public boolean nereusWritable(int epoch) {
        synchronized (guard) {
            return storage != null && publishedEpoch == epoch;
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
            var exact = requireStorage(epoch);
            var lengths = new ArrayList<Integer>();
            boolean duplicate = true;
            for (var batch : records.batches()) {
                batch.ensureValid();
                if (batch.magic() != RecordBatch.MAGIC_VALUE_V2) {
                    throw new org.apache.kafka.common.errors.UnsupportedForMessageFormatException(
                            "shared BK requires magic-v2");
                }
                lengths.add(config().maxMessageSize());
                duplicate &= batch.hasProducerId()
                        && !batch.isControlBatch()
                        && producers.activeProducers().containsKey(batch.producerId())
                        && exact.capture()
                                .committedProducerState()
                                .findDuplicate(new KafkaBatchDuplicateIdentityV1(
                                        batch.producerId(),
                                        batch.producerEpoch(),
                                        batch.baseSequence(),
                                        batch.lastSequence()))
                                .isPresent();
            }
            if (lengths.isEmpty())
                return super.appendAsLeader(
                        records, epoch, origin, requestLocal, verificationGuard, transactionVersion);
            if (admission.get() != null) throw new IllegalStateException("nested shared BK append");
            try (var reserved = duplicate ? null : exact.admit(lengths)) {
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

    private void appendStable(long lastOffset, MemoryRecords records) {
        var reserved = admission.get();
        if (reserved == null) throw new KafkaStorageException("native append has no pre-offset BK admission");
        var batches = new ArrayList<KafkaNativeAssignedRecordBatchV1>();
        for (var batch : records.batches()) {
            var bytes = ByteBuffer.allocate(batch.sizeInBytes());
            batch.writeTo(bytes);
            var raw = CanonicalBytes.copyOf(bytes.array());
            batches.add(KafkaNativeAssignedRecordBatchV1.validate(KafkaRawAssignedRecordBatchFactsV1.parse(raw)));
        }
        physicallyDispatched.set(true);
        var result = await(storage.appendAssigned(reserved, batches), appendTimeout);
        if (result.outcome() != KafkaOrderedAppendOutcomeV1.COMMITTED_ORDERED
                || result.endOffsetExclusive().orElseThrow() != lastOffset + 1) {
            storage.fence();
            throw new KafkaStorageException("shared BK append did not commit the exact native batch range");
        }
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
    public FetchDataInfo read(long offset, int maximumBytes, FetchIsolation isolation, boolean minOneMessage) {
        synchronized (guard) {
            var exact = requireStorage(publishedEpoch);
            var mode = readIsolation(isolation);
            var snapshot = exact.capture();
            if (offset < snapshot.root().frontiers().trimStartOffset()
                    || offset > snapshot.root().frontiers().readableEndOffset()) {
                throw new OffsetOutOfRangeException("shared BK Fetch offset is outside the captured range");
            }
            if (maximumBytes <= 0 || offset >= snapshot.root().readUpperBound(mode)) {
                return new FetchDataInfo(
                        new LogOffsetMetadata(offset),
                        MemoryRecords.EMPTY,
                        false,
                        isolation == FetchIsolation.TXN_COMMITTED ? Optional.of(List.of()) : Optional.empty());
            }
            var result = await(
                    exact.read(new KafkaBookKeeperSequentialReadRequestV1(
                            offset, mode, Math.min(maximumBytes, hardMaxFetchBytes), Optional.empty())),
                    fetchTimeout);
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
            result.batches()
                    .forEach(batch -> buffer.put(batch.rawAssignedRecordBatch().toByteArray()));
            buffer.flip();
            var aborted = isolation == FetchIsolation.TXN_COMMITTED
                    ? Optional.of(result.abortedTransactions().stream()
                            .map(transaction ->
                                    new org.apache.kafka.common.message.FetchResponseData.AbortedTransaction()
                                            .setProducerId(transaction.producerId())
                                            .setFirstOffset(transaction.firstOffset()))
                            .toList())
                    : Optional.<List<org.apache.kafka.common.message.FetchResponseData.AbortedTransaction>>empty();
            return new FetchDataInfo(
                    new LogOffsetMetadata(result.batches().get(0).startOffset(), 0, 0),
                    MemoryRecords.readableRecords(buffer),
                    false,
                    aborted);
        }
    }

    @Override
    public LogOffsetMetadata maybeConvertToOffsetMetadata(long offset) {
        return new LogOffsetMetadata(offset, 0, 0);
    }

    public CompletionStage<Optional<FileRecords.TimestampAndOffset>> lookupTimestamp(long timestamp, int epoch) {
        synchronized (guard) {
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

    @Override
    public LogConfig updateConfigAtMetadataOffset(LogConfig config, long metadataOffset) {
        synchronized (guard) {
            if (metadataOffset < configMetadataOffset) throw new KafkaStorageException("stale metadata config update");
            var previous = super.updateConfig(config);
            configMetadataOffset = metadataOffset;
            return previous;
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
