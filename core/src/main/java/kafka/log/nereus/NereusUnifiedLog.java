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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.AbortedTxn;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LazyIndex;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogFileUtils;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.LogSegments;
import org.apache.kafka.storage.internals.log.LogStartOffsetIncrementReason;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.RequiredAcksAwareAppend;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.internals.log.VerificationGuard;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.checkpoint.KafkaCanonicalCheckpointState;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.kafka.checkpoint.KafkaDerivedIndexState;
import com.nereusstream.kafka.checkpoint.KafkaLeaderEpochState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState;
import com.nereusstream.kafka.partition.KafkaAppendContext;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.partition.KafkaPartitionState;
import com.nereusstream.kafka.partition.KafkaPartitionStorage;
import com.nereusstream.kafka.partition.KafkaStableAppendResult;
import com.nereusstream.kafka.partition.KafkaStableSnapshot;
import com.nereusstream.kafka.partition.KafkaStorageReadRequest;
import com.nereusstream.kafka.partition.KafkaStorageReadResult;
import com.nereusstream.kafka.retention.KafkaDeleteRecordsCoordinator;
import com.nereusstream.kafka.retention.KafkaPartitionMaintenance;
import com.nereusstream.kafka.retention.KafkaTrimBarrier;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Map;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Per-partition stock UnifiedLog shell whose durable state is published only after exact Nereus recovery.
 *
 * <p>Stock validation and offset assignment stay in UnifiedLog. The final LocalLog append and read are redirected to
 * the exact recovered Nereus storage; the synthetic local segment remains empty and is never durable truth.
 */
public final class NereusUnifiedLog extends UnifiedLog implements RequiredAcksAwareAppend {
    private final Object nereusGuard = new Object();
    private final KafkaPartitionIdentity identity;
    private final Duration appendTimeout;
    private final Duration fetchTimeout;
    private final int hardMaxFetchBytes;
    private final NereusProducerStateManager producerStateManager;
    private final ThreadLocal<AppendInvocation> appendInvocation = new ThreadLocal<>();

    private NereusKafkaRecoveredState recoveredState;
    private KafkaPartitionStorage storage;
    private long canonicalSegmentBaseOffset;
    private long canonicalLogicalBytes;
    private long canonicalLargestTimestamp = RecordBatch.NO_TIMESTAMP;
    private long canonicalMaxTimestampOffset = -1;

    private NereusUnifiedLog(
            Parts parts,
            org.apache.kafka.storage.log.metrics.BrokerTopicStats brokerTopicStats,
            int producerIdExpirationCheckIntervalMs,
            Optional<Uuid> topicId,
            KafkaPartitionIdentity identity,
            Duration appendTimeout,
            Duration fetchTimeout,
            int hardMaxFetchBytes,
            LogOffsetsListener logOffsetsListener
    ) throws IOException {
        super(
                0L,
                parts.localLog,
                brokerTopicStats,
                producerIdExpirationCheckIntervalMs,
                parts.leaderEpochCache,
                parts.producerStateManager,
                topicId,
                false,
                logOffsetsListener);
        this.identity = Objects.requireNonNull(identity, "identity");
        this.appendTimeout = positive(appendTimeout, "appendTimeout");
        this.fetchTimeout = positive(fetchTimeout, "fetchTimeout");
        if (hardMaxFetchBytes <= 0) {
            throw new IllegalArgumentException("hardMaxFetchBytes must be positive");
        }
        this.hardMaxFetchBytes = hardMaxFetchBytes;
        this.producerStateManager = parts.producerStateManager;
        parts.localLog.bindStableAppend(this::appendStable);
    }

    public static NereusUnifiedLog create(
            File dir,
            LogConfig config,
            Scheduler scheduler,
            org.apache.kafka.storage.log.metrics.BrokerTopicStats brokerTopicStats,
            Time time,
            int maxTransactionTimeoutMs,
            ProducerStateManagerConfig producerStateManagerConfig,
            int producerIdExpirationCheckIntervalMs,
            LogDirFailureChannel logDirFailureChannel,
            Uuid topicId,
            KafkaPartitionIdentity identity,
            Duration appendTimeout,
            Duration fetchTimeout,
            int hardMaxFetchBytes,
            LogOffsetsListener logOffsetsListener
    ) throws IOException {
        Objects.requireNonNull(topicId, "topicId");
        Objects.requireNonNull(identity, "identity");
        if (Uuid.ZERO_UUID.equals(topicId)
                || !topicId.toString().equals(identity.topicId())) {
            throw invariant("Nereus log requires one exact non-zero topic ID");
        }
        return new NereusUnifiedLog(
                createParts(
                        dir,
                        config,
                        scheduler,
                        time,
                        maxTransactionTimeoutMs,
                        producerStateManagerConfig,
                        logDirFailureChannel),
                brokerTopicStats,
                producerIdExpirationCheckIntervalMs,
                Optional.of(topicId),
                identity,
                appendTimeout,
                fetchTimeout,
                hardMaxFetchBytes,
                logOffsetsListener);
    }

    public void installRecoveredState(
            int leaderEpoch,
            NereusKafkaRecoveredState state
    ) throws IOException {
        Objects.requireNonNull(state, "state");
        synchronized (nereusGuard) {
            requireExactState(leaderEpoch, state);
            if (storage != null) {
                throw invariant("Cannot replace recovered state while Nereus storage is published");
            }
            if (recoveredState != null && recoveredState != state) {
                throw fenced("A different Nereus recovered state is already installed");
            }
            if (recoveredState == null) {
                super.truncateFullyAndStartAt(
                        state.stableEndOffset(),
                        Optional.of(state.logStartOffset()));
                producerStateManager.restoreCanonical(
                        state.producerTransactionState());
                state.leaderEpochRanges().forEach(range ->
                        super.assignEpochStartOffset(
                                range.leaderEpoch(),
                                range.startOffset()));
                super.updateHighWatermark(state.stableEndOffset());
                recoveredState = state;
            }
        }
    }

    public NereusProducerStateManager prepareProducerRecovery(
            int leaderEpoch,
            long logStartOffset
    ) {
        synchronized (nereusGuard) {
            if (leaderEpoch < 0 || logStartOffset < 0) {
                throw new IllegalArgumentException(
                        "Kafka recovery bounds must be non-negative");
            }
            if (storage != null || recoveredState != null) {
                throw fenced(
                        "Kafka producer recovery cannot replace published state");
            }
            return producerStateManager;
        }
    }

    public void installStorage(
            int leaderEpoch,
            KafkaPartitionStorage candidate
    ) {
        Objects.requireNonNull(candidate, "candidate");
        synchronized (nereusGuard) {
            if (recoveredState == null) {
                throw invariant("Nereus storage cannot publish before recovered Kafka state");
            }
            requireExactState(leaderEpoch, recoveredState);
            KafkaStableSnapshot published = candidate.publishDerivedOffsets(
                    recoveredState.stableEndOffset(),
                    recoveredState.stableEndOffset(),
                    recoveredState.lastStableOffset());
            KafkaStableSnapshot snapshot = candidate.stableSnapshot();
            if (!snapshot.equals(published)
                    || !matchesStorage(candidate, leaderEpoch, snapshot)) {
                throw invariant("Nereus storage snapshot does not match recovered Kafka state");
            }
            if (storage != null && storage != candidate) {
                throw fenced("A different Nereus storage instance is already published");
            }
            canonicalSegmentBaseOffset = recoveredState.logStartOffset();
            canonicalLogicalBytes = recoveredState.logicalBytes();
            canonicalLargestTimestamp =
                    recoveredState.largestTimestamp().orElse(RecordBatch.NO_TIMESTAMP);
            canonicalMaxTimestampOffset =
                    recoveredState.maxTimestampOffset().orElse(-1);
            storage = candidate;
        }
    }

    public void removeStorage(
            int leaderEpoch,
            KafkaPartitionStorage expected
    ) {
        synchronized (nereusGuard) {
            if (recoveredState == null || recoveredState.leaderEpoch() != leaderEpoch) {
                return;
            }
            if (expected == null || storage == expected) {
                storage = null;
                recoveredState = null;
            }
        }
    }

    public boolean nereusWritable(int leaderEpoch) {
        synchronized (nereusGuard) {
            return storage != null
                    && recoveredState != null
                    && recoveredState.leaderEpoch() == leaderEpoch
                    && storage.state() == KafkaPartitionState.LEADER_WRITABLE;
        }
    }

    public KafkaPartitionIdentity nereusIdentity() {
        return identity;
    }

    @Override
    public LogAppendInfo appendAsLeader(
            MemoryRecords records,
            int leaderEpoch,
            AppendOrigin origin,
            RequestLocal requestLocal,
            VerificationGuard verificationGuard,
            short transactionVersion
    ) {
        return appendAsLeader(
                records,
                leaderEpoch,
                origin,
                requestLocal,
                verificationGuard,
                transactionVersion,
                (short) 1);
    }

    @Override
    public LogAppendInfo appendAsLeader(
            MemoryRecords records,
            int leaderEpoch,
            AppendOrigin origin,
            RequestLocal requestLocal,
            VerificationGuard verificationGuard,
            short transactionVersion,
            short requiredAcks
    ) {
        if (origin != AppendOrigin.CLIENT
                && origin != AppendOrigin.COORDINATOR) {
            throw NereusKafkaExceptionMapper.map(unsupported(
                    "Nereus append accepts only client or coordinator records"));
        }
        if (requiredAcks != 0 && requiredAcks != 1 && requiredAcks != -1) {
            throw new IllegalArgumentException("Kafka requiredAcks must be 0, 1, or -1");
        }
        if (appendInvocation.get() != null) {
            throw new IllegalStateException("Nested Nereus append invocation is not supported");
        }
        AppendInvocation invocation = new AppendInvocation(leaderEpoch, requiredAcks);
        appendInvocation.set(invocation);
        try {
            synchronized (nereusGuard) {
                requirePublished(leaderEpoch);
                LogAppendInfo result = super.appendAsLeader(
                        records,
                        leaderEpoch,
                        origin,
                        requestLocal,
                        verificationGuard,
                        transactionVersion);
                if (invocation.committedStorage != null) {
                    long stableEndOffset = Math.addExact(result.lastOffset(), 1);
                    observeCommittedAppend(invocation);
                    advanceHighWatermarkToStableEnd(stableEndOffset);
                    KafkaStableSnapshot published =
                            invocation.committedStorage.publishDerivedOffsets(
                                    stableEndOffset,
                                    highWatermark(),
                                    lastStableOffset());
                    if (published.stableEndOffset()
                            != stableEndOffset) {
                        throw invariant(
                                "Nereus derived offset publication returned a mismatched end");
                    }
                }
                return result;
            }
        } catch (RuntimeException | Error failure) {
            if (invocation.committedStorage != null) {
                fenceUnknownAppend(invocation.committedStorage);
            }
            throw failure;
        } finally {
            appendInvocation.remove();
        }
    }

    private void advanceHighWatermarkToStableEnd(long stableEndOffset) {
        try {
            long updatedHighWatermark = super.updateHighWatermark(stableEndOffset);
            if (updatedHighWatermark != stableEndOffset) {
                throw invariant(
                        "Nereus stable append did not advance the Kafka high watermark");
            }
        } catch (IOException failure) {
            throw new KafkaStorageException(
                    "Failed to publish the Nereus stable append as Kafka high watermark",
                    failure);
        }
    }

    @Override
    public LogAppendInfo appendAsFollower(MemoryRecords records, int leaderEpoch) {
        throw new KafkaStorageException(
                "Nereus authoritative storage does not accept Kafka follower appends");
    }

    /**
     * Executes the product-owned checkpoint-before-trim flow outside the Kafka partition lock.
     *
     * <p>The supplied publisher reacquires the exact partition lock before exposing the durable
     * log start and waking delayed Fetch/DeleteRecords operations.
     */
    public long deleteRecords(
            int leaderEpoch,
            long normalizedOffset,
            MaintenanceAuthority authority
    ) {
        if (leaderEpoch < 0 || normalizedOffset < 0) {
            throw new IllegalArgumentException(
                    "Kafka DeleteRecords leader epoch and normalized offset must be non-negative");
        }
        Objects.requireNonNull(authority, "authority");
        KafkaPartitionStorage exactStorage;
        KafkaPartitionMaintenance maintenance;
        synchronized (nereusGuard) {
            requirePublished(leaderEpoch);
            exactStorage = storage;
            maintenance = exactStorage.maintenance().orElseThrow(() ->
                    new KafkaStorageException(
                            "Nereus partition maintenance is not configured"));
        }
        KafkaPartitionMaintenance.Hooks hooks =
                maintenanceHooks(exactStorage, leaderEpoch, authority);
        CompletableFuture<KafkaDeleteRecordsCoordinator.Result> deletion;
        try {
            deletion = Objects.requireNonNull(
                    maintenance.deleteRecords(hooks, normalizedOffset),
                    "Nereus DeleteRecords future");
        } catch (Throwable failure) {
            throw NereusKafkaExceptionMapper.map(failure);
        }
        KafkaDeleteRecordsCoordinator.Result result;
        try {
            result = deletion.get(appendTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw NereusKafkaExceptionMapper.map(new NereusException(
                    ErrorCode.CANCELLED,
                    true,
                    "Nereus DeleteRecords wait was interrupted",
                    failure));
        } catch (TimeoutException failure) {
            throw NereusKafkaExceptionMapper.map(new NereusException(
                    ErrorCode.TIMEOUT,
                    true,
                    "Nereus DeleteRecords did not finish before the configured timeout",
                    failure));
        } catch (ExecutionException failure) {
            throw NereusKafkaExceptionMapper.map(failure);
        }
        synchronized (nereusGuard) {
            requireSamePublishedStorage(exactStorage);
            if (result.requestedOffset() != normalizedOffset
                    || result.durableLowWatermark() < normalizedOffset
                    || result.durableLowWatermark()
                            != exactStorage.stableSnapshot().logStartOffset()) {
                throw invariant(
                        "Nereus DeleteRecords result does not match durable partition state");
            }
        }
        return result.durableLowWatermark();
    }

    /** Creates authority-fenced hooks for the product-owned periodic retention runtime. */
    public KafkaPartitionMaintenance.Hooks maintenanceHooks(
            int leaderEpoch,
            MaintenanceAuthority authority
    ) {
        Objects.requireNonNull(authority, "authority");
        KafkaPartitionStorage exactStorage;
        synchronized (nereusGuard) {
            requirePublished(leaderEpoch);
            exactStorage = storage;
            if (exactStorage.maintenance().isEmpty()) {
                throw new KafkaStorageException(
                        "Nereus partition maintenance is not configured");
            }
        }
        return maintenanceHooks(exactStorage, leaderEpoch, authority);
    }

    public void publishDurableLogStart(
            KafkaPartitionStorage expectedStorage,
            int expectedLeaderEpoch,
            long durableOffset
    ) {
        Objects.requireNonNull(expectedStorage, "expectedStorage");
        synchronized (nereusGuard) {
            if (storage != expectedStorage
                    || recoveredState == null
                    || recoveredState.leaderEpoch() != expectedLeaderEpoch
                    || expectedStorage.state() != KafkaPartitionState.LEADER_WRITABLE
                    || expectedStorage.stableSnapshot().logStartOffset() < durableOffset) {
                throw fenced(
                        "Nereus DeleteRecords completion belongs to a stale leader");
            }
            super.maybeIncrementLogStartOffset(
                    durableOffset,
                    LogStartOffsetIncrementReason.ClientRecordDeletion);
        }
    }

    private KafkaPartitionMaintenance.Hooks maintenanceHooks(
            KafkaPartitionStorage exactStorage,
            int leaderEpoch,
            MaintenanceAuthority authority
    ) {
        return new KafkaPartitionMaintenance.Hooks() {
            @Override
            public CompletableFuture<KafkaPartitionMaintenance.Capture> capture(
                KafkaCheckpointSourceState currentSource
            ) {
                try {
                    KafkaPartitionMaintenance.Capture captured =
                            authority.capture(
                                    exactStorage,
                                    leaderEpoch,
                                    () -> {
                                        synchronized (nereusGuard) {
                                            requireSamePublishedStorage(exactStorage);
                                            KafkaStableSnapshot snapshot =
                                                    exactStorage.stableSnapshot();
                                            return new KafkaPartitionMaintenance.Capture(
                                                    canonicalCheckpoint(
                                                            currentSource, snapshot),
                                                    snapshot.highWatermark(),
                                                    snapshot.lastStableOffset());
                                        }
                                    });
                    return CompletableFuture.completedFuture(captured);
                } catch (Throwable failure) {
                    return CompletableFuture.failedFuture(failure);
                }
            }

            @Override
            public CompletableFuture<Void> advanceLogStart(
                    KafkaTrimBarrier.Snapshot revalidated,
                    long durableTrimOffset,
                    VersionedKafkaPartitionBinding publishedBinding
            ) {
                try {
                    if (!revalidated.identity().equals(identity)
                            || publishedBinding.value().observedLeaderEpoch() != leaderEpoch) {
                        throw fenced(
                                "Nereus durable trim completion changed partition authority");
                    }
                    authority.publish(exactStorage, leaderEpoch, durableTrimOffset);
                    return CompletableFuture.completedFuture(null);
                } catch (Throwable failure) {
                    return CompletableFuture.failedFuture(failure);
                }
            }
        };
    }

    private KafkaCanonicalCheckpointState canonicalCheckpoint(
            KafkaCheckpointSourceState source,
            KafkaStableSnapshot snapshot
    ) {
        Objects.requireNonNull(source, "source");
        if (source.trimOffset() != snapshot.logStartOffset()
                || source.endOffset() != snapshot.stableEndOffset()
                || source.stateMapEndOffset() != source.endOffset()
                || source.appendInFlight()
                || producerStateManager.mapEndOffset() != source.endOffset()) {
            throw invariant(
                    "Nereus maintenance capture does not match the exact stable source");
        }
        KafkaVirtualSegmentState.LogConfigHistoryEntry config = canonicalConfig(
                config(), canonicalSegmentBaseOffset);
        List<KafkaVirtualSegmentState.VirtualSegment> segments;
        List<KafkaDerivedIndexState.SegmentTimeIndex> timeIndexes;
        List<KafkaDerivedIndexState.SegmentLogicalByteIndex> logicalIndexes;
        if (canonicalLogicalBytes == 0
                && canonicalSegmentBaseOffset == source.endOffset()) {
            segments = List.of();
            timeIndexes = List.of();
            logicalIndexes = List.of();
        } else {
            if (canonicalLargestTimestamp < 0
                    || canonicalMaxTimestampOffset < canonicalSegmentBaseOffset
                    || canonicalMaxTimestampOffset >= source.endOffset()) {
                throw invariant(
                        "Nereus maintenance cannot publish an incomplete timestamp image");
            }
            segments = List.of(new KafkaVirtualSegmentState.VirtualSegment(
                    canonicalSegmentBaseOffset,
                    source.endOffset(),
                    0,
                    0,
                    0,
                    0,
                    canonicalLargestTimestamp,
                    canonicalMaxTimestampOffset,
                    canonicalLogicalBytes,
                    0,
                    canonicalLogicalBytes,
                    config.configDigest(),
                    KafkaVirtualSegmentState.RollReason.INITIAL,
                    KafkaVirtualSegmentState.SegmentState.ACTIVE));
            timeIndexes = List.of(new KafkaDerivedIndexState.SegmentTimeIndex(
                    canonicalSegmentBaseOffset, List.of()));
            logicalIndexes = List.of(new KafkaDerivedIndexState.SegmentLogicalByteIndex(
                    canonicalSegmentBaseOffset, canonicalLogicalBytes, List.of()));
        }
        return new KafkaCanonicalCheckpointState(
                source.endOffset(),
                source.trimOffset(),
                source.endOffset(),
                producerStateManager.exportCanonical(source.endOffset()),
                new KafkaLeaderEpochState(
                        source.trimOffset(),
                        source.endOffset(),
                        canonicalLeaderEpochs(source.trimOffset())),
                new KafkaVirtualSegmentState(
                        source.trimOffset(),
                        source.endOffset(),
                        segments,
                        List.of(config)),
                new KafkaDerivedIndexState(
                        source.trimOffset(),
                        source.endOffset(),
                        timeIndexes,
                        logicalIndexes));
    }

    private List<KafkaLeaderEpochState.LeaderEpochRange> canonicalLeaderEpochs(
            long logStartOffset
    ) {
        List<NereusKafkaRecoveredState.LeaderEpochRange> recovered =
                recoveredState.leaderEpochRanges();
        int first = 0;
        for (int index = 0; index < recovered.size(); index++) {
            if (recovered.get(index).startOffset() <= logStartOffset) {
                first = index;
            }
        }
        return recovered.subList(first, recovered.size()).stream()
                .map(range -> new KafkaLeaderEpochState.LeaderEpochRange(
                        range.leaderEpoch(), range.startOffset()))
                .toList();
    }

    private static KafkaVirtualSegmentState.LogConfigHistoryEntry canonicalConfig(
            LogConfig config,
            long effectiveFromOffset
    ) {
        int cleanupPolicyFlags = (config.delete
                ? KafkaVirtualSegmentState.LogConfigHistoryEntry.CLEANUP_DELETE_FLAG
                : 0)
                | (config.compact
                        ? KafkaVirtualSegmentState.LogConfigHistoryEntry.CLEANUP_COMPACT_FLAG
                        : 0);
        if (cleanupPolicyFlags == 0) {
            throw invariant("Nereus Kafka log has no supported cleanup policy");
        }
        return KafkaVirtualSegmentState.LogConfigHistoryEntry.create(
                0,
                effectiveFromOffset,
                config.segmentSize(),
                config.segmentMs,
                config.segmentJitterMs,
                config.maxIndexSize,
                config.indexInterval,
                config.retentionSize,
                config.retentionMs,
                config.fileDeleteDelayMs,
                config.deleteRetentionMs,
                config.compactionLagMs,
                config.maxCompactionLagMs,
                config.minCleanableRatio,
                cleanupPolicyFlags);
    }

    private void observeCommittedAppend(AppendInvocation invocation) {
        canonicalLogicalBytes = addExact(
                canonicalLogicalBytes,
                invocation.logicalBytes,
                "Nereus canonical logical-byte count overflow");
        if (invocation.largestTimestamp != RecordBatch.NO_TIMESTAMP
                && (canonicalLargestTimestamp == RecordBatch.NO_TIMESTAMP
                        || invocation.largestTimestamp > canonicalLargestTimestamp
                        || (invocation.largestTimestamp == canonicalLargestTimestamp
                                && invocation.maxTimestampOffset
                                        < canonicalMaxTimestampOffset))) {
            canonicalLargestTimestamp = invocation.largestTimestamp;
            canonicalMaxTimestampOffset = invocation.maxTimestampOffset;
        }
    }

    private static long addExact(long left, long right, String message) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException failure) {
            throw invariant(message);
        }
    }

    @Override
    public FetchDataInfo read(
            long startOffset,
            int maxLength,
            FetchIsolation isolation,
            boolean minOneMessage
    ) {
        Objects.requireNonNull(isolation, "isolation");
        KafkaPartitionStorage exactStorage;
        KafkaStableSnapshot snapshot;
        long maxOffsetExclusive;
        synchronized (nereusGuard) {
            if (storage == null
                    || recoveredState == null
                    || storage.state() != KafkaPartitionState.LEADER_WRITABLE) {
                throw new KafkaStorageException(
                        "Nereus partition storage is not recovered and published");
            }
            exactStorage = storage;
            snapshot = exactStorage.stableSnapshot();
            maxOffsetExclusive = switch (isolation) {
                case LOG_END -> logEndOffset();
                case HIGH_WATERMARK -> highWatermark();
                case TXN_COMMITTED -> lastStableOffset();
            };
        }
        if (startOffset < snapshot.logStartOffset()
                || startOffset > snapshot.stableEndOffset()) {
            throw new OffsetOutOfRangeException(
                    "Nereus Fetch offset " + startOffset + " is outside stable range ["
                            + snapshot.logStartOffset() + ", "
                            + snapshot.stableEndOffset() + "]");
        }
        if (maxLength <= 0 || startOffset >= maxOffsetExclusive) {
            requireSamePublishedStorage(exactStorage);
            return new FetchDataInfo(
                    new LogOffsetMetadata(startOffset),
                    MemoryRecords.EMPTY,
                    false,
                    isolation == FetchIsolation.TXN_COMMITTED
                            ? Optional.of(List.of())
                            : Optional.empty());
        }

        KafkaStorageReadRequest request = new KafkaStorageReadRequest(
                startOffset,
                maxOffsetExclusive,
                Math.max(1, maxLength),
                maxLength,
                hardMaxFetchBytes,
                minOneMessage,
                0,
                0,
                fetchTimeout);
        KafkaStorageReadResult result = awaitRead(exactStorage, request);
        com.nereusstream.kafka.codec.KafkaFetchAssembly assembly = result.fetchAssembly();
        requireSamePublishedStorage(exactStorage);
        validateReadResult(snapshot, maxOffsetExclusive, maxLength, minOneMessage, result);
        MemoryRecords records = MemoryRecords.readableRecords(assembly.recordsBuffer());
        Optional<List<org.apache.kafka.common.message.FetchResponseData.AbortedTransaction>>
                abortedTransactions = abortedTransactionsForRead(
                        exactStorage,
                        isolation,
                        startOffset,
                        assembly.nextLogicalOffset(),
                        assembly.sizeInBytes());
        long actualFirstOffset = assembly.actualFirstBatchBaseOffset().orElse(startOffset);
        int relativePosition = Math.toIntExact(assembly.relativeLogicalBytePosition());
        LogOffsetMetadata fetchOffset = new LogOffsetMetadata(
                actualFirstOffset,
                assembly.virtualSegmentBaseOffset(),
                relativePosition);
        return new FetchDataInfo(
                fetchOffset, records, false, abortedTransactions);
    }

    private Optional<List<org.apache.kafka.common.message.FetchResponseData.AbortedTransaction>>
            abortedTransactionsForRead(
                    KafkaPartitionStorage exactStorage,
                    FetchIsolation isolation,
                    long startOffset,
                    long upperBoundOffset,
                    int sizeInBytes
    ) {
        if (isolation != FetchIsolation.TXN_COMMITTED) {
            return Optional.empty();
        }
        if (sizeInBytes == 0 || upperBoundOffset <= startOffset) {
            return Optional.of(List.of());
        }
        synchronized (nereusGuard) {
            requireSamePublishedStorage(exactStorage);
            return Optional.of(producerStateManager.collectAbortedTransactions(
                    startOffset, upperBoundOffset).stream()
                    .map(AbortedTxn::asAbortedTransaction)
                    .toList());
        }
    }

    private void appendStable(long lastOffset, MemoryRecords records) {
        AppendInvocation invocation = appendInvocation.get();
        if (invocation == null) {
            throw new KafkaStorageException("Nereus append reached LocalLog without exact request context");
        }
        KafkaPartitionStorage exactStorage;
        long expectedStartOffset;
        synchronized (nereusGuard) {
            if (!nereusWritable(invocation.leaderEpoch)) {
                throw new KafkaStorageException(
                        "Nereus partition storage is no longer writable for the append");
            }
            exactStorage = storage;
            expectedStartOffset = exactStorage.stableSnapshot().stableEndOffset();
        }
        KafkaAppendContext context = new KafkaAppendContext(
                expectedStartOffset,
                invocation.leaderEpoch,
                invocation.requiredAcks,
                appendTimeout,
                Map.of(
                        "topic", identity.observedTopicName(),
                        "partition", Integer.toString(identity.partition())));
        CompletableFuture<KafkaStableAppendResult> append;
        try {
            append = Objects.requireNonNull(
                    exactStorage.append(records.buffer().duplicate(), context),
                    "Nereus partition storage append future");
        } catch (Throwable failure) {
            throw NereusKafkaExceptionMapper.map(failure);
        }

        KafkaStableAppendResult result;
        try {
            result = append.get(appendTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            fenceUnknownAppend(exactStorage);
            throw NereusKafkaExceptionMapper.map(new NereusException(
                    ErrorCode.CANCELLED,
                    true,
                    "Nereus stable append wait was interrupted",
                    failure,
                    AppendOutcome.MAY_HAVE_COMMITTED));
        } catch (TimeoutException failure) {
            fenceUnknownAppend(exactStorage);
            throw NereusKafkaExceptionMapper.map(new NereusException(
                    ErrorCode.TIMEOUT,
                    true,
                    "Nereus stable append did not finish before the configured timeout",
                    failure,
                    AppendOutcome.MAY_HAVE_COMMITTED));
        } catch (ExecutionException failure) {
            throw NereusKafkaExceptionMapper.map(failure);
        }
        try {
            validateAppendResult(exactStorage, context, records, lastOffset, result);
        } catch (RuntimeException | Error failure) {
            fenceUnknownAppend(exactStorage);
            throw failure;
        }
        invocation.markStable(exactStorage, records);
    }

    private KafkaStorageReadResult awaitRead(
            KafkaPartitionStorage exactStorage,
            KafkaStorageReadRequest request
    ) {
        CompletableFuture<KafkaStorageReadResult> read;
        try {
            read = Objects.requireNonNull(
                    exactStorage.read(request),
                    "Nereus partition storage read future");
        } catch (Throwable failure) {
            throw NereusKafkaExceptionMapper.map(failure);
        }
        try {
            return read.get(fetchTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw NereusKafkaExceptionMapper.map(new NereusException(
                    ErrorCode.CANCELLED,
                    true,
                    "Nereus Fetch wait was interrupted",
                    failure));
        } catch (TimeoutException failure) {
            throw NereusKafkaExceptionMapper.map(new NereusException(
                    ErrorCode.TIMEOUT,
                    true,
                    "Nereus Fetch did not finish before the configured timeout",
                    failure));
        } catch (ExecutionException failure) {
            throw NereusKafkaExceptionMapper.map(failure);
        }
    }

    private static void validateAppendResult(
            KafkaPartitionStorage exactStorage,
            KafkaAppendContext context,
            MemoryRecords records,
            long lastOffset,
            KafkaStableAppendResult result
    ) {
        Objects.requireNonNull(result, "result");
        KafkaStableSnapshot snapshot = result.stableSnapshot();
        boolean encodedMatches = result.requiredAcks() == context.requiredAcks()
                && result.encodedAppend().range().startOffset() == context.expectedStartOffset()
                && result.encodedAppend().range().endOffset() == lastOffset + 1
                && result.encodedAppend().encodedBytes() == records.sizeInBytes();
        boolean storageMatches = snapshot.stableEndOffset() == lastOffset + 1
                && snapshot.equals(exactStorage.stableSnapshot())
                && exactStorage.state() == KafkaPartitionState.LEADER_WRITABLE;
        if (!encodedMatches || !storageMatches) {
            throw NereusKafkaExceptionMapper.map(invariant(
                    "Nereus stable append result does not match the stock validated append"));
        }
    }

    private static void validateReadResult(
            KafkaStableSnapshot requestedSnapshot,
            long maxOffsetExclusive,
            int maxLength,
            boolean minOneMessage,
            KafkaStorageReadResult result
    ) {
        Objects.requireNonNull(result, "result");
        com.nereusstream.kafka.codec.KafkaFetchAssembly assembly = result.fetchAssembly();
        KafkaStableSnapshot resultSnapshot = result.stableSnapshot();
        boolean snapshotMatches = resultSnapshot.logStartOffset() == requestedSnapshot.logStartOffset()
                && resultSnapshot.stableEndOffset() >= requestedSnapshot.stableEndOffset();
        boolean boundsMatch = assembly.nextLogicalOffset() <= maxOffsetExclusive
                && assembly.sourceCoverageEndOffset() <= maxOffsetExclusive
                && assembly.abortedTransactions().isEmpty();
        boolean overflowMatches = !assembly.firstEntryOverflow()
                ? assembly.sizeInBytes() <= maxLength
                : minOneMessage;
        if (!snapshotMatches || !boundsMatch || !overflowMatches) {
            throw NereusKafkaExceptionMapper.map(invariant(
                    "Nereus Fetch result violates the exact stock read bounds"));
        }
    }

    private static void fenceUnknownAppend(KafkaPartitionStorage exactStorage) {
        try {
            exactStorage.resign();
        } catch (Throwable ignored) {
            // The protocol response is already fenced by the unknown append outcome.
        }
    }

    private void requirePublished(int leaderEpoch) {
        synchronized (nereusGuard) {
            if (!nereusWritable(leaderEpoch)) {
                throw new KafkaStorageException(
                        "Nereus partition storage is not writable for leader epoch "
                                + leaderEpoch);
            }
        }
    }

    private void requireSamePublishedStorage(KafkaPartitionStorage exactStorage) {
        synchronized (nereusGuard) {
            if (storage != exactStorage
                    || recoveredState == null
                    || exactStorage.state() != KafkaPartitionState.LEADER_WRITABLE) {
                throw new KafkaStorageException(
                        "Nereus partition storage changed while serving Fetch");
            }
        }
    }

    private void requireExactState(
            int leaderEpoch,
            NereusKafkaRecoveredState state
    ) {
        if (!state.frozen()
                || !state.identity().equals(identity)
                || state.leaderEpoch() != leaderEpoch
                || !state.topicPartition().equals(topicPartition())
                || !state.topicId().equals(topicId().orElse(Uuid.ZERO_UUID))
                || state.producerStateManager() != producerStateManager) {
            throw invariant("Nereus recovered state does not match the exact log shell");
        }
    }

    private boolean matchesStorage(
            KafkaPartitionStorage candidate,
            int leaderEpoch,
            KafkaStableSnapshot snapshot
    ) {
        if (!candidate.identity().equals(identity)
                || candidate.leaderEpoch() != leaderEpoch
                || candidate.state() != KafkaPartitionState.LEADER_WRITABLE) {
            return false;
        }
        return snapshot.logStartOffset() == recoveredState.logStartOffset()
                && snapshot.stableEndOffset() == recoveredState.stableEndOffset()
                && snapshot.highWatermark() == recoveredState.stableEndOffset()
                && snapshot.lastStableOffset() == recoveredState.lastStableOffset();
    }

    private static Parts createParts(
            File dir,
            LogConfig config,
            Scheduler scheduler,
            Time time,
            int maxTransactionTimeoutMs,
            ProducerStateManagerConfig producerStateManagerConfig,
            LogDirFailureChannel logDirFailureChannel
    ) throws IOException {
        Files.createDirectories(dir.toPath());
        TopicPartition topicPartition = UnifiedLog.parseTopicPartitionName(dir);
        LogSegments segments = new LogSegments(topicPartition);
        NereusTransactionIndex transactionIndex = new NereusTransactionIndex(
                0L, LogFileUtils.transactionIndexFile(dir, 0L));
        LogSegment segment = new LogSegment(
                FileRecords.open(
                        LogFileUtils.logFile(dir, 0L),
                        false,
                        config.initFileSize(),
                        config.preallocate),
                LazyIndex.forOffset(
                        LogFileUtils.offsetIndexFile(dir, 0L),
                        0L,
                        config.maxIndexSize),
                LazyIndex.forTime(
                        LogFileUtils.timeIndexFile(dir, 0L),
                        0L,
                        config.maxIndexSize),
                transactionIndex,
                0L,
                config.indexInterval,
                config.randomSegmentJitter(),
                time);
        segments.add(segment);
        NereusLocalLog localLog = new NereusLocalLog(
                dir,
                config,
                segments,
                0L,
                new LogOffsetMetadata(0L),
                scheduler,
                time,
                topicPartition,
                logDirFailureChannel,
                transactionIndex);
        org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache leaderEpochCache =
                UnifiedLog.createLeaderEpochCache(
                dir,
                topicPartition,
                logDirFailureChannel,
                Optional.empty(),
                scheduler);
        NereusProducerStateManager producerStateManager =
                new NereusProducerStateManager(
                topicPartition,
                dir,
                maxTransactionTimeoutMs,
                producerStateManagerConfig,
                time,
                transactionIndex);
        return new Parts(localLog, leaderEpochCache, producerStateManager);
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative() || value.toMillis() <= 0) {
            throw new IllegalArgumentException(name + " must be positive and millisecond-representable");
        }
        return value;
    }

    private static NereusException fenced(String message) {
        return new NereusException(ErrorCode.FENCED_APPEND, false, message);
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                message);
    }

    private static NereusException unsupported(String message) {
        return new NereusException(ErrorCode.UNSUPPORTED_FORMAT, false, message);
    }

    private record Parts(
            NereusLocalLog localLog,
            org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache leaderEpochCache,
            NereusProducerStateManager producerStateManager
    ) { }

    private static final class AppendInvocation {
        private final int leaderEpoch;
        private final short requiredAcks;
        private KafkaPartitionStorage committedStorage;
        private long logicalBytes;
        private long largestTimestamp = RecordBatch.NO_TIMESTAMP;
        private long maxTimestampOffset = -1;

        private AppendInvocation(int leaderEpoch, short requiredAcks) {
            this.leaderEpoch = leaderEpoch;
            this.requiredAcks = requiredAcks;
        }

        private void markStable(
                KafkaPartitionStorage exactStorage,
                MemoryRecords records
        ) {
            committedStorage = exactStorage;
            logicalBytes = records.sizeInBytes();
            for (RecordBatch batch : records.batches()) {
                for (Record record : batch) {
                    long timestamp = record.timestamp();
                    if (timestamp >= 0
                            && (largestTimestamp == RecordBatch.NO_TIMESTAMP
                                    || timestamp > largestTimestamp
                                    || (timestamp == largestTimestamp
                                            && record.offset() < maxTimestampOffset))) {
                        largestTimestamp = timestamp;
                        maxTimestampOffset = record.offset();
                    }
                }
            }
        }
    }

    /** Partition-lock authority supplied by the exact current ReplicaManager partition. */
    public interface MaintenanceAuthority {
        KafkaPartitionMaintenance.Capture capture(
                KafkaPartitionStorage expectedStorage,
                int expectedLeaderEpoch,
                MaintenanceCapture capture);

        void publish(
                KafkaPartitionStorage expectedStorage,
                int expectedLeaderEpoch,
                long durableOffset);
    }

    @FunctionalInterface
    public interface MaintenanceCapture {
        KafkaPartitionMaintenance.Capture capture();
    }
}
