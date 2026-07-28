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
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.AbortedTxn;
import org.apache.kafka.storage.internals.log.AsyncOffsetReader;
import org.apache.kafka.storage.internals.log.CleanedTransactionMetadata;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LastRecord;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.LogSegments;
import org.apache.kafka.storage.internals.log.LogStartOffsetIncrementReason;
import org.apache.kafka.storage.internals.log.OffsetResultHolder;
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
import com.nereusstream.kafka.checkpoint.KafkaProducerTransactionState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState;
import com.nereusstream.kafka.compaction.KafkaCompactionPartitionPass;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.AbortedTransactionRange;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.MarkerDecision;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.OpenTransactionRange;
import com.nereusstream.kafka.compaction.KafkaCompactionPlanner;
import com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.MarkerStatus;
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
import com.nereusstream.materialization.MaterializationPolicy;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    private final NereusLocalLog nereusLocalLog;
    private final NereusCanonicalLogState canonicalState;
    private final Time time;
    private final ThreadLocal<AppendInvocation> appendInvocation = new ThreadLocal<>();

    private NereusKafkaRecoveredState recoveredState;
    private KafkaPartitionStorage storage;
    private long latestConfigMetadataOffset = -1;

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
        this.nereusLocalLog = parts.localLog;
        this.canonicalState = parts.canonicalState;
        this.time = parts.localLog.time();
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
                        logDirFailureChannel,
                        identity),
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
                canonicalState.restore(
                        state.logStartOffset(),
                        state.stableEndOffset(),
                        state.checkpointState(),
                        state.committedTail(),
                        config(),
                        Math.max(0, latestConfigMetadataOffset),
                        time.milliseconds());
                nereusLocalLog.installCanonicalSegments(
                        canonicalState.segmentBaseOffsets(state.stableEndOffset()));
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
    public LogConfig updateConfig(LogConfig newConfig) {
        synchronized (nereusGuard) {
            if (recoveredState == null
                    || canonicalState.matchesCurrentConfig(newConfig)) {
                return super.updateConfig(newConfig);
            }
            long metadataOffset = canonicalState.nextSyntheticMetadataOffset();
            return updateConfigLocked(newConfig, metadataOffset);
        }
    }

    public LogConfig updateConfigAtMetadataOffset(
            LogConfig newConfig,
            long metadataOffset
    ) {
        if (metadataOffset < 0) {
            throw new IllegalArgumentException(
                    "Kafka config metadata offset must be non-negative");
        }
        synchronized (nereusGuard) {
            if (latestConfigMetadataOffset >= 0
                    && metadataOffset < latestConfigMetadataOffset) {
                throw invariant("Kafka config metadata offset regressed");
            }
            return updateConfigLocked(newConfig, metadataOffset);
        }
    }

    private LogConfig updateConfigLocked(
            LogConfig newConfig,
            long metadataOffset
    ) {
        Objects.requireNonNull(newConfig, "newConfig");
        if (recoveredState != null) {
            canonicalState.updateConfig(newConfig, metadataOffset);
        }
        latestConfigMetadataOffset = metadataOffset;
        return super.updateConfig(newConfig);
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

    /**
     * Creates one product capture provider backed by the exact stock transaction-cleaner oracle.
     *
     * <p>The initial canonical state and the active-producer snapshot are taken under the current
     * Partition lock. The selected decision horizon is then read from Nereus outside that lock,
     * and the same producer snapshot is revalidated before the capture is returned.
     */
    public KafkaCompactionPartitionPass.CaptureProvider compactionCaptureProvider(
            int leaderEpoch,
            MaintenanceAuthority authority,
            CompactionConfiguration configuration
    ) {
        Objects.requireNonNull(authority, "authority");
        CompactionConfiguration exactConfiguration =
                Objects.requireNonNull(configuration, "configuration");
        KafkaPartitionStorage exactStorage;
        KafkaPartitionMaintenance maintenance;
        synchronized (nereusGuard) {
            requirePublished(leaderEpoch);
            exactStorage = storage;
            maintenance = exactStorage.maintenance().orElseThrow(() ->
                    new KafkaStorageException(
                            "Nereus partition maintenance is not configured"));
        }
        KafkaPartitionMaintenance.CompactionHooks hooks =
                compactionHooks(
                        exactStorage,
                        leaderEpoch,
                        authority,
                        exactConfiguration);
        return partition -> {
            if (!partition.equals(identity.durableId())) {
                return CompletableFuture.failedFuture(
                        invariant("Kafka compaction requested another partition identity"));
            }
            return maintenance.captureCompaction(hooks);
        };
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
            canonicalState.advanceLogStart(durableOffset);
            try {
                nereusLocalLog.installCanonicalSegments(
                        canonicalState.segmentBaseOffsets(
                                expectedStorage.stableSnapshot().stableEndOffset()));
            } catch (IOException failure) {
                fenceUnknownAppend(expectedStorage);
                throw new KafkaStorageException(
                        "Failed to rebuild Nereus virtual segment shells after durable trim",
                        failure);
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

    private KafkaPartitionMaintenance.CompactionHooks compactionHooks(
            KafkaPartitionStorage exactStorage,
            int leaderEpoch,
            MaintenanceAuthority authority,
            CompactionConfiguration configuration
    ) {
        return new KafkaPartitionMaintenance.CompactionHooks() {
            @Override
            public CompletableFuture<KafkaPartitionMaintenance.CompactionState> capture(
                    KafkaCheckpointSourceState currentSource
            ) {
                try {
                    KafkaPartitionMaintenance.CompactionState captured =
                            authority.captureCompaction(
                                    exactStorage,
                                    leaderEpoch,
                                    () -> {
                                        synchronized (nereusGuard) {
                                            requireSamePublishedStorage(exactStorage);
                                            KafkaStableSnapshot snapshot =
                                                    exactStorage.stableSnapshot();
                                            return new KafkaPartitionMaintenance.CompactionState(
                                                    canonicalCheckpoint(
                                                            currentSource, snapshot),
                                                    snapshot.highWatermark(),
                                                    snapshot.lastStableOffset(),
                                                    configuration.outputPolicy(),
                                                    configuration.writeSettings());
                                        }
                                    });
                    return CompletableFuture.completedFuture(captured);
                } catch (Throwable failure) {
                    return CompletableFuture.failedFuture(failure);
                }
            }

            @Override
            public CompletableFuture<KafkaCompactionPartitionPass.PassOneInputs> capturePassOne(
                    KafkaCheckpointSourceState currentSource,
                    KafkaCompactionPlanner.Candidate candidate,
                    KafkaPartitionMaintenance.CompactionState state
            ) {
                try {
                    return CompletableFuture.completedFuture(
                            scanCompactionPassOne(
                                    exactStorage,
                                    leaderEpoch,
                                    authority,
                                    currentSource,
                                    candidate,
                                    state,
                                    configuration));
                } catch (Throwable failure) {
                    return CompletableFuture.failedFuture(failure);
                }
            }
        };
    }

    private KafkaCompactionPartitionPass.PassOneInputs scanCompactionPassOne(
            KafkaPartitionStorage exactStorage,
            int leaderEpoch,
            MaintenanceAuthority authority,
            KafkaCheckpointSourceState currentSource,
            KafkaCompactionPlanner.Candidate candidate,
            KafkaPartitionMaintenance.CompactionState state,
            CompactionConfiguration configuration
    ) {
        if (candidate.shouldCompact()
                && candidate.decisionHorizon().recordCount()
                > configuration.maxDecodedRecords()) {
            throw invariant(
                    "Kafka compaction decision horizon exceeds the decoded-record limit");
        }
        CompactionTransactionState before =
                captureCompactionTransactions(
                        exactStorage,
                        leaderEpoch,
                        authority,
                        currentSource,
                        state);
        List<MarkerDecision> markers =
                candidate.shouldCompact()
                        ? scanMarkerDecisions(candidate, before)
                        : List.of();
        CompactionTransactionState after =
                captureCompactionTransactions(
                        exactStorage,
                        leaderEpoch,
                        authority,
                        currentSource,
                        state);
        if (!after.equals(before)) {
            throw fenced(
                    "Kafka producer/transaction state changed during compaction pre-scan");
        }
        KafkaProducerTransactionState producerState = before.producerState();
        List<AbortedTransactionRange> aborted =
                producerState.abortedTransactions().stream()
                        .filter(transaction ->
                                transaction.lastOffset()
                                        >= candidate.decisionHorizon().startOffset()
                                && transaction.firstOffset()
                                        < candidate.decisionHorizon().endOffset())
                        .map(transaction ->
                                new AbortedTransactionRange(
                                        transaction.producerId(),
                                        transaction.firstOffset(),
                                        transaction.lastOffset()))
                        .toList();
        List<OpenTransactionRange> open =
                producerState.openTransactions().stream()
                        .filter(transaction ->
                                transaction.firstOffset()
                                        < candidate.decisionHorizon().endOffset())
                        .map(transaction ->
                                new OpenTransactionRange(
                                        transaction.producerId(),
                                        transaction.firstOffset()))
                        .toList();
        if (aborted.size() > KafkaCompactionPassOneCollector.MAX_TRANSACTION_FACTS
                || open.size() > KafkaCompactionPassOneCollector.MAX_TRANSACTION_FACTS) {
            throw invariant(
                    "Kafka compaction transaction facts exceed the pass-one limit");
        }
        return new KafkaCompactionPartitionPass.PassOneInputs(
                currentSource.endOffset(),
                configuration.maxDecodedRecords(),
                configuration.maxKeyBytes(),
                configuration.maxInMemoryKeyBytes(),
                aborted,
                open,
                markers);
    }

    private CompactionTransactionState captureCompactionTransactions(
            KafkaPartitionStorage exactStorage,
            int leaderEpoch,
            MaintenanceAuthority authority,
            KafkaCheckpointSourceState source,
            KafkaPartitionMaintenance.CompactionState state
    ) {
        return authority.captureCompactionTransactions(
                exactStorage,
                leaderEpoch,
                () -> {
                    synchronized (nereusGuard) {
                        requireSamePublishedStorage(exactStorage);
                        KafkaStableSnapshot current = exactStorage.stableSnapshot();
                        KafkaProducerTransactionState producerState =
                                producerStateManager.exportCanonical(source.endOffset());
                        if (current.logStartOffset() != source.trimOffset()
                                || current.stableEndOffset() != source.endOffset()
                                || !producerState.equals(
                                        state.canonicalState().producerTransactionState())) {
                            throw fenced(
                                    "Kafka compaction transaction capture changed stable source");
                        }
                        return new CompactionTransactionState(
                                producerState,
                                Map.copyOf(lastRecordsOfActiveProducers()));
                    }
                });
    }

    private List<MarkerDecision> scanMarkerDecisions(
            KafkaCompactionPlanner.Candidate candidate,
            CompactionTransactionState transactionState
    ) {
        CleanedTransactionMetadata cleaned = new CleanedTransactionMetadata();
        cleaned.addAbortedTransactions(
                transactionState.producerState().abortedTransactions().stream()
                        .map(transaction ->
                                new AbortedTxn(
                                        transaction.producerId(),
                                        transaction.firstOffset(),
                                        transaction.lastOffset(),
                                        transaction.lastStableOffset()))
                        .toList());
        long nextOffset = candidate.decisionHorizon().startOffset();
        long endOffset = candidate.decisionHorizon().endOffset();
        ArrayList<MarkerDecision> markers = new ArrayList<>();
        while (nextOffset < endOffset) {
            FetchDataInfo page =
                    read(
                            nextOffset,
                            hardMaxFetchBytes,
                            FetchIsolation.LOG_END,
                            true);
            long pageStart = nextOffset;
            for (RecordBatch batch : page.records.batches()) {
                if (batch.baseOffset() >= endOffset) {
                    break;
                }
                if (batch.baseOffset() != nextOffset
                        || batch.nextOffset() > endOffset) {
                    throw invariant(
                            "Kafka compaction transaction pre-scan is not dense");
                }
                if (batch.isControlBatch()) {
                    if (markers.size()
                            >= KafkaCompactionPassOneCollector.MAX_TRANSACTION_FACTS) {
                        throw invariant(
                                "Kafka compaction marker facts exceed the pass-one limit");
                    }
                    boolean discardable = cleaned.onControlBatchRead(batch);
                    boolean activeLastMarker =
                            isActiveLastMarker(
                                    batch,
                                    transactionState.activeProducerLastRecords());
                    markers.add(
                            new MarkerDecision(
                                    batch.lastOffset(),
                                    discardable && !activeLastMarker
                                            ? MarkerStatus.DELETE_ELIGIBLE
                                            : MarkerStatus.RETAIN_REQUIRED));
                } else {
                    cleaned.onBatchRead(batch);
                }
                nextOffset = batch.nextOffset();
            }
            if (nextOffset == pageStart) {
                throw invariant(
                        "Kafka compaction transaction pre-scan made no progress");
            }
        }
        return List.copyOf(markers);
    }

    private static boolean isActiveLastMarker(
            RecordBatch batch,
            Map<Long, LastRecord> activeProducerLastRecords
    ) {
        LastRecord last = activeProducerLastRecords.get(batch.producerId());
        return last != null
                && last.lastDataOffset().isEmpty()
                && last.producerEpoch() == batch.producerEpoch();
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
        KafkaVirtualSegmentState virtualSegments = canonicalState.virtualSegments();
        KafkaDerivedIndexState derivedIndexes = canonicalState.derivedIndexes();
        virtualSegments.requireBounds(source.trimOffset(), source.endOffset());
        derivedIndexes.requireBounds(source.trimOffset(), source.endOffset());
        return new KafkaCanonicalCheckpointState(
                source.endOffset(),
                source.trimOffset(),
                source.endOffset(),
                producerStateManager.exportCanonical(source.endOffset()),
                new KafkaLeaderEpochState(
                        source.trimOffset(),
                        source.endOffset(),
                        canonicalLeaderEpochs(source.trimOffset())),
                virtualSegments,
                derivedIndexes);
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

    private void observeCommittedAppend(AppendInvocation invocation) {
        canonicalState.commitStable(invocation.batches, time.milliseconds());
        nereusLocalLog.updateLogEndOffset(logEndOffset());
    }

    @Override
    public OffsetResultHolder fetchOffsetByTimestamp(
            long targetTimestamp,
            Optional<AsyncOffsetReader> remoteOffsetReader
    ) {
        Objects.requireNonNull(remoteOffsetReader, "remoteOffsetReader");
        if (targetTimestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP
                || targetTimestamp == ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP) {
            return timestampResult(RecordBatch.NO_TIMESTAMP, logStartOffset());
        }
        if (targetTimestamp == ListOffsetsRequest.LATEST_TIMESTAMP) {
            return timestampResult(RecordBatch.NO_TIMESTAMP, logEndOffset());
        }
        if (targetTimestamp == ListOffsetsRequest.LATEST_TIERED_TIMESTAMP
                || targetTimestamp == ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP) {
            return timestampResult(RecordBatch.NO_TIMESTAMP, -1);
        }
        if (targetTimestamp == ListOffsetsRequest.MAX_TIMESTAMP) {
            synchronized (nereusGuard) {
                requireReadable();
                return new OffsetResultHolder(
                        canonicalState.maxTimestampPosition()
                                .map(position -> new FileRecords.TimestampAndOffset(
                                        position.timestamp(),
                                        position.offset(),
                                        Optional.empty())));
            }
        }
        if (targetTimestamp < 0) {
            return new OffsetResultHolder(Optional.empty());
        }
        return new OffsetResultHolder(scanOffsetByTimestamp(targetTimestamp));
    }

    private Optional<FileRecords.TimestampAndOffset> scanOffsetByTimestamp(
            long targetTimestamp
    ) {
        long nextOffset;
        long maximumOffset;
        synchronized (nereusGuard) {
            requireReadable();
            nextOffset = canonicalState.timestampScanCandidate(targetTimestamp);
            maximumOffset = storage.stableSnapshot().stableEndOffset();
        }
        long scannedBytes = 0;
        long scanLimit = Math.max(
                (long) hardMaxFetchBytes,
                64L * 1024 * 1024);
        while (nextOffset < maximumOffset) {
            FetchDataInfo page = read(
                    nextOffset,
                    hardMaxFetchBytes,
                    FetchIsolation.LOG_END,
                    true);
            MemoryRecords records = (MemoryRecords) page.records;
            if (records.sizeInBytes() == 0) {
                throw NereusKafkaExceptionMapper.map(invariant(
                        "Nereus timestamp scan made no progress before stable end"));
            }
            for (RecordBatch batch : records.batches()) {
                for (Record record : batch) {
                    if (record.timestamp() >= targetTimestamp) {
                        Optional<Integer> epoch =
                                batch.partitionLeaderEpoch() < 0
                                        ? Optional.empty()
                                        : Optional.of(batch.partitionLeaderEpoch());
                        return Optional.of(new FileRecords.TimestampAndOffset(
                                record.timestamp(), record.offset(), epoch));
                    }
                }
                nextOffset = Math.addExact(batch.lastOffset(), 1);
            }
            scannedBytes = Math.addExact(scannedBytes, records.sizeInBytes());
            if (scannedBytes > scanLimit && nextOffset < maximumOffset) {
                throw NereusKafkaExceptionMapper.map(new NereusException(
                        ErrorCode.READ_LIMIT_TOO_SMALL,
                        true,
                        "Nereus timestamp lookup exceeded the bounded scan budget"));
            }
        }
        return Optional.empty();
    }

    private static OffsetResultHolder timestampResult(
            long timestamp,
            long offset
    ) {
        return new OffsetResultHolder(new FileRecords.TimestampAndOffset(
                timestamp, offset, Optional.empty()));
    }

    @Override
    public LogOffsetMetadata maybeConvertToOffsetMetadata(long offset) {
        synchronized (nereusGuard) {
            if (storage == null
                    || recoveredState == null
                    || offset < logStartOffset()
                    || offset > logEndOffset()) {
                return new LogOffsetMetadata(offset);
            }
            NereusCanonicalLogState.Position position =
                    canonicalState.positionForOffset(offset);
            return new LogOffsetMetadata(
                    offset,
                    position.segmentBaseOffset(),
                    Math.toIntExact(position.relativeLogicalBytes()));
        }
    }

    private void requireReadable() {
        if (storage == null
                || recoveredState == null
                || storage.state() != KafkaPartitionState.LEADER_WRITABLE) {
            throw new KafkaStorageException(
                    "Nereus partition storage is not recovered and published");
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

        NereusCanonicalLogState.Position requestedPosition;
        synchronized (nereusGuard) {
            requireSamePublishedStorage(exactStorage);
            requestedPosition = canonicalState.positionForOffset(startOffset);
        }
        KafkaStorageReadRequest request = new KafkaStorageReadRequest(
                startOffset,
                maxOffsetExclusive,
                Math.max(1, maxLength),
                maxLength,
                hardMaxFetchBytes,
                minOneMessage,
                requestedPosition.segmentBaseOffset(),
                requestedPosition.relativeLogicalBytes(),
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
        NereusCanonicalLogState.Position actualPosition;
        synchronized (nereusGuard) {
            requireSamePublishedStorage(exactStorage);
            actualPosition = canonicalState.positionForOffset(actualFirstOffset);
        }
        int relativePosition = Math.toIntExact(actualPosition.relativeLogicalBytes());
        LogOffsetMetadata fetchOffset = new LogOffsetMetadata(
                actualFirstOffset,
                actualPosition.segmentBaseOffset(),
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
            LogDirFailureChannel logDirFailureChannel,
            KafkaPartitionIdentity identity
    ) throws IOException {
        Files.createDirectories(dir.toPath());
        TopicPartition topicPartition = UnifiedLog.parseTopicPartitionName(dir);
        LogSegments segments = new LogSegments(topicPartition);
        NereusCanonicalLogState canonicalState = new NereusCanonicalLogState(
                identity.durableId().canonicalIdentity());
        NereusTransactionIndex transactionIndex = new NereusTransactionIndex(
                0L,
                org.apache.kafka.storage.internals.log.LogFileUtils
                        .transactionIndexFile(dir, 0L));
        NereusLogSegment segment = NereusLogSegment.open(
                dir, 0L, config, time, transactionIndex, canonicalState);
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
                transactionIndex,
                canonicalState);
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
        return new Parts(
                localLog, leaderEpochCache, producerStateManager, canonicalState);
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
            NereusProducerStateManager producerStateManager,
            NereusCanonicalLogState canonicalState
    ) { }

    private static final class AppendInvocation {
        private final int leaderEpoch;
        private final short requiredAcks;
        private KafkaPartitionStorage committedStorage;
        private List<NereusCanonicalLogState.BatchObservation> batches = List.of();

        private AppendInvocation(int leaderEpoch, short requiredAcks) {
            this.leaderEpoch = leaderEpoch;
            this.requiredAcks = requiredAcks;
        }

        private void markStable(
                KafkaPartitionStorage exactStorage,
                MemoryRecords records
        ) {
            committedStorage = exactStorage;
            batches = NereusCanonicalLogState.observe(records);
        }
    }

    /** Partition-lock authority supplied by the exact current ReplicaManager partition. */
    public interface MaintenanceAuthority {
        KafkaPartitionMaintenance.Capture capture(
                KafkaPartitionStorage expectedStorage,
                int expectedLeaderEpoch,
                MaintenanceCapture capture);

        KafkaPartitionMaintenance.CompactionState captureCompaction(
                KafkaPartitionStorage expectedStorage,
                int expectedLeaderEpoch,
                CompactionCapture capture);

        CompactionTransactionState captureCompactionTransactions(
                KafkaPartitionStorage expectedStorage,
                int expectedLeaderEpoch,
                CompactionTransactionCapture capture);

        void publish(
                KafkaPartitionStorage expectedStorage,
                int expectedLeaderEpoch,
                long durableOffset);
    }

    @FunctionalInterface
    public interface MaintenanceCapture {
        KafkaPartitionMaintenance.Capture capture();
    }

    @FunctionalInterface
    public interface CompactionCapture {
        KafkaPartitionMaintenance.CompactionState capture();
    }

    @FunctionalInterface
    public interface CompactionTransactionCapture {
        CompactionTransactionState capture();
    }

    public record CompactionConfiguration(
            MaterializationPolicy outputPolicy,
            long maxDecodedRecords,
            int maxKeyBytes,
            long maxInMemoryKeyBytes,
            KafkaCompactionPartitionPass.WriteSettings writeSettings
    ) {
        public CompactionConfiguration {
            Objects.requireNonNull(outputPolicy, "outputPolicy");
            Objects.requireNonNull(writeSettings, "writeSettings");
            if (maxDecodedRecords <= 0
                    || maxKeyBytes <= 0
                    || maxInMemoryKeyBytes <= 0) {
                throw new IllegalArgumentException(
                        "Kafka compaction capture limits must be positive");
            }
        }
    }

    public record CompactionTransactionState(
            KafkaProducerTransactionState producerState,
            Map<Long, LastRecord> activeProducerLastRecords
    ) {
        public CompactionTransactionState {
            Objects.requireNonNull(producerState, "producerState");
            activeProducerLastRecords =
                    Map.copyOf(
                            Objects.requireNonNull(
                                    activeProducerLastRecords,
                                    "activeProducerLastRecords"));
        }
    }
}
