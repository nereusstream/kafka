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
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.LogSegments;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.internals.log.VerificationGuard;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.partition.KafkaPartitionState;
import com.nereusstream.kafka.partition.KafkaPartitionStorage;
import com.nereusstream.kafka.partition.KafkaStableSnapshot;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Objects;
import java.util.Optional;

/**
 * Per-partition stock UnifiedLog shell whose durable state is published only after exact Nereus recovery.
 *
 * <p>This slice deliberately rejects Produce and Fetch even after publication. It establishes the factory and
 * recovery/storage lifecycle boundary without allowing a stock local append fallback; the following data-plane slice
 * replaces those fail-closed methods with stable Nereus IO.
 */
public final class NereusUnifiedLog extends UnifiedLog {
    private final Object nereusGuard = new Object();
    private final KafkaPartitionIdentity identity;

    private NereusKafkaRecoveredState recoveredState;
    private KafkaPartitionStorage storage;

    private NereusUnifiedLog(
            Parts parts,
            org.apache.kafka.storage.log.metrics.BrokerTopicStats brokerTopicStats,
            int producerIdExpirationCheckIntervalMs,
            Optional<Uuid> topicId,
            KafkaPartitionIdentity identity,
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
                super.updateHighWatermark(state.stableEndOffset());
                recoveredState = state;
            }
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
            KafkaStableSnapshot snapshot = candidate.stableSnapshot();
            if (!matchesStorage(candidate, leaderEpoch, snapshot)) {
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
    public LogAppendInfo appendAsLeader(
            MemoryRecords records,
            int leaderEpoch,
            AppendOrigin origin,
            RequestLocal requestLocal,
            VerificationGuard verificationGuard,
            short transactionVersion
    ) {
        requirePublished(leaderEpoch);
        throw dataPlanePending("Produce");
    }

    @Override
    public LogAppendInfo appendAsFollower(MemoryRecords records, int leaderEpoch) {
        throw new KafkaStorageException(
                "Nereus authoritative storage does not accept Kafka follower appends");
    }

    @Override
    public FetchDataInfo read(
            long startOffset,
            int maxLength,
            FetchIsolation isolation,
            boolean minOneMessage
    ) {
        synchronized (nereusGuard) {
            if (storage == null || recoveredState == null) {
                throw new KafkaStorageException(
                        "Nereus partition storage is not recovered and published");
            }
        }
        throw dataPlanePending("Fetch");
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

    private void requireExactState(
            int leaderEpoch,
            NereusKafkaRecoveredState state
    ) {
        if (!state.frozen()
                || !state.identity().equals(identity)
                || state.leaderEpoch() != leaderEpoch
                || !state.topicPartition().equals(topicPartition())
                || !state.topicId().equals(topicId().orElse(Uuid.ZERO_UUID))) {
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
        LogSegment segment = LogSegment.open(
                dir,
                0L,
                config,
                time,
                config.initFileSize(),
                config.preallocate);
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
                logDirFailureChannel);
        org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache leaderEpochCache =
                UnifiedLog.createLeaderEpochCache(
                dir,
                topicPartition,
                logDirFailureChannel,
                Optional.empty(),
                scheduler);
        ProducerStateManager producerStateManager = new ProducerStateManager(
                topicPartition,
                dir,
                maxTransactionTimeoutMs,
                producerStateManagerConfig,
                time);
        return new Parts(localLog, leaderEpochCache, producerStateManager);
    }

    private static KafkaStorageException dataPlanePending(String operation) {
        return new KafkaStorageException(
                "Nereus native " + operation
                        + " data plane is not installed in this implementation slice");
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

    private record Parts(
            NereusLocalLog localLog,
            org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache leaderEpochCache,
            ProducerStateManager producerStateManager
    ) { }
}
