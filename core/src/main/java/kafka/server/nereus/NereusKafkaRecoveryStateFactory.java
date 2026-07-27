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

import kafka.cluster.Partition;
import kafka.log.nereus.NereusKafkaRecoveredState;
import kafka.log.nereus.NereusKafkaRecoveryStateCodec;
import kafka.log.nereus.NereusProducerStateManager;
import kafka.log.nereus.NereusUnifiedLog;
import kafka.server.ReplicaManager;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.recovery.KafkaPartitionRecoveryRequest;
import com.nereusstream.kafka.recovery.KafkaRecoveryState;
import com.nereusstream.kafka.recovery.KafkaRecoveryStateFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Creates fresh stock-RecordBatch-derived M3 state and publishes it into the exact live Partition. */
public final class NereusKafkaRecoveryStateFactory
        implements KafkaRecoveryStateFactory {
    private final ReplicaManager replicaManager;

    public NereusKafkaRecoveryStateFactory(ReplicaManager replicaManager) {
        this.replicaManager = Objects.requireNonNull(
                replicaManager, "replicaManager");
    }

    @Override
    public KafkaRecoveryState<NereusKafkaRecoveredState> create(
            KafkaPartitionRecoveryRequest request
    ) {
        Objects.requireNonNull(request, "request");
        KafkaPartitionIdentity identity =
                request.checkpointRequest().identity();
        KafkaCheckpointSourceState source =
                request.checkpointRequest().currentSource();
        int leaderEpoch;
        try {
            leaderEpoch = Math.toIntExact(source.authority().authorityEpoch());
        } catch (ArithmeticException failure) {
            throw invariant("Kafka recovery leader epoch does not fit Kafka int");
        }
        TopicPartition topicPartition = new TopicPartition(
                identity.observedTopicName(),
                identity.partition());
        Partition partition = replicaManager.onlinePartition(topicPartition)
                .getOrElse(() -> {
                    throw invariant(
                            "Kafka recovery target is not an online ReplicaManager partition");
                });
        requireExactPartition(partition, identity, leaderEpoch);
        if (!(partition.localLogOrException() instanceof NereusUnifiedLog nereusLog)) {
            throw invariant(
                    "Kafka recovery target does not own a Nereus UnifiedLog");
        }
        NereusProducerStateManager producerStateManager =
                nereusLog.prepareProducerRecovery(
                        leaderEpoch, source.trimOffset());
        NereusKafkaRecoveryStateCodec codec =
                new NereusKafkaRecoveryStateCodec(
                        identity,
                        leaderEpoch,
                        source.trimOffset(),
                        source.endOffset(),
                        producerStateManager);
        return new KafkaRecoveryState<>(
                codec,
                recovered -> publish(
                        partition, identity, leaderEpoch, recovered.state()));
    }

    private static CompletableFuture<Void> publish(
            Partition partition,
            KafkaPartitionIdentity identity,
            int leaderEpoch,
            NereusKafkaRecoveredState state
    ) {
        NereusUnifiedLog log = null;
        try {
            if (!state.identity().equals(identity)
                    || state.leaderEpoch() != leaderEpoch
                    || !state.frozen()) {
                throw invariant("Kafka recovery publisher received mismatched state");
            }
            if (!(partition.localLogOrException() instanceof NereusUnifiedLog nereusLog)) {
                throw invariant(
                        "Kafka recovery target does not own a Nereus UnifiedLog");
            }
            log = nereusLog;
            log.installRecoveredState(leaderEpoch, state);
            partition.installNereusRecoveredState(leaderEpoch, state);
            return CompletableFuture.completedFuture(null);
        } catch (Throwable failure) {
            if (log != null) {
                try {
                    log.removeStorage(leaderEpoch, null);
                } catch (Throwable cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
            }
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static void requireExactPartition(
            Partition partition,
            KafkaPartitionIdentity identity,
            int leaderEpoch
    ) {
        Uuid expectedTopicId;
        try {
            expectedTopicId = Uuid.fromString(identity.topicId());
        } catch (Throwable failure) {
            throw invariant("Kafka recovery identity has an invalid Kafka topic ID");
        }
        if (!partition.topic().equals(identity.observedTopicName())
                || partition.partitionId() != identity.partition()
                || !partition.topicId().contains(expectedTopicId)
                || !partition.isLeader()
                || partition.getLeaderEpoch() != leaderEpoch) {
            throw fenced("Kafka recovery target is not the exact current leader partition");
        }
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
}
