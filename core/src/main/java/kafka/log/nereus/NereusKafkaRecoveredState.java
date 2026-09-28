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
import org.apache.kafka.storage.internals.log.LeaderEpochAwareRecoveryState;

import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentProtocolSnapshotV1;

import java.util.Objects;

/** Immutable shared-commit state installed after checkpoint and tail recovery. */
public record NereusKafkaRecoveredState(
        TopicPartition topicPartition, Uuid topicId, int leaderEpoch, KafkaCoherentProtocolSnapshotV1 sharedState)
        implements LeaderEpochAwareRecoveryState {
    public NereusKafkaRecoveredState {
        Objects.requireNonNull(topicPartition, "topicPartition");
        Objects.requireNonNull(topicId, "topicId");
        Objects.requireNonNull(sharedState, "sharedState");
        if (topicId.equals(Uuid.ZERO_UUID)
                || leaderEpoch != sharedState.root().fence().kafkaLeaderEpoch()
                || !topicPartition
                        .topic()
                        .equals(sharedState
                                .root()
                                .fence()
                                .topicIncarnation()
                                .topicName()
                                .value())
                || !topicId.equals(new Uuid(
                        sharedState
                                .root()
                                .fence()
                                .topicIncarnation()
                                .topicId()
                                .value()
                                .highBits(),
                        sharedState
                                .root()
                                .fence()
                                .topicIncarnation()
                                .topicId()
                                .value()
                                .lowBits()))
                || topicPartition.partition() != sharedState.root().fence().partitionId()) {
            throw new IllegalArgumentException("native recovered identity differs from the shared BK root");
        }
    }

    @Override
    public boolean frozen() {
        return true;
    }
}
