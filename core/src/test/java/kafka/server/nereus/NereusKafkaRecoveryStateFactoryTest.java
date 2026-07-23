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
import kafka.server.ReplicaManager;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import com.nereusstream.api.AppendAuthority;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.recovery.KafkaCheckpointRecoveryRequest;
import com.nereusstream.kafka.recovery.KafkaPartitionRecoveryRequest;
import com.nereusstream.kafka.recovery.KafkaRecoveredPartition;
import com.nereusstream.kafka.recovery.KafkaRecoveryState;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Optional;

import scala.Option;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NereusKafkaRecoveryStateFactoryTest {
    private static final Uuid TOPIC_ID =
            Uuid.fromString("AAAAAAAAAAAAAAAAAAAAAQ");
    private static final KafkaPartitionIdentity IDENTITY =
            new KafkaPartitionIdentity(
                    "kraft-cluster",
                    TOPIC_ID.toString(),
                    0,
                    "events");

    @Test
    void createsFreshStateAndPublishesOnlyToTheExactLeaderPartition() {
        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);
        Partition partition = Mockito.mock(Partition.class);
        TopicPartition topicPartition = new TopicPartition("events", 0);
        when(replicaManager.onlinePartition(topicPartition))
                .thenReturn(Option.apply(partition));
        when(partition.topic()).thenReturn("events");
        when(partition.partitionId()).thenReturn(0);
        when(partition.topicId()).thenReturn(Option.apply(TOPIC_ID));
        when(partition.isLeader()).thenReturn(true);
        when(partition.getLeaderEpoch()).thenReturn(7);
        KafkaCheckpointSourceState source = source(7);
        KafkaPartitionRecoveryRequest request = request(source);
        NereusKafkaRecoveryStateFactory factory =
                new NereusKafkaRecoveryStateFactory(replicaManager);

        KafkaRecoveryState<NereusKafkaRecoveredState> recovery =
                factory.create(request);
        NereusKafkaRecoveredState state = recovery.codec().freshState();
        recovery.codec().validateRecoveredState(state, source);
        KafkaRecoveredPartition<NereusKafkaRecoveredState> recovered =
                new KafkaRecoveredPartition<>(
                        state,
                        source,
                        0,
                        0,
                        0,
                        Optional.empty());

        recovery.publisher().publish(recovered).join();

        verify(partition).installNereusRecoveredState(7, state);
        assertSame(IDENTITY, state.identity());
    }

    @Test
    void rejectsAStaleOrNonLeaderReplicaManagerPartition() {
        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);
        Partition partition = Mockito.mock(Partition.class);
        TopicPartition topicPartition = new TopicPartition("events", 0);
        when(replicaManager.onlinePartition(topicPartition))
                .thenReturn(Option.apply(partition));
        when(partition.topic()).thenReturn("events");
        when(partition.partitionId()).thenReturn(0);
        when(partition.topicId()).thenReturn(Option.apply(TOPIC_ID));
        when(partition.isLeader()).thenReturn(true);
        when(partition.getLeaderEpoch()).thenReturn(8);

        assertThrows(
                NereusException.class,
                () -> new NereusKafkaRecoveryStateFactory(replicaManager)
                        .create(request(source(7))));
    }

    private static KafkaPartitionRecoveryRequest request(
            KafkaCheckpointSourceState source
    ) {
        KafkaCheckpointRecoveryRequest checkpoint =
                Mockito.mock(KafkaCheckpointRecoveryRequest.class);
        when(checkpoint.identity()).thenReturn(IDENTITY);
        when(checkpoint.currentSource()).thenReturn(source);
        return new KafkaPartitionRecoveryRequest(
                checkpoint,
                Duration.ofSeconds(5));
    }

    private static KafkaCheckpointSourceState source(int leaderEpoch) {
        return new KafkaCheckpointSourceState(
                new AppendAuthority(
                        "kafka-partition-leader-v1",
                        IDENTITY.durableId().canonicalIdentity(),
                        leaderEpoch,
                        "broker-1",
                        9),
                "writer-1",
                1,
                "fencing-token",
                1,
                0,
                0,
                1,
                "commit-1",
                new Checksum(ChecksumType.SHA256, "a".repeat(64)),
                false,
                0);
    }
}
