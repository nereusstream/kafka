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
package kafka.server.nsip;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.test.KafkaClusterTestKit;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class KafkaBookKeeperEpochProbe {
    private KafkaBookKeeperEpochProbe() { }

    static void assertEndOffset(KafkaClusterTestKit cluster, int broker, TopicPartition partition,
                                int currentEpoch, int epoch, long endOffset) throws Exception {
        var result = epochEnd(cluster, broker, partition, currentEpoch, epoch);
        assertEquals(Errors.NONE.code(), result.errorCode());
        assertEquals(epoch, result.leaderEpoch());
        assertEquals(endOffset, result.endOffset());
    }

    static void assertFenced(KafkaClusterTestKit cluster, int broker, TopicPartition partition, int staleEpoch)
            throws Exception {
        var result = epochEnd(cluster, broker, partition, staleEpoch, staleEpoch);
        assertEquals(Errors.FENCED_LEADER_EPOCH.code(), result.errorCode());
        assertEquals(OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET, result.endOffset());
    }

    private static EpochEndOffset epochEnd(KafkaClusterTestKit cluster, int broker, TopicPartition partition,
                                           int currentEpoch, int epoch) throws Exception {
        var topics = new OffsetForLeaderTopicCollection();
        topics.add(new OffsetForLeaderTopic().setTopic(partition.topic())
                .setPartitions(List.of(new OffsetForLeaderPartition().setPartition(partition.partition())
                        .setCurrentLeaderEpoch(currentEpoch).setLeaderEpoch(epoch))));
        var request = OffsetsForLeaderEpochRequest.Builder.forConsumer(topics).build();
        try (var wire = new NativeKafkaRequest(cluster, broker, request)) {
            var result = (OffsetsForLeaderEpochResponse) wire.receive(30_000);
            var topic = result.data().topics().iterator().next();
            assertEquals(partition.topic(), topic.topic());
            var response = topic.partitions().get(0);
            assertEquals(partition.partition(), response.partition());
            return response;
        }
    }
}
