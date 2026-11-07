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
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.test.KafkaClusterTestKit;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

final class KafkaBookKeeperCoordinatorProbe {
    private KafkaBookKeeperCoordinatorProbe() { }

    static void assertNotReady(KafkaClusterTestKit cluster, int broker, TopicPartition partition, String groupId)
            throws Exception {
        var topic = new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName(partition.topic()).setPartitionIndexes(List.of(partition.partition()));
        var group = new OffsetFetchRequestData.OffsetFetchRequestGroup().setGroupId(groupId).setTopics(List.of(topic));
        var fetch = OffsetFetchRequest.Builder.forTopicNames(
                new OffsetFetchRequestData().setGroups(List.of(group)), true).build((short) 9);
        try (var wire = new NativeKafkaRequest(cluster, broker, fetch)) {
            var response = (OffsetFetchResponse) wire.receive(30_000);
            assertPending(response.group(groupId).errorCode(), "GroupCoordinator");
        }
        var initialize = new InitProducerIdRequest.Builder(new InitProducerIdRequestData()
                .setTransactionalId("pending-owner-probe").setTransactionTimeoutMs(30_000)).build((short) 4);
        try (var wire = new NativeKafkaRequest(cluster, broker, initialize)) {
            var response = (InitProducerIdResponse) wire.receive(30_000);
            assertPending(response.data().errorCode(), "TransactionCoordinator");
        }
    }

    private static void assertPending(short errorCode, String coordinator) {
        var error = Errors.forCode(errorCode);
        assertTrue(error == Errors.NOT_COORDINATOR || error == Errors.COORDINATOR_LOAD_IN_PROGRESS,
                coordinator + " returned " + error + " before native Owner recovery");
    }
}
