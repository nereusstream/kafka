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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.test.TestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A single Fetch request remains pending at LSO until the transaction commits. */
final class KafkaBookKeeperFetchProbe {
    private static final short FETCH_VERSION = 12;
    private static final int MAX_WAIT_MS = 20_000;

    private KafkaBookKeeperFetchProbe() {}

    static void commitAndVerifyFetch(
            KafkaClusterTestKit cluster,
            int broker,
            Uuid topicId,
            TopicPartition partition,
            KafkaProducer<String, String> producer,
            List<String> expected) throws Exception {
        var fetch = FetchRequest.Builder.forConsumer(FETCH_VERSION, MAX_WAIT_MS, 1,
                        Map.of(partition, new FetchRequest.PartitionData(
                                topicId, 0, FetchRequest.INVALID_LOG_START_OFFSET, 1 << 20, Optional.empty())))
                .isolationLevel(IsolationLevel.READ_COMMITTED)
                .build(FETCH_VERSION);
        var purgatory = cluster.brokers().get(broker).replicaManager().delayedFetchPurgatory();
        assertEquals(0, purgatory.numDelayed(), "no other client should have a delayed Fetch");
        long start = System.nanoTime();
        try (var wire = new NativeKafkaRequest(cluster, broker, fetch)) {
            TestUtils.waitForCondition(() -> purgatory.numDelayed() == 1, 5_000,
                    "read_committed Fetch did not wait at the open transaction");
            producer.commitTransaction();
            // The first and only request must return before Kafka's delayed Fetch timer fires.
            var response = (FetchResponse) wire.receive(5_000);
            assertTrue(System.nanoTime() - start < TimeUnit.MILLISECONDS.toNanos(MAX_WAIT_MS),
                    "Fetch returned only when its wait expired");
            var result = response.responseData(Map.of(topicId, partition.topic()), FETCH_VERSION).get(partition);
            assertEquals(Errors.NONE.code(), result.errorCode());
            assertEquals(expected.size() + 1, result.highWatermark());
            assertEquals(result.highWatermark(), result.lastStableOffset());
            var offsets = new ArrayList<Long>();
            var actual = new ArrayList<String>();
            for (var batch : FetchResponse.recordsOrFail(result).batches()) {
                if (batch.isControlBatch()) {
                    continue;
                }
                for (var record : batch) {
                    offsets.add(record.offset());
                    actual.add(java.nio.charset.StandardCharsets.UTF_8.decode(record.value()).toString());
                }
            }
            assertEquals(expected, actual);
            assertEquals(java.util.stream.LongStream.range(0, expected.size()).boxed().toList(), offsets);
        }
    }
}
