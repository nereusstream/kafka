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

import kafka.server.nsip.KafkaBookKeeperEpochProbe;
import kafka.server.nsip.NativeKafkaRequest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.FetchResponse;

import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static kafka.server.nereus.KafkaBookKeeperAckRealTest.assertFetchedValues;
import static kafka.server.nereus.KafkaBookKeeperAckRealTest.delayedFetch;
import static kafka.server.nereus.KafkaBookKeeperAckRealTest.produce;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.OBJECT;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Checks the native wire semantics of Object WAL appends, Fetch and recovered leader epochs. */
final class KafkaObjectProtocolRealScenario {
    private KafkaObjectProtocolRealScenario() { }

    static void commitAndWake(int port, Uuid topic, KafkaProducer<String, String> producer) throws Exception {
        var topicName = Map.of(topic, OBJECT.topic());
        try (var uncommitted = new NativeKafkaRequest(port,
                    delayedFetch(OBJECT, topic, 2, IsolationLevel.READ_UNCOMMITTED, 0));
                var waiting = new NativeKafkaRequest(port,
                    delayedFetch(OBJECT, topic, 2, IsolationLevel.READ_COMMITTED, 15_000))) {
            var visible = ((FetchResponse) uncommitted.receive(5_000)).responseData(topicName, (short) 16).get(OBJECT);
            assertEquals(3, visible.highWatermark());
            assertFetchedValues(visible, List.of("committed"));
            assertThrows(SocketTimeoutException.class, () -> waiting.receive(250),
                    "Object WAL Fetch returned while its original transaction was open");
            producer.commitTransaction();
            var stable = ((FetchResponse) waiting.receive(5_000)).responseData(topicName, (short) 16).get(OBJECT);
            assertEquals(4, stable.highWatermark());
            assertEquals(4, stable.lastStableOffset());
            assertFetchedValues(stable, List.of("committed"));
            System.out.println("Native Object WAL delayed Fetch woke at the original transaction marker");
        }
    }

    static void assertAckZero(int port, Admin admin, long end) throws Exception {
        try (var request = new NativeKafkaRequest(port, produce((short) 0, "acks-0"))) {
            waitForCondition(() -> admin.listOffsets(Map.of(OBJECT, OffsetSpec.latest()))
                            .partitionResult(OBJECT).get(5, TimeUnit.SECONDS).offset() == end,
                    10_000, "Object WAL did not publish acks=0");
            assertThrows(SocketTimeoutException.class, () -> request.receive(250),
                    "Object WAL Produce sent a response for acks=0");
        }
    }

    static void assertRecoveredEpochs(int port, int first, int second, int current, long end) throws Exception {
        KafkaBookKeeperEpochProbe.assertEndOffset(port, OBJECT, current, first, 5);
        KafkaBookKeeperEpochProbe.assertEndOffset(port, OBJECT, current, second, 8);
        KafkaBookKeeperEpochProbe.assertEndOffset(port, OBJECT, current, current, end);
        KafkaBookKeeperEpochProbe.assertFenced(port, OBJECT, second);
    }
}
