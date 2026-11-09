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

import kafka.server.nsip.NativeKafkaRequest;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;

import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static kafka.server.nereus.KafkaBookKeeperAckRealTest.assertFetchedValues;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.OBJECT;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.awaitInitialOwner;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.topic;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the minimum byte, deadline and topic deletion exits of a two-partition Object WAL Fetch. */
final class KafkaObjectFetchExitRealScenario {
    private static final short VERSION = 16;
    private static final TopicPartition PEER = new TopicPartition("native-waiter-peer", 0);

    private KafkaObjectFetchExitRealScenario() { }

    static void verify(int port, Uuid original, Properties client, Admin admin, long end, String bucket)
            throws Exception {
        var created = admin.createTopics(List.of(topic(PEER, "OBJECT_WAL")));
        var peer = created.topicId(PEER.topic()).get(30, TimeUnit.SECONDS);
        awaitInitialOwner(admin, PEER);
        var names = Map.of(original, OBJECT.topic(), peer, PEER.topic());
        try (var producer = new KafkaProducer<String, String>(client)) {
            producer.partitionsFor(PEER.topic());
            try (var wire = new NativeKafkaRequest(port, fetch(original, end, peer, 0, 5_000, 4_096))) {
                assertEquals(0, producer.send(new ProducerRecord<>(PEER.topic(), 0, "key", "short"))
                        .get(20, TimeUnit.SECONDS).offset());
                assertThrows(SocketTimeoutException.class, () -> wire.receive(250),
                        "The multi-partition Fetch completed below its minimum bytes");
                var response = ((FetchResponse) wire.receive(6_000)).responseData(names, VERSION);
                assertEquals(2, response.size());
                assertEquals(end, response.get(OBJECT).highWatermark());
                assertFetchedValues(response.get(OBJECT), List.of());
                assertEquals(1, response.get(PEER).highWatermark());
                assertFetchedValues(response.get(PEER), List.of("short"));
            }
        }
        try (var provider = KafkaObjectFaultRelay.wholeBucket(bucket)) {
            provider.arm("bucket-unavailable");
            try (var wire = new NativeKafkaRequest(port, fetch(original, end, peer, 1, 15_000, 1))) {
                assertThrows(SocketTimeoutException.class, () -> wire.receive(250),
                        "The multi-partition Fetch completed before deletion");
                admin.deleteTopics(List.of(PEER.topic())).all().get(5, TimeUnit.SECONDS);
                var response = ((FetchResponse) wire.receive(5_000)).responseData(names, VERSION);
                assertEquals(2, response.size());
                assertEquals(end, response.get(OBJECT).highWatermark());
                assertFetchedValues(response.get(OBJECT), List.of());
                var error = Errors.forCode(response.get(PEER).errorCode());
                assertTrue(error == Errors.UNKNOWN_TOPIC_OR_PARTITION || error == Errors.UNKNOWN_TOPIC_ID
                                || error == Errors.NOT_LEADER_OR_FOLLOWER,
                        "Deleted Object Fetch partition returned " + error);
                assertEquals(0, provider.hits(), "The end-of-log delayed Fetch contacted the Object Provider");
            }
        }
    }

    private static FetchRequest fetch(Uuid original, long end, Uuid peer, long peerOffset, int wait, int minimumBytes) {
        return FetchRequest.Builder.forConsumer(VERSION, wait, minimumBytes,
                        Map.of(OBJECT, partition(original, end), PEER, partition(peer, peerOffset)))
                .isolationLevel(IsolationLevel.READ_COMMITTED).build(VERSION);
    }

    private static FetchRequest.PartitionData partition(Uuid topic, long offset) {
        return new FetchRequest.PartitionData(topic, offset, FetchRequest.INVALID_LOG_START_OFFSET,
                1 << 20, Optional.empty());
    }
}
