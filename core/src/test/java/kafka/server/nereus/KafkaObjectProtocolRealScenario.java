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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;

import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
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
        commitAndWake(port, topic, producer, 2);
    }

    static void commitAndWake(int port, Uuid topic, KafkaProducer<String, String> producer, long offset) throws Exception {
        var topicName = Map.of(topic, OBJECT.topic());
        try (var uncommitted = new NativeKafkaRequest(port,
                    delayedFetch(OBJECT, topic, offset, IsolationLevel.READ_UNCOMMITTED, 0));
                var waiting = new NativeKafkaRequest(port,
                    delayedFetch(OBJECT, topic, offset, IsolationLevel.READ_COMMITTED, 15_000))) {
            var visible = ((FetchResponse) uncommitted.receive(5_000)).responseData(topicName, (short) 16).get(OBJECT);
            assertEquals(offset + 1, visible.highWatermark());
            assertFetchedValues(visible, List.of("committed"));
            assertThrows(SocketTimeoutException.class, () -> waiting.receive(250),
                    "Object WAL Fetch returned while its original transaction was open");
            producer.commitTransaction();
            var stable = ((FetchResponse) waiting.receive(5_000)).responseData(topicName, (short) 16).get(OBJECT);
            assertEquals(offset + 2, stable.highWatermark());
            assertEquals(offset + 2, stable.lastStableOffset());
            assertFetchedValues(stable, List.of("committed"));
            System.out.println("Native delayed Fetch woke at the original transaction marker");
        }
    }

    static void assertAckZero(int port, Admin admin, long end) throws Exception {
        try (var request = new NativeKafkaRequest(port, produce((short) 0, "acks-0"))) {
            awaitEnd(admin, end);
            assertThrows(SocketTimeoutException.class, () -> request.receive(250),
                    "Object WAL Produce sent a response for acks=0");
        }
    }

    static ProduceRequest recoverLostProduceResponse(int port, Admin admin) throws Exception {
        return recoverLostProduceResponse(port, admin, 1, "before-1");
    }

    static ProduceRequest recoverLostProduceResponse(int port, Admin admin, long offset, String value) throws Exception {
        var request = new InitProducerIdRequest.Builder(new InitProducerIdRequestData()
                .setTransactionalId(null).setTransactionTimeoutMs(30_000)).build((short) 4);
        InitProducerIdResponse init;
        try (var wire = new NativeKafkaRequest(port, request)) {
            init = (InitProducerIdResponse) wire.receive(30_000);
        }
        assertEquals(Errors.NONE.code(), init.data().errorCode());
        long pid = init.data().producerId();
        short epoch = init.data().producerEpoch();
        new NativeKafkaRequest(port, withProducerIdentity(pid, epoch, value)).close();
        awaitEnd(admin, offset + 1);
        var changedPayload = withProducerIdentity(pid, epoch, "duplicate-must-not-appear");
        assertProduceOffset(port, changedPayload, offset);
        awaitEnd(admin, offset + 1);
        return changedPayload;
    }

    private static ProduceRequest withProducerIdentity(long pid, short epoch, String value) {
        var records = MemoryRecords.withIdempotentRecords(Compression.NONE, pid, epoch, 0,
                new SimpleRecord("key".getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8)));
        var topics = new ProduceRequestData.TopicProduceDataCollection();
        topics.add(new ProduceRequestData.TopicProduceData().setName(OBJECT.topic()).setPartitionData(
                List.of(new ProduceRequestData.PartitionProduceData().setIndex(OBJECT.partition()).setRecords(records))));
        return new ProduceRequest(new ProduceRequestData().setAcks((short) -1).setTimeoutMs(10_000).setTopicData(topics),
                (short) 9);
    }

    static void verifyNonIdempotentRetry(int port, Admin admin, long original) throws Exception {
        var request = produce((short) -1, "non-idempotent-retry");
        new NativeKafkaRequest(port, request).close();
        awaitEnd(admin, original + 1);
        assertProduceOffset(port, request, original + 1);
        awaitEnd(admin, original + 2);
    }

    static void assertProduceOffset(int port, ProduceRequest request, long offset) throws Exception {
        try (var wire = new NativeKafkaRequest(port, request)) {
            var response = (ProduceResponse) wire.receive(30_000);
            var partition = response.data().responses().iterator().next().partitionResponses().get(0);
            assertEquals(Errors.NONE.code(), partition.errorCode());
            assertEquals(offset, partition.baseOffset());
        }
    }

    private static void awaitEnd(Admin admin, long end) throws Exception {
        waitForCondition(() -> admin.listOffsets(Map.of(OBJECT, OffsetSpec.latest()))
                        .partitionResult(OBJECT).get(5, TimeUnit.SECONDS).offset() == end,
                10_000, "Object WAL did not publish durable end " + end);
    }

    static void verifyCompressedWireBatches(int port, Uuid topic, long start) throws Exception {
        appendCompressed(port, start, "first-0", "first-1");
        appendCompressed(port, start + 2, "second-0", "second-1");
        appendCompressed(port, start + 4, "third-0", "third-1");
        KafkaBookKeeperAckRealTest.assertWireBatch(port, topic, start + 1, start, start + 6,
                List.of("first-0", "first-1"));
        KafkaBookKeeperAckRealTest.assertWireBatch(port, topic, start + 2, start + 2, start + 6,
                List.of("second-0", "second-1"));
        appendCompressed(port, start + 6, "fourth-0", "fourth-1");
        KafkaBookKeeperAckRealTest.assertWireBatch(port, topic, start + 4, start + 4, start + 8,
                List.of("third-0", "third-1"));
        KafkaBookKeeperAckRealTest.assertWireBatch(port, topic, start + 6, start + 6, start + 8,
                List.of("fourth-0", "fourth-1"));
    }

    private static void appendCompressed(int port, long offset, String first, String second) throws Exception {
        var records = KafkaBookKeeperAckRealTest.compressed(first, second);
        assertProduceOffset(port, produce((short) -1, records), offset);
    }

    static void assertRecoveredEpochs(int port, int first, int second, int current, long end) throws Exception {
        KafkaBookKeeperEpochProbe.assertEndOffset(port, OBJECT, current, first, 5);
        KafkaBookKeeperEpochProbe.assertEndOffset(port, OBJECT, current, second, 8);
        KafkaBookKeeperEpochProbe.assertEndOffset(port, OBJECT, current, current, end);
        KafkaBookKeeperEpochProbe.assertFenced(port, OBJECT, second);
    }
}
