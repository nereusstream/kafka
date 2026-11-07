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

import kafka.server.nsip.KafkaBookKeeperOwnerRelocationRealTest;
import kafka.server.nsip.NativeKafkaRequest;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class KafkaObjectCompactionRealScenario {
    static final int SEALED_TRANSACTION_END = 11;
    private static final String TOPIC = "native-cold";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    private static final short FETCH_VERSION = 16;

    private KafkaObjectCompactionRealScenario() { }

    static int seed(KafkaClusterTestKit cluster, Properties client) throws Exception {
        long now = System.currentTimeMillis();
        IdempotentSample.seed(cluster, now);
        append(cluster, 3, row(now + 1, "replace", "new-three"), row(now + 1, "replace-2", "new-four"));
        append(cluster, 5, row(now + 2, "gone", "old-five"));
        append(cluster, 6, row(now + 3, "gone", null));
        return seedTransactions(client);
    }

    private static int seedTransactions(Properties client) throws Exception {
        try (var admin = Admin.create(client)) {
            var transactionState = new NewTopic("__transaction_state", Map.of(0, List.of(0)))
                    .configs(Map.of("cleanup.policy", "compact"));
            admin.createTopics(List.of(transactionState)).all().get(30, TimeUnit.SECONDS);
        }
        var settings = new Properties();
        settings.putAll(client);
        settings.put("key.serializer", StringSerializer.class.getName());
        settings.put("value.serializer", StringSerializer.class.getName());
        settings.put("enable.idempotence", "true");
        settings.put("acks", "all");
        settings.put("transactional.id", "m6-object-semantic");
        try (var producer = new KafkaProducer<String, String>(settings)) {
            producer.initTransactions();
            producer.beginTransaction();
            assertEquals(7L, producer.send(new ProducerRecord<>(TOPIC, 0, "committed-key", "committed-seven"))
                    .get(30, TimeUnit.SECONDS).offset());
            producer.commitTransaction();
            producer.beginTransaction();
            assertEquals(9L, producer.send(new ProducerRecord<>(TOPIC, 0, "aborted-key", "aborted-nine"))
                    .get(30, TimeUnit.SECONDS).offset());
            producer.abortTransaction();
        }
        return SEALED_TRANSACTION_END;
    }

    private static SimpleRecord row(long timestamp, String key, String value) {
        return new SimpleRecord(timestamp, key.getBytes(StandardCharsets.UTF_8),
                value == null ? null : value.getBytes(StandardCharsets.UTF_8));
    }

    private static void append(KafkaClusterTestKit cluster, long offset, SimpleRecord... rows) throws Exception {
        append(cluster, 0, offset, MemoryRecords.withRecords(Compression.NONE, rows));
    }

    private static void append(KafkaClusterTestKit cluster, int broker, long offset, MemoryRecords raw) throws Exception {
        var topic = new ProduceRequestData.TopicProduceData().setName(TOPIC)
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(raw)));
        var topics = new ProduceRequestData.TopicProduceDataCollection();
        topics.add(topic);
        var request = new ProduceRequest(new ProduceRequestData().setAcks((short) -1).setTimeoutMs(10_000).setTopicData(topics),
                (short) 9);
        try (var wire = new NativeKafkaRequest(cluster, broker, request)) {
            var response = (ProduceResponse) wire.receive(30_000);
            var data = response.data().responses().iterator().next().partitionResponses().get(0);
            assertEquals(Errors.NONE, Errors.forCode(data.errorCode()));
            assertEquals(offset, data.baseOffset());
        }
    }

    static void verify(KafkaClusterTestKit cluster, Uuid topic, int broker, int end, Properties client) throws Exception {
        // The provider can finish before the Controller acknowledges RECOVERED. The official Admin client
        // retries the broker's recovery response; the wire Fetches below then inspect the exact ready owner.
        try (var admin = Admin.create(client)) {
            assertEquals(1, admin.listOffsets(Map.of(PARTITION, OffsetSpec.forTimestamp(0)))
                    .partitionResult(PARTITION).get(30, TimeUnit.SECONDS).offset());
        }

        var first = fetch(cluster, topic, broker, 0);
        assertEquals(Errors.NONE, Errors.forCode(first.errorCode()));
        assertEquals(end, first.highWatermark());
        assertEquals(end, first.lastStableOffset());
        var batch = FetchResponse.recordsOrFail(first).batches().iterator().next();
        assertEquals(0, batch.baseOffset());
        assertEquals(2, batch.lastOffset());
        var offsets = new ArrayList<Long>();
        batch.forEach(record -> offsets.add(record.offset()));
        assertEquals(List.of(1L), offsets, "the rewritten batch did not preserve its original sparse offsets");
        IdempotentSample.verifyReplay(cluster, broker, batch);

        var gap = fetch(cluster, topic, broker, 5);
        assertEquals(Errors.NONE, Errors.forCode(gap.errorCode()));
        assertEquals(end, gap.highWatermark(), "retry of the original producer batch allocated another offset");
        var tombstoneBatch = FetchResponse.recordsOrFail(gap).batches().iterator().next();
        var tombstone = tombstoneBatch.iterator().next();
        assertEquals(6, tombstone.offset(), "Fetch did not skip the fully compacted batch");
        assertNull(tombstone.value(), "compaction discarded or rewrote the latest tombstone");
        assertTrue(tombstoneBatch.deleteHorizonMs().orElseThrow() > tombstone.timestamp());

        verifyTransactions(cluster, topic, broker, end, client);
    }

    private static void verifyTransactions(KafkaClusterTestKit cluster, Uuid topic, int broker, int end, Properties client)
            throws Exception {
        verifyMarker(cluster, topic, broker, 8, ControlRecordType.COMMIT);
        verifyMarker(cluster, topic, broker, 10, ControlRecordType.ABORT);

        var aborted = fetch(cluster, topic, broker, 9);
        assertEquals(Errors.NONE, Errors.forCode(aborted.errorCode()));
        var data = FetchResponse.recordsOrFail(aborted).batches().iterator().next();
        assertTrue(data.isTransactional());
        assertEquals(9, data.baseOffset());
        assertEquals(List.of(9L), aborted.abortedTransactions().stream().map(row -> row.firstOffset()).toList());
        assertEquals(data.producerId(), aborted.abortedTransactions().get(0).producerId());

        var visible = new ArrayList<>(Arrays.asList("middle", "new-three", "new-four", null, "committed-seven"));
        for (int offset = SEALED_TRANSACTION_END; offset < end; offset++) visible.add("input-" + offset);
        assertEquals(visible, KafkaBookKeeperOwnerRelocationRealTest.consume(client, visible.size()));
    }

    private static void verifyMarker(KafkaClusterTestKit cluster, Uuid topic, int broker, long offset, ControlRecordType kind)
            throws Exception {
        var response = fetch(cluster, topic, broker, offset);
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));
        var batch = FetchResponse.recordsOrFail(response).batches().iterator().next();
        assertTrue(batch.isControlBatch());
        assertEquals(offset, batch.baseOffset());
        assertEquals(kind, ControlRecordType.parse(batch.iterator().next().key()));
    }

    private static final class IdempotentSample {
        private static void seed(KafkaClusterTestKit cluster, long timestamp) throws Exception {
            var request = new InitProducerIdRequest.Builder(new InitProducerIdRequestData()
                    .setTransactionalId(null).setTransactionTimeoutMs(30_000)).build((short) 4);
            try (var wire = new NativeKafkaRequest(cluster, 0, request)) {
                var producer = ((InitProducerIdResponse) wire.receive(30_000)).data();
                assertEquals(Errors.NONE, Errors.forCode(producer.errorCode()));
                append(cluster, 0, 0, records(producer.producerId(), producer.producerEpoch(), timestamp));
            }
        }

        private static void verifyReplay(KafkaClusterTestKit cluster, int broker, RecordBatch compacted) throws Exception {
            assertTrue(compacted.hasProducerId());
            assertEquals(0, compacted.baseSequence());
            assertEquals(2, compacted.lastSequence());
            assertEquals(1, compacted.iterator().next().sequence());
            append(cluster, broker, 0, records(compacted.producerId(), compacted.producerEpoch(),
                    compacted.iterator().next().timestamp()));
        }

        private static MemoryRecords records(long producerId, short producerEpoch, long timestamp) {
            return MemoryRecords.withIdempotentRecords(Compression.NONE, producerId, producerEpoch, 0,
                    row(timestamp, "replace", "old-zero"), row(timestamp, "keep", "middle"),
                    row(timestamp, "replace-2", "old-two"));
        }
    }

    static void verifyMissingFailsClosed(KafkaClusterTestKit cluster, Uuid topic, int broker) throws Exception {
        var missing = fetch(cluster, topic, broker, 0);
        assertEquals(Errors.KAFKA_STORAGE_ERROR, Errors.forCode(missing.errorCode()),
                "an unfiltered BK replay exposed the compacted prefix");
    }

    private static org.apache.kafka.common.message.FetchResponseData.PartitionData fetch(
            KafkaClusterTestKit cluster, Uuid topic, int broker, long offset) throws Exception {
        var request = FetchRequest.Builder.forConsumer(FETCH_VERSION, 0, 0, Map.of(PARTITION,
                new FetchRequest.PartitionData(topic, offset, FetchRequest.INVALID_LOG_START_OFFSET, 1, Optional.empty())))
                .setMaxBytes(1).isolationLevel(IsolationLevel.READ_COMMITTED).build(FETCH_VERSION);
        try (var wire = new NativeKafkaRequest(cluster, broker, request)) {
            return ((FetchResponse) wire.receive(30_000)).responseData(Map.of(topic, TOPIC), FETCH_VERSION).get(PARTITION);
        }
    }
}
