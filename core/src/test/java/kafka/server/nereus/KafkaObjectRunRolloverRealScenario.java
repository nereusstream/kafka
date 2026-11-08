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
import kafka.server.nsip.KafkaBookKeeperProcessRealTest.NativeNode;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.server.config.NereusKafkaConfigs;

import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import io.oxia.client.api.AsyncOxiaClient;

import static kafka.server.nereus.KafkaObjectWalProcessRealTest.OBJECT;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.authorization;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.scope;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Keeps one native Fetch in the exact old physical Run while the next append asks to roll it. */
final class KafkaObjectRunRolloverRealScenario {
    private KafkaObjectRunRolloverRealScenario() { }

    static KafkaObjectFaultRelay.PutTrace stopWithLostCheckpoint(NativeNode broker, String bucket,
            KafkaObjectWalAdapterRealTest.NativeTerminal terminal) throws Exception {
        try (var relay = KafkaObjectFaultRelay.lostPut(bucket, terminal.checkpointPrefix() + "/")) {
            relay.arm("first-kafka-checkpoint-put-response-lost");
            var shutdown = CompletableFuture.runAsync(() -> {
                try {
                    broker.stop();
                } catch (Exception failure) {
                    throw new CompletionException(failure);
                }
            });
            String version = relay.awaitAppliedPut();
            assertFalse(shutdown.isDone(), "Broker exited with its original Kafka checkpoint response held");
            relay.losePutResponses();
            shutdown.get(30, TimeUnit.SECONDS);
            var trace = relay.originalPutTrace();
            assertEquals(version, trace.version());
            return trace;
        }
    }

    static void fillAndReadBeforeRollover(Properties client, int port, long boundary,
            KafkaObjectWalAdapterRealTest.Provisioned fixture, AsyncOxiaClient control, Uuid topicId) throws Exception {
        try (var producer = new KafkaProducer<String, String>(client)) {
            for (long offset = 9; offset < boundary; offset++) {
                assertEquals(offset, producer.send(new ProducerRecord<>(OBJECT.topic(), 0, "key", "run-" + offset))
                        .get(30, TimeUnit.SECONDS).offset());
            }
            var oldRun = KafkaObjectWalAdapterRealTest.NativeTerminal.capture(fixture, control, topicId, OBJECT.topic(), 0);
            var selected = authorization(oldRun.metadata(), oldRun.ref().shardId()).read(scope(fixture, topicId)).orElseThrow();
            var grant = selected.grants().get(selected.grants().size() - 1);
            assertEquals(boundary - 1, grant.locator().startOffset());
            assertEquals(boundary, grant.locator().endOffsetExclusive());
            assertEquals(oldRun.ref().rootSha256(), grant.locator().extent().walRunRootSha());
            var bucket = (String) fixture.settings().get(NereusKafkaConfigs.OBJECT_BUCKET_CONFIG);
            try (var relay = new KafkaObjectFaultRelay(bucket, grant.object().key(), "*")) {
                relay.arm("held-unversioned");
                verifyPinnedFetch(port, topicId, boundary, relay);
            }
            assertEquals(boundary, producer.send(new ProducerRecord<>(OBJECT.topic(), 0, "key", "run-" + boundary))
                    .get(30, TimeUnit.SECONDS).offset());
        }
    }

    private static void verifyPinnedFetch(int port, Uuid topicId, long boundary, KafkaObjectFaultRelay relay) throws Exception {
        short version = 16;
        var fetch = FetchRequest.Builder.forConsumer(version, 0, 1,
                        Map.of(OBJECT, new FetchRequest.PartitionData(topicId, boundary - 1,
                                FetchRequest.INVALID_LOG_START_OFFSET, 4096, Optional.empty())))
                .setMaxBytes(4096).isolationLevel(IsolationLevel.READ_COMMITTED).build(version);
        try (var request = new NativeKafkaRequest(port, fetch)) {
            relay.awaitAppliedRange();
            assertThrows(SocketTimeoutException.class, () -> request.receive(200),
                    "The Fetch completed while the original range response was held");
            assertNeedsRollover(port);
            relay.release();
            var response = ((FetchResponse) request.receive(10_000)).responseData(Map.of(topicId, OBJECT.topic()), version)
                    .get(OBJECT);
            assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));
            assertEquals(boundary, response.highWatermark());
            assertEquals(boundary, response.lastStableOffset());
            var records = FetchResponse.recordsOrFail(response).records().iterator();
            var record = records.next();
            assertEquals(boundary - 1, record.offset());
            assertEquals("run-" + (boundary - 1), StandardCharsets.UTF_8.decode(record.value()).toString());
            assertFalse(records.hasNext(), "The in-flight Fetch exposed a later physical Run");
            System.out.printf("Native Object Fetch drained exact old Run through offset %d%n", boundary - 1);
        }
    }

    private static void assertNeedsRollover(int port) throws Exception {
        var records = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("unadmitted".getBytes(StandardCharsets.UTF_8)));
        var topics = new ProduceRequestData.TopicProduceDataCollection();
        topics.add(new ProduceRequestData.TopicProduceData().setName(OBJECT.topic()).setPartitionData(
                List.of(new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(records))));
        var produce = new ProduceRequest(
                new ProduceRequestData().setAcks((short) -1).setTimeoutMs(5_000).setTopicData(topics), (short) 9);
        try (var request = new NativeKafkaRequest(port, produce)) {
            var response = (ProduceResponse) request.receive(5_000);
            var result = response.data().responses().iterator().next().partitionResponses().get(0);
            assertEquals(Errors.KAFKA_STORAGE_ERROR, Errors.forCode(result.errorCode()));
            assertEquals(-1, result.baseOffset());
        }
    }
}
