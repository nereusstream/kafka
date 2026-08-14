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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaProducerGuardedPreflightTest {
    @Test
    void missingPartitionFailsBeforeMetadataOrAccumulatorOwnership() throws Exception {
        try (KafkaProducer<byte[], byte[]> producer = producer(Map.of())) {
            ProducerResourceGuard guard = guard(0);
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> producer.sendGuarded(new ProducerRecord<>("topic", (Integer) null, null, new byte[]{1}),
                            guard).get());
            assertEquals(ResourceGuardFailureReason.INVALID_GUARD,
                    ((ResourceGuardException) failure.getCause()).reason());
        }
    }

    @Test
    void topicAndPartitionMismatchNeverFallsBackToOrdinarySend() throws Exception {
        try (KafkaProducer<byte[], byte[]> producer = producer(Map.of())) {
            ProducerResourceGuard guard = guard(2);
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> producer.sendGuarded(new ProducerRecord<>("replacement", 1, null, new byte[]{1}),
                            guard).get());
            assertEquals(ResourceGuardFailureReason.INVALID_GUARD,
                    ((ResourceGuardException) failure.getCause()).reason());
        }
    }

    @Test
    void unsupportedConfigurationCallsGuardedCallbackWithTypedFailure() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.ACKS_CONFIG, "0");
        AtomicReference<GuardedRecordMetadata> metadata = new AtomicReference<>();
        AtomicReference<Exception> callbackFailure = new AtomicReference<>();
        try (KafkaProducer<byte[], byte[]> producer = producer(configs)) {
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> producer.sendGuarded(new ProducerRecord<>("topic", 0, null, new byte[]{1}), guard(0),
                            (result, exception) -> {
                                metadata.set(result);
                                callbackFailure.set(exception);
                            }).get());
            assertNull(metadata.get());
            assertEquals(ResourceGuardFailureReason.UNSUPPORTED_CONFIGURATION,
                    ((ResourceGuardException) failure.getCause()).reason());
            assertEquals(failure.getCause(), callbackFailure.get());
        }
    }

    @Test
    void guardedTransactionalProduceRequiresActiveTransactionV2() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "guarded-transaction");
        try (KafkaProducer<byte[], byte[]> producer = producer(configs)) {
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> producer.sendGuardedInTransaction(
                            new ProducerRecord<>("topic", 0, null, new byte[]{1}), guard(0)).get());
            ResourceGuardException cause = (ResourceGuardException) failure.getCause();
            assertEquals(ResourceGuardFailureReason.UNSUPPORTED_CONFIGURATION, cause.reason());
            assertEquals(true, cause.definitelyNotPersisted());
        }
    }

    @Test
    @org.junit.jupiter.api.Timeout(5)
    void guardedSuccessReturnsBrokerBoundEvidence() throws Exception {
        MockTime time = new MockTime();
        ProducerMetadata metadata = new ProducerMetadata(0, 0, Long.MAX_VALUE, 60_000,
                new LogContext(), new ClusterResourceListeners(), time);
        Uuid topicId = new Uuid(7, 8);
        MockClient client = new MockClient(time, metadata);
        client.setNodeApiVersions(NodeApiVersions.create(ApiKeys.PRODUCE.id,
                ApiKeys.PRODUCE.oldestVersion(), ApiKeys.PRODUCE.latestVersion()));
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1,
                Collections.singletonMap("topic", 1), Collections.singletonMap("topic", topicId)));

        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1");
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, "guarded-success-test");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1_000);
        configs.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(new ProducerConfig(configs),
                new ByteArraySerializer(), new ByteArraySerializer(), metadata, client, null,
                new ApiVersions(), time)) {
            Cluster cluster = metadata.fetch();
            ProducerResourceGuard guard = new ProducerResourceGuard(cluster.clusterResource().clusterId(),
                    "topic", topicId, 0);
            ProduceResponseData.TopicProduceResponse topicResponse = new ProduceResponseData.TopicProduceResponse()
                    .setName("").setTopicId(topicId);
            topicResponse.partitionResponses().add(new ProduceResponseData.PartitionProduceResponse()
                    .setIndex(0).setErrorCode((short) 0).setBaseOffset(11).setLogAppendTimeMs(99));
            AtomicReference<ProduceRequest> requestReference = new AtomicReference<>();
            client.prepareResponse(body -> {
                requestReference.set((ProduceRequest) body);
                return true;
            }, new ProduceResponse(new ProduceResponseData().setResponses(
                    new ProduceResponseData.TopicProduceResponseCollection(Collections.singletonList(topicResponse)))));

            Future<GuardedRecordMetadata> future = producer.sendGuarded(
                    new ProducerRecord<>("topic", 0, null, new byte[]{1, 2, 3}), guard);
            GuardedRecordMetadata result = future.get();
            assertNotNull(result);
            assertEquals(13, requestReference.get().version());
            assertEquals(11, result.recordMetadata().offset());
            assertEquals(topicId, result.responseEvidence().expectedTopicId());
            assertEquals(99, result.responseEvidence().logAppendTimeMs());
            assertEquals(13, result.responseEvidence().requestVersion());
        }
    }

    private static KafkaProducer<byte[], byte[]> producer(final Map<String, Object> overrides) {
        Map<String, Object> configs = new HashMap<>(overrides);
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1");
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, "guarded-preflight-test");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 100);
        return new KafkaProducer<>(configs, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private static ProducerResourceGuard guard(final int partition) {
        return new ProducerResourceGuard("cluster", "topic", new Uuid(1, 2), partition);
    }
}
