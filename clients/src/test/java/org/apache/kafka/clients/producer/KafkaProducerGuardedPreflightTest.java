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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
