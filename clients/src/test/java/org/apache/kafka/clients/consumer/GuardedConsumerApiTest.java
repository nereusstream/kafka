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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GuardedConsumerApiTest {
    @Test
    void guardRejectsUnusableIdentityAndExposesTopicPartition() {
        Uuid topicId = new Uuid(1, 2);
        ConsumerResourceGuard guard = new ConsumerResourceGuard("cluster", "topic", topicId, 3);

        assertEquals("cluster", guard.authenticatedClusterId());
        assertEquals(topicId, guard.expectedTopicId());
        assertEquals(new org.apache.kafka.common.TopicPartition("topic", 3), guard.topicPartition());
        assertThrows(IllegalArgumentException.class,
                () -> new ConsumerResourceGuard("cluster", "topic", Uuid.ZERO_UUID, 3));
        assertThrows(IllegalArgumentException.class,
                () -> new ConsumerResourceGuard("cluster", "topic", topicId, -1));
    }

    @Test
    void fetchEvidenceDefensivelyCopiesDigestAndRetainsExactWireIdentity() {
        byte[] digest = bytes(7);
        GuardedFetchEvidence evidence = new GuardedFetchEvidence("cluster", "topic", new Uuid(1, 2), 3,
                (short) 13, 4, 5, 6, 7, 8, 9, 10, 11, digest);

        digest[0] = 99;
        assertArrayEquals(bytes(7), evidence.fetchResponseBodySha256());
        assertEquals(13, evidence.requestVersion());
        assertEquals(6, evidence.sessionId());
        assertEquals(evidence, new GuardedFetchEvidence("cluster", "topic", new Uuid(1, 2), 3,
                (short) 13, 4, 5, 6, 7, 8, 9, 10, 11, bytes(7)));
    }

    @Test
    void guardedRecordsCarryProofWithoutChangingConsumerRecordsAccessors() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 3, 8, "key", "value");
        ConsumerRecords<String, String> records = new ConsumerRecords<>(
                Map.of(new org.apache.kafka.common.TopicPartition("topic", 3), List.of(record)), Map.of());
        GuardedFetchEvidence evidence = new GuardedFetchEvidence("cluster", "topic", new Uuid(1, 2), 3,
                (short) 13, 4, 5, -1, 8, 8, 8, 10, 11, bytes(1));

        GuardedConsumerRecords<String, String> guarded = new GuardedConsumerRecords<>(records, evidence);

        assertEquals(List.of(record), guarded.records(new org.apache.kafka.common.TopicPartition("topic", 3)));
        assertEquals(evidence, guarded.fetchEvidence());
        assertThrows(IllegalArgumentException.class, () -> new GuardedConsumerRecords<>(records, null));
    }

    private static byte[] bytes(final int seed) {
        byte[] result = new byte[32];
        Arrays.fill(result, (byte) seed);
        return result;
    }
}
