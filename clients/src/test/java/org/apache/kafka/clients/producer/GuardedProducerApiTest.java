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

import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GuardedProducerApiTest {
    @Test
    void guardIsImmutableAndRejectsZeroResourceIdentity() {
        Uuid topicId = new Uuid(1, 2);
        ProducerResourceGuard guard = new ProducerResourceGuard("cluster", "topic", topicId, 3);
        assertEquals("cluster", guard.authenticatedClusterId());
        assertEquals("topic", guard.canonicalTopic());
        assertEquals(topicId, guard.expectedTopicId());
        assertEquals(3, guard.partition());

        assertThrows(IllegalArgumentException.class,
                () -> new ProducerResourceGuard("cluster", "topic", Uuid.ZERO_UUID, 3));
        assertThrows(IllegalArgumentException.class,
                () -> new ProducerResourceGuard("cluster", "topic", topicId, -1));
    }

    @Test
    void evidenceDefensivelyCopiesDigestsAndKeepsOptionalLeaderEpoch() {
        byte[] requestDigest = bytes(1);
        byte[] responseDigest = bytes(2);
        byte[] batchDigest = bytes(3);
        byte[] valueDigest = bytes(4);
        GuardedResponseEvidence evidence = new GuardedResponseEvidence("cluster", "topic", new Uuid(1, 2), 3,
                (short) 13, 4, 5, (short) 0, 6, 7, OptionalInt.of(8), requestDigest, responseDigest,
                batchDigest, valueDigest, 0, 1);

        requestDigest[0] = 99;
        assertArrayEquals(bytes(1), evidence.produceRequestBodySha256());
        assertEquals(OptionalInt.of(8), evidence.responseLeaderEpoch());
        assertEquals(evidence, new GuardedResponseEvidence("cluster", "topic", new Uuid(1, 2), 3,
                (short) 13, 4, 5, (short) 0, 6, 7, OptionalInt.of(8), bytes(1), bytes(2), bytes(3),
                bytes(4), 0, 1));
    }

    @Test
    void exceptionExposesTypedReasonAndNeverAcceptsSuccessAsNonPersistence() {
        ProducerResourceGuard guard = new ProducerResourceGuard("cluster", "topic", new Uuid(1, 2), 0);
        ResourceGuardException exception = new ResourceGuardException("mismatch", null,
                ResourceGuardFailureReason.TOPIC_ID_MISMATCH, guard, Optional.empty(), true);
        assertEquals(ResourceGuardFailureReason.TOPIC_ID_MISMATCH, exception.reason());
        assertEquals(guard, exception.guard());
        assertEquals(true, exception.definitelyNotPersisted());
        assertThrows(IllegalArgumentException.class, () -> new ResourceGuardException("bad", null,
                ResourceGuardFailureReason.RESPONSE_EVIDENCE_INTEGRITY, guard,
                Optional.of(new GuardedResponseEvidence("cluster", "topic", new Uuid(1, 2), 0,
                        (short) 13, 1, 2, (short) 0, 3, 4, OptionalInt.empty(), bytes(1), bytes(2), bytes(3),
                        bytes(4), 0, 1)), true));
    }

    private static byte[] bytes(final int seed) {
        byte[] value = new byte[32];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
