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
package org.apache.kafka.metadata;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.metadata.nereus.KafkaTopicBindingTestFixtures;
import org.apache.kafka.metadata.nereus.NereusTopicProfileProjectionV1;
import org.apache.kafka.server.common.KRaftVersion;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NereusTopicProfileProjectionTest {
    private static final String TOPIC_NAME = "profile-projection";
    private static final Uuid TOPIC_ID = new Uuid(41L, 42L);

    @Test
    void testProjectionComesOnlyFromAggregate() {
        TopicImage topic = new TopicImage(
            TOPIC_NAME,
            TOPIC_ID,
            Map.of(),
            Optional.of(KafkaTopicBindingTestFixtures.aggregate(TOPIC_ID, TOPIC_NAME)));
        MetadataImage empty = MetadataImage.EMPTY;
        MetadataImage image = new MetadataImage(
            empty.provenance(),
            empty.features(),
            empty.cluster(),
            TopicsImage.EMPTY.including(topic),
            empty.configs(),
            empty.clientQuotas(),
            empty.producerIds(),
            empty.acls(),
            empty.scram(),
            empty.delegationTokens());
        KRaftMetadataCache cache = new KRaftMetadataCache(1, () -> KRaftVersion.KRAFT_VERSION_1);
        cache.setImage(image);

        NereusTopicProfileProjectionV1 projection = cache.nereusTopicProfile(TOPIC_NAME).orElseThrow();
        assertEquals("BOOKKEEPER_WAL_ONLY", projection.profileName());
        assertTrue(projection.explicitlyConfigured());
        assertTrue(cache.topicConfig(TOPIC_NAME).isEmpty());
        assertFalse(cache.nereusTopicProfile("missing").isPresent());
    }
}
