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
package org.apache.kafka.image;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicBindingAggregateRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.nereus.KafkaTopicBindingAggregateMapperV1;
import org.apache.kafka.metadata.nereus.KafkaTopicBindingAggregateV1;
import org.apache.kafka.metadata.nereus.KafkaTopicBindingTestFixtures;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.NereusStorageVersion;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaTopicBindingImageAuthorityTest {
    private static final Uuid TOPIC_ID = new Uuid(0x101L, 0x202L);
    private static final String TOPIC_NAME = "image-topic";

    @Test
    void testReplaySnapshotOrderAndRemoveCascade() {
        TopicsDelta delta = new TopicsDelta(TopicsImage.EMPTY);
        delta.replay(new TopicRecord().setName(TOPIC_NAME).setTopicId(TOPIC_ID));
        delta.replay(aggregateRecord(TOPIC_ID, TOPIC_NAME));
        delta.replay(partitionRecord(TOPIC_ID, 1));
        delta.replay(partitionRecord(TOPIC_ID, 0));
        TopicsImage image = delta.apply();

        assertTrue(image.getTopic(TOPIC_ID).nereusAggregate().isPresent());
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder(MetadataVersion.latestProduction()).build());
        List<ApiMessageAndVersion> records = writer.records();
        assertEquals(4, records.size());
        assertInstanceOf(TopicRecord.class, records.get(0).message());
        assertInstanceOf(TopicBindingAggregateRecord.class, records.get(1).message());
        assertEquals(0, assertInstanceOf(PartitionRecord.class, records.get(2).message()).partitionId());
        assertEquals(1, assertInstanceOf(PartitionRecord.class, records.get(3).message()).partitionId());

        TopicsDelta remove = new TopicsDelta(image);
        remove.replay(new RemoveTopicRecord().setTopicId(TOPIC_ID));
        assertEquals(null, remove.apply().getTopic(TOPIC_ID));
    }

    @Test
    void testRejectsUnknownDuplicateAndMismatchedAggregate() {
        TopicBindingAggregateRecord aggregate = aggregateRecord(TOPIC_ID, TOPIC_NAME);
        assertThrows(RuntimeException.class, () -> new TopicsDelta(TopicsImage.EMPTY).replay(aggregate));

        TopicsDelta duplicate = new TopicsDelta(TopicsImage.EMPTY);
        duplicate.replay(new TopicRecord().setName(TOPIC_NAME).setTopicId(TOPIC_ID));
        duplicate.replay(aggregate);
        assertThrows(RuntimeException.class, () -> duplicate.replay(aggregate.duplicate()));

        TopicsDelta mismatch = new TopicsDelta(TopicsImage.EMPTY);
        mismatch.replay(new TopicRecord().setName("other-topic").setTopicId(TOPIC_ID));
        assertThrows(IllegalArgumentException.class, () -> mismatch.replay(aggregate.duplicate()));
    }

    @Test
    void testPublicationBoundaryRequiresExactAggregateForFeature2() {
        MetadataDelta missing = new MetadataDelta(MetadataImage.EMPTY);
        missing.replay(feature2());
        missing.replay(new TopicRecord().setName(TOPIC_NAME).setTopicId(TOPIC_ID));
        assertThrows(IllegalStateException.class, () -> missing.apply(MetadataProvenance.EMPTY));

        MetadataDelta valid = new MetadataDelta(MetadataImage.EMPTY);
        valid.replay(feature2());
        valid.replay(new TopicRecord().setName(TOPIC_NAME).setTopicId(TOPIC_ID));
        valid.replay(aggregateRecord(TOPIC_ID, TOPIC_NAME));
        MetadataImage image = valid.apply(MetadataProvenance.EMPTY);
        assertTrue(image.topics().getTopic(TOPIC_ID).nereusAggregate().isPresent());
    }

    @Test
    void testFeatureActivationScansAllExistingTopics() {
        MetadataDelta stockTopic = new MetadataDelta(MetadataImage.EMPTY);
        stockTopic.replay(new TopicRecord().setName(TOPIC_NAME).setTopicId(TOPIC_ID));
        MetadataImage stockImage = stockTopic.apply(MetadataProvenance.EMPTY);

        MetadataDelta activation = new MetadataDelta(stockImage);
        activation.replay(feature2());
        assertThrows(IllegalStateException.class, () -> activation.apply(MetadataProvenance.EMPTY));
    }

    @Test
    void testFeatureDisabledPublicationRejectsAggregate() {
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        delta.replay(new TopicRecord().setName(TOPIC_NAME).setTopicId(TOPIC_ID));
        delta.replay(aggregateRecord(TOPIC_ID, TOPIC_NAME));
        assertThrows(IllegalStateException.class, () -> delta.apply(MetadataProvenance.EMPTY));
    }

    @Test
    void testOrdinaryPublicationValidatesOnlyTouchedTopics() {
        KafkaTopicBindingAggregateV1 validAggregate =
            KafkaTopicBindingTestFixtures.aggregate(TOPIC_ID, TOPIC_NAME);
        TopicImage invalidUntouched = new TopicImage("untouched", new Uuid(9L, 9L), Map.of());
        Map<Integer, org.apache.kafka.metadata.PartitionRegistration> partitions = new HashMap<>();
        TopicImage validTouched = new TopicImage(
            TOPIC_NAME, TOPIC_ID, partitions, java.util.Optional.of(validAggregate));
        TopicsImage topics = TopicsImage.EMPTY.including(invalidUntouched).including(validTouched);
        MetadataImage base = metadataImageWithFeature2(topics);

        MetadataDelta delta = new MetadataDelta(base);
        delta.replay(partitionRecord(TOPIC_ID, 0));
        assertTrue(delta.apply(MetadataProvenance.EMPTY).topics().getTopic(TOPIC_ID).partitions().containsKey(0));
    }

    private static MetadataImage metadataImageWithFeature2(TopicsImage topics) {
        return new MetadataImage(
            MetadataProvenance.EMPTY,
            new FeaturesImage(
                Map.of(NereusStorageVersion.FEATURE_NAME, NereusStorageVersion.NSV_2.featureLevel()),
                java.util.Optional.of(MetadataVersion.latestProduction())),
            ClusterImage.EMPTY,
            topics,
            ConfigurationsImage.EMPTY,
            ClientQuotasImage.EMPTY,
            ProducerIdsImage.EMPTY,
            AclsImage.EMPTY,
            ScramImage.EMPTY,
            DelegationTokenImage.EMPTY);
    }

    private static FeatureLevelRecord feature2() {
        return new FeatureLevelRecord()
            .setName(NereusStorageVersion.FEATURE_NAME)
            .setFeatureLevel(NereusStorageVersion.NSV_2.featureLevel());
    }

    private static TopicBindingAggregateRecord aggregateRecord(Uuid topicId, String topicName) {
        return KafkaTopicBindingAggregateMapperV1.toRecord(
            KafkaTopicBindingTestFixtures.aggregate(topicId, topicName));
    }

    private static PartitionRecord partitionRecord(Uuid topicId, int partitionId) {
        return new PartitionRecord()
            .setTopicId(topicId)
            .setPartitionId(partitionId)
            .setReplicas(List.of(0))
            .setIsr(List.of(0))
            .setLeader(0);
    }
}
