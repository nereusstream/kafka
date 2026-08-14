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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.test.TestUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 2,
    serverProperties = {
        @ClusterConfigProperty(key = ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1")
    }
)
public class KafkaProducerGuardedIntegrationTest {
    private static final String TOPIC = "guarded-topic";

    @ClusterTest
    public void oldTopicIncarnationIsRejectedAfterDeleteAndRecreate(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            Uuid oldTopicId = createTopic(admin, TOPIC, (short) 2);
            ProducerResourceGuard oldGuard = guard(cluster, oldTopicId);

            try (KafkaProducer<byte[], byte[]> producer = producer(cluster)) {
                GuardedRecordMetadata first = producer.sendGuarded(record(0, "before-recreate"), oldGuard).get();
                assertEquals(oldTopicId, first.responseEvidence().expectedTopicId());
            }

            admin.deleteTopics(List.of(TOPIC)).all().get();
            TestUtils.waitForCondition(() -> {
                try {
                    return !admin.listTopics().names().get().contains(TOPIC);
                } catch (Exception failure) {
                    return false;
                }
            }, "topic metadata did not disappear after delete: " + TOPIC);
            Uuid newTopicId = createTopic(admin, TOPIC, (short) 2);
            assertNotEquals(oldTopicId, newTopicId);

            // A fresh producer proves the broker incarnation, independent of any prior metadata cache.
            try (KafkaProducer<byte[], byte[]> producer = producer(cluster)) {
                ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> producer.sendGuarded(record(0, "old-incarnation"), oldGuard).get());
                ResourceGuardException guardedFailure = assertInstanceOf(ResourceGuardException.class,
                    failure.getCause());
                assertEquals(ResourceGuardFailureReason.TOPIC_ID_MISMATCH, guardedFailure.reason());
                assertEquals(oldTopicId, guardedFailure.guard().expectedTopicId());

                GuardedRecordMetadata recreated = producer.sendGuarded(record(0, "after-recreate"),
                    guard(cluster, newTopicId)).get();
                assertEquals(newTopicId, recreated.responseEvidence().expectedTopicId());
            }
        }
    }

    @ClusterTest
    public void sameTopicIncarnationRemainsGuardedAcrossLeaderFailover(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin(); KafkaProducer<byte[], byte[]> producer = producer(cluster)) {
            Uuid topicId = createTopic(admin, TOPIC, (short) 2);
            TopicPartition topicPartition = new TopicPartition(TOPIC, 0);
            int oldLeader = leader(admin, topicPartition);
            ProducerResourceGuard guard = guard(cluster, topicId);

            GuardedRecordMetadata beforeFailover = producer.sendGuarded(record(0, "before-failover"), guard).get();
            assertEquals(oldLeader, beforeFailover.responseEvidence().brokerNodeId());

            cluster.brokers().get(oldLeader).shutdown();
            TestUtils.waitForCondition(() -> leader(admin, topicPartition) != oldLeader,
                "topic leader did not fail over from broker " + oldLeader);

            GuardedRecordMetadata afterFailover = producer.sendGuarded(record(0, "after-failover"), guard).get();
            assertNotEquals(oldLeader, afterFailover.responseEvidence().brokerNodeId());
            assertEquals(topicId, afterFailover.responseEvidence().expectedTopicId());
            assertEquals(topicId, afterFailover.resourceGuard().expectedTopicId());
        }
    }

    private static ProducerRecord<byte[], byte[]> record(final int partition, final String value) {
        return new ProducerRecord<>(TOPIC, partition, null, value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static KafkaProducer<byte[], byte[]> producer(final ClusterInstance cluster) {
        Map<String, Object> config = Map.of(
            BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
            ACKS_CONFIG, "all",
            ENABLE_IDEMPOTENCE_CONFIG, true,
            MAX_BLOCK_MS_CONFIG, 15_000,
            REQUEST_TIMEOUT_MS_CONFIG, 10_000,
            KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
            VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(cluster.setClientSaslConfig(cluster.setClientSslConfig(config)));
    }

    private static Uuid createTopic(final Admin admin, final String topic, final short replicationFactor) throws Exception {
        return admin.createTopics(List.of(new NewTopic(topic, 1, replicationFactor))).topicId(topic).get();
    }

    private static TopicDescription description(final Admin admin, final String topic) throws Exception {
        return admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
    }

    private static int leader(final Admin admin, final TopicPartition topicPartition) throws Exception {
        TopicDescription description = description(admin, topicPartition.topic());
        return description.partitions().stream()
            .filter(partition -> partition.partition() == topicPartition.partition())
            .map(TopicPartitionInfo::leader)
            .map(node -> node.id())
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("topic has no leader: " + topicPartition));
    }

    private static ProducerResourceGuard guard(final ClusterInstance cluster, final Uuid topicId) {
        return new ProducerResourceGuard(cluster.clusterId(), TOPIC, topicId, 0);
    }
}
