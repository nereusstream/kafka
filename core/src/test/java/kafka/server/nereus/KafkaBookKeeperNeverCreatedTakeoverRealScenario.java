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
import kafka.server.storage.BrokerStorageRuntimeFactory;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.nereus.KafkaTopicBindingAggregateMapperV1;
import org.apache.kafka.server.common.MetadataVersion;

import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperRunBirthRecoveryV1;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Holds a real persisted OPEN response until another Broker fences its reserved, never-created birth. */
public final class KafkaBookKeeperNeverCreatedTakeoverRealScenario {
    private KafkaBookKeeperNeverCreatedTakeoverRealScenario() { }

    public static void run(Optional<Path> sourceSummary) throws Exception {
        String uri = System.getProperty("nereus.nsip.bkUri");
        String oxia = System.getProperty("nereus.nsip.oxia");
        if (uri == null || oxia == null) throw new IllegalStateException("real BK URI and Oxia address are required");
        Path inputs = Files.createTempDirectory("nereus-nsip-never-created-");
        var releaseOldOpen = new CompletableFuture<Void>();
        try {
            KafkaBookKeeperOwnerRelocationRealTest.writeCapability(inputs, sourceSummary);
            var bootstrap = BootstrapMetadata.fromVersion(MetadataVersion.latestTesting(), "nsip-never-created")
                    .copyWithFeatureRecord("nereus.storage.version", (short) 2);
            var nodes = new TestKitNodes.Builder(bootstrap).setNumBrokerNodes(3).setNumControllerNodes(1).build();
            var held = new AtomicBoolean();
            var persistedOpen = new CountDownLatch(1);
            var completedRecovery = new CountDownLatch[] {
                new CountDownLatch(1), new CountDownLatch(1), new CountDownLatch(1)
            };
            var recoveryFailures = new AtomicReferenceArray<Throwable>(3);
            BrokerStorageRuntimeFactory factory = context -> {
                var responses = new NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver() {
                    @Override
                    public CompletionStage<Void> afterOpen(CompletionStage<Void> response) {
                        if (context.config().brokerId() != 0 || !held.compareAndSet(false, true)) return response;
                        return response.thenCompose(ignored -> {
                            persistedOpen.countDown();
                            return releaseOldOpen;
                        });
                    }

                    @Override
                    public void afterRecovery(org.apache.kafka.common.TopicIdPartition identity, Throwable failure) {
                        if (identity.topicPartition().equals(new TopicPartition("native-cold", 0))) {
                            recoveryFailures.set(context.config().brokerId(), failure);
                            completedRecovery[context.config().brokerId()].countDown();
                        }
                    }
                };
                return new NereusBrokerStorageRuntime(
                        context, new NereusKafkaOwnedProviderRuntime(context, responses));
            };
            var builder = new KafkaClusterTestKit.Builder(nodes)
                    .setBrokerStorageRuntimeFactory(factory)
                    .setControllerStorageRuntimeFactory(new NereusControllerStorageRuntimeFactory());
            KafkaBookKeeperOwnerRelocationRealTest.nativeConfig(inputs, uri, oxia).forEach(builder::setConfigProp);
            try (KafkaClusterTestKit cluster = builder.build()) {
                cluster.format();
                cluster.startup();
                cluster.waitForReadyBrokers();
                var client = new Properties();
                client.put("bootstrap.servers", cluster.bootstrapServers());
                try (Admin admin = Admin.create(client)) {
                    var partition = new TopicPartition("native-cold", 0);
                    admin.createTopics(List.of(new NewTopic("native-cold", Map.of(0, List.of(0)))))
                            .all().get(30, TimeUnit.SECONDS);
                    assertTrue(persistedOpen.await(30, TimeUnit.SECONDS), "old Broker did not persist OPEN before create");
                    var topicId = admin.describeTopics(List.of("native-cold"))
                            .allTopicNames().get(30, TimeUnit.SECONDS).get("native-cold").topicId();
                    var brokerZero = cluster.brokers().get(0);
                    var aggregate = KafkaTopicBindingAggregateMapperV1.toRecord(brokerZero.metadataCache()
                            .getImage().topics().getTopic(topicId).nereusAggregate().orElseThrow());
                    try (var probe = new NereusClosedOwnerProbe(
                            brokerZero.config(), topicId, partition, aggregate, 1)) {
                        reassign(admin, partition, 1);
                        assertEquals(1, KafkaBookKeeperOwnerRelocationRealTest.awaitLeader(admin, 0));
                        assertTrue(completedRecovery[1].await(30, TimeUnit.SECONDS),
                                "successor Broker did not finish native recovery");
                        assertNull(recoveryFailures.get(1), "successor could not activate after never-created birth");
                        assertTrue(probe.archived(1), "old owner was not durably closed");
                        assertEquals(2, probe.owner().owner().ownerEpoch());
                        assertFalse(probe.owner().closed());
                        assertNeverCreated(probe);
                        var producerConfig = new Properties();
                        producerConfig.putAll(client);
                        producerConfig.put("key.serializer", StringSerializer.class.getName());
                        producerConfig.put("value.serializer", StringSerializer.class.getName());
                        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig)) {
                            assertEquals(0, producer.send(new ProducerRecord<>(
                                    "native-cold", 0, "key", "after-never-created"))
                                    .get(30, TimeUnit.SECONDS).offset());
                        }
                        assertEquals(List.of("after-never-created"),
                                KafkaBookKeeperOwnerRelocationRealTest.consume(client, 1));
                        releaseOldOpen.complete(null);
                        assertTrue(completedRecovery[0].await(30, TimeUnit.SECONDS),
                                "superseded Broker did not resolve its held native open");
                        assertNeverCreated(probe);
                    }
                }
            }
        } finally {
            releaseOldOpen.complete(null);
            try (var files = Files.walk(inputs)) {
                for (var file : files.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(file);
            }
        }
    }

    private static void reassign(Admin admin, TopicPartition partition, int broker) throws Exception {
        admin.alterPartitionReassignments(Map.of(
                partition, Optional.of(new NewPartitionReassignment(List.of(broker)))))
                .all().get(30, TimeUnit.SECONDS);
    }

    private static void assertNeverCreated(NereusClosedOwnerProbe probe) throws Exception {
        var births = probe.closedBirths(1);
        assertEquals(1, births.size());
        assertEquals(KafkaBookKeeperRunBirthRecoveryV1.Kind.NEVER_CREATED, births.get(0).kind());
    }
}
