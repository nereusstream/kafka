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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.test.TestUtils;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Caps;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperSequentialReadRequestV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadOutcomeV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Real Coordinator records, native compaction/GC, then cold ownership and protocol reads without Object. */
public final class KafkaBookKeeperInternalCompactionRealScenario {
    private static final String USER = "m5-internal-user";
    private static final String OFFSETS = "__consumer_offsets";
    private static final String TRANSACTIONS = "__transaction_state";
    private static final TopicPartition USER_PARTITION = new TopicPartition(USER, 0);
    private static final String GROUP = "m5-internal-offsets";
    private static final String TRANSACTIONAL_ID = "m5-internal-transactions";

    private KafkaBookKeeperInternalCompactionRealScenario() { }

    public static void run(Path receipt) throws Exception {
        var inputs = Files.createTempDirectory("nereus-m5-internal-compaction-");
        var state = new State();
        try {
            KafkaBookKeeperOwnerRelocationRealTest.writeCapability(inputs, Optional.of(receipt));
            try (var cluster = cluster(inputs, state)) {
                cluster.format();
                cluster.startup();
                cluster.waitForReadyBrokers();
                var client = new Properties();
                client.put("bootstrap.servers", cluster.bootstrapServers());
                try (var admin = Admin.create(client)) {
                    var topics = List.of(USER, OFFSETS, TRANSACTIONS);
                    admin.createTopics(topics.stream().map(name -> new NewTopic(name, Map.of(0, List.of(0)))
                            .configs(Map.of("cleanup.policy", "compact", "min.cleanable.dirty.ratio", "0.0"))).toList())
                            .all().get(30, TimeUnit.SECONDS);
                    produceUser(client, state.opened(USER, 0));
                    long committed = produceOffsets(client, state.opened(OFFSETS, 0));
                    produceTransactions(client, state.opened(TRANSACTIONS, 0));
                    for (var topic : topics) compactAndReclaim(state.opened(topic, 0));
                    // Actual protocol consumers independently verify the materialized internal state after GC/recovery.
                    move(admin, OFFSETS);
                    state.opened(OFFSETS, 1);
                    try (var consumer = new KafkaConsumer<String, String>(consumerConfig(client))) {
                        assertEquals(committed, consumer.committed(java.util.Set.of(USER_PARTITION), Duration.ofSeconds(30))
                                .get(USER_PARTITION).offset());
                        consumer.assign(List.of(USER_PARTITION));
                        consumer.commitSync(Map.of(USER_PARTITION, new OffsetAndMetadata(committed + 1)), Duration.ofSeconds(30));
                    }
                    move(admin, TRANSACTIONS);
                    state.opened(TRANSACTIONS, 1);
                    try (var producer = new KafkaProducer<String, String>(producerConfig(client, true))) {
                        producer.initTransactions();
                        transact(producer, "after-internal-cold");
                    }
                    var user = state.opened(USER, 0);
                    // EndTxn may acknowledge before its asynchronous marker reaches the data partition.
                    TestUtils.waitForCondition(() -> {
                        var frontiers = user.storage().capture().root().frontiers();
                        return frontiers.lastStableOffset() == frontiers.highWatermark();
                    }, 30_000, "transaction marker did not make the native user prefix stable");
                    var before = records(user);
                    move(admin, USER);
                    assertEquals(before, records(state.opened(USER, 1)), "cold native recovery changed selected user bytes after GC");
                }
            }
        } finally {
            try (var files = Files.walk(inputs)) {
                for (var file : files.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(file);
            }
        }
    }

    private static KafkaClusterTestKit cluster(Path inputs, State state) throws Exception {
        var bootstrap = BootstrapMetadata.fromVersion(MetadataVersion.latestTesting(), "m5-native-internal")
                .copyWithFeatureRecord("nereus.storage.version", (short) 2);
        var nodes = new TestKitNodes.Builder(bootstrap).setNumBrokerNodes(3).setNumControllerNodes(1).build();
        var builder = new KafkaClusterTestKit.Builder(nodes)
                .setControllerStorageRuntimeFactory(new NereusControllerStorageRuntimeFactory())
                .setBrokerStorageRuntimeFactory(context -> new NereusBrokerStorageRuntime(context,
                        new NereusKafkaOwnedProviderRuntime(context, state.observer(context.config().brokerId()))));
        var nativeConfig = KafkaBookKeeperOwnerRelocationRealTest.nativeConfig(inputs, System.getProperty("nereus.nsip.bkUri"),
                System.getProperty("nereus.nsip.oxia"));
        KafkaBookKeeperM5PolicyRealScenario.configure(builder, inputs, nativeConfig, true, "BK_ONLY_INTERNAL");
        builder.setConfigProp("nereus.kafka.storage.bookkeeper.max.bytes.per.ledger", "1048576");
        builder.setConfigProp("message.max.bytes", "4096");
        // Three independently selected native caches coexist in this actual Cell.
        builder.setConfigProp("nereus.kafka.storage.compaction.max.concurrent.tasks", "3");
        builder.setConfigProp("offsets.topic.num.partitions", "1");
        builder.setConfigProp("offsets.topic.replication.factor", "1");
        builder.setConfigProp("transaction.state.log.num.partitions", "1");
        builder.setConfigProp("transaction.state.log.replication.factor", "1");
        builder.setConfigProp("transaction.state.log.min.isr", "1");
        return builder.build();
    }

    private static Properties producerConfig(Properties client, boolean transactional) {
        var config = new Properties();
        config.putAll(client);
        config.put("key.serializer", StringSerializer.class.getName());
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("acks", "all");
        if (transactional) config.put("transactional.id", TRANSACTIONAL_ID);
        return config;
    }

    private static Properties consumerConfig(Properties client) {
        var config = new Properties();
        config.putAll(client);
        config.put("key.deserializer", StringDeserializer.class.getName());
        config.put("value.deserializer", StringDeserializer.class.getName());
        config.put("group.id", GROUP);
        config.put("enable.auto.commit", "false");
        return config;
    }

    private static boolean hasCut(NereusKafkaOwnedProviderRuntime.Opened opened) {
        return opened.storage().nativeReadSources().sealedRoots().stream()
                .filter(root -> root.kafkaEndOffsetExclusive().orElseThrow() > root.kafkaStartOffset()).count() >= 2;
    }

    private static void produceUser(Properties client, NereusKafkaOwnedProviderRuntime.Opened opened) throws Exception {
        try (var producer = new KafkaProducer<String, String>(producerConfig(client, false))) {
            for (int i = 0; i < 48; i++) producer.send(new ProducerRecord<>(USER, 0, "user-key", "user-" + i)).get(30, TimeUnit.SECONDS);
        }
        assertTrue(hasCut(opened), "real user Produce did not seal two native runs");
    }

    private static long produceOffsets(Properties client, NereusKafkaOwnedProviderRuntime.Opened opened) {
        long committed = 0;
        try (var consumer = new KafkaConsumer<String, String>(consumerConfig(client))) {
            consumer.assign(List.of(USER_PARTITION));
            for (int i = 1; i <= 48 && !hasCut(opened); i++) {
                consumer.commitSync(Map.of(USER_PARTITION, new OffsetAndMetadata(i)), Duration.ofSeconds(30));
                committed = i;
            }
        }
        assertTrue(hasCut(opened), "actual OffsetCommit did not seal two native internal runs");
        return committed;
    }

    private static void produceTransactions(Properties client, NereusKafkaOwnedProviderRuntime.Opened opened) throws Exception {
        try (var producer = new KafkaProducer<String, String>(producerConfig(client, true))) {
            producer.initTransactions();
            for (int i = 0; i < 24 && !hasCut(opened); i++) transact(producer, "before-internal-cold-" + i);
        }
        assertTrue(hasCut(opened), "actual transaction state transitions did not seal two native internal runs");
    }

    private static void transact(KafkaProducer<String, String> producer, String value) throws Exception {
        producer.beginTransaction();
        producer.send(new ProducerRecord<>(USER, 0, "transaction-key", value)).get(30, TimeUnit.SECONDS);
        producer.commitTransaction();
    }

    private static void compactAndReclaim(NereusKafkaOwnedProviderRuntime.Opened opened) throws Exception {
        var bounds = KafkaBookKeeperNativeInputRealScenario.BOUNDS;
        var limits = new Caps(bounds.payloadBytes(), bounds.batches(), bounds.records(), bounds.records(),
                bounds.decodedBytes(), bounds.decodedBytes(), bounds.decodedBytes(), bounds.records(), bounds.records());
        var caps = KafkaBookKeeperSelectedInputRealScenario.caps(limits, 3);
        var allowance = new NereusKafkaNativeInputV2.CaptureAllowance(limits, 0, 0, 4096, 0, 4096, 64);
        var captured = KafkaBookKeeperNativeInputRealScenario.withCurrentMetadataImage(() -> opened.captureM5Inputs(
                KafkaBookKeeperNativeInputRealScenario.budget(opened), bounds, caps, allowance), true);
        var task = captured.prepareTask(4096, 65536).toCompletableFuture().get(30, TimeUnit.SECONDS);
        assertFalse(task.materialize().toCompletableFuture().get(30, TimeUnit.SECONDS).isEmpty());
        var outcome = KafkaBookKeeperNativeInputRealScenario.withCurrentMetadataImage(() ->
                KafkaBookKeeperRootVerifierRealScenario.publicationUntilKnown(task));
        assertTrue(outcome == PublicationOutcome.APPLIED_EXACT || outcome == PublicationOutcome.EXISTING_EXACT);
        opened.retireNativeM5Fallback().toCompletableFuture().get(30, TimeUnit.SECONDS);
        KafkaBookKeeperM5ReplacementGcRealScenario.reclaimIfAdmitted(opened, true);
        assertFalse(captured.discard().toCompletableFuture().isDone(), "selected native cache was released before Owner drain");
    }

    private static List<CanonicalBytes> records(NereusKafkaOwnedProviderRuntime.Opened opened) throws Exception {
        var result = opened.storage().read(new KafkaBookKeeperSequentialReadRequestV1(0, KafkaReadIsolationV1.REPLICA,
                Long.MAX_VALUE, Optional.empty())).toCompletableFuture().get(30, TimeUnit.SECONDS);
        assertEquals(KafkaBookKeeperReadOutcomeV1.FOUND, result.outcome(), result.detail());
        return result.batches().stream().map(batch -> batch.rawAssignedRecordBatch()).toList();
    }

    private static void move(Admin admin, String topic) throws Exception {
        var partition = new TopicPartition(topic, 0);
        admin.alterPartitionReassignments(Map.of(partition, Optional.of(new NewPartitionReassignment(List.of(1)))))
                .all().get(30, TimeUnit.SECONDS);
        TestUtils.waitForCondition(() -> admin.describeTopics(List.of(topic)).allTopicNames().get(5, TimeUnit.SECONDS)
                .get(topic).partitions().get(0).leader().id() == 1, 30_000, "native internal topic did not move to Broker1");
    }

    private static final class State {
        private final Map<String, CompletableFuture<NereusKafkaOwnedProviderRuntime.Opened>> ready = new ConcurrentHashMap<>();

        private CompletableFuture<NereusKafkaOwnedProviderRuntime.Opened> future(String topic, int broker) {
            return ready.computeIfAbsent(topic + ":" + broker, ignored -> new CompletableFuture<>());
        }

        private NereusKafkaOwnedProviderRuntime.Opened opened(String topic, int broker) throws Exception {
            return future(topic, broker).get(30, TimeUnit.SECONDS);
        }

        private NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver observer(int broker) {
            return new NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver() {
                @Override
                public void afterReady(org.apache.kafka.common.TopicIdPartition partition, NereusKafkaOwnedProviderRuntime.Opened opened) {
                    future(partition.topic(), broker).complete(opened);
                }
                @Override
                public void afterRecovery(org.apache.kafka.common.TopicIdPartition partition, Throwable failure) {
                    if (failure != null) future(partition.topic(), broker).completeExceptionally(failure);
                }
            };
        }
    }
}
