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

import kafka.log.nereus.NereusUnifiedLog;
import kafka.server.nsip.KafkaBookKeeperOwnerRelocationRealTest;
import kafka.server.nsip.NativeKafkaRequest;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.config.NereusKafkaConfigs;
import org.apache.kafka.test.TestUtils;

import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2DataV1;
import com.nereusstream.storage.api.bookkeeper.AppendQuorumProofV1;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.oxia.client.api.AsyncOxiaClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The native Produce response stays pending after real BK persistence until shared publication. */
public class KafkaBookKeeperAckRealTest {
    private static final String TOPIC = "native-cold";

    @Test
    @Timeout(180)
    public void bothAckModesWaitForSharedCommitAndAckZeroHasNoResponse(@TempDir Path inputs) throws Exception {
        var observer = new HoldDurability();
        try (var cluster = nativeCluster(inputs, observer, 1)) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            var client = new Properties();
            client.setProperty("bootstrap.servers", cluster.bootstrapServers());
            try (var admin = Admin.create(client)) {
                admin.createTopics(List.of(new NewTopic(TOPIC, Map.of(0, List.of(0)))))
                        .all().get(30, TimeUnit.SECONDS);
            }
            var storage = observer.storage.get(30, TimeUnit.SECONDS);
            assertAckAfterPublication(client, observer, storage, "1", 0);
            assertAckAfterPublication(client, observer, storage, "all", 1);
            int controlCalls = observer.controlCalls.get();
            try (var request = new NativeKafkaRequest(cluster, 0, produce((short) 0, "acks-0"))) {
                TestUtils.waitForCondition(() -> storage.capture().root().frontiers().highWatermark() == 3,
                        "acks=0 Produce did not commit");
                assertThrows(SocketTimeoutException.class, () -> request.receive(250),
                        "acks=0 Produce sent a response");
            }
            assertEquals(controlCalls, observer.controlCalls.get(), "acks=0 Produce used control metadata");
            assertEquals(List.of("acks-1", "acks-all", "acks-0"),
                    KafkaBookKeeperOwnerRelocationRealTest.consume(client, 3));
            cluster.fatalFaultHandler().maybeRethrowFirstException();
            cluster.nonFatalFaultHandler().maybeRethrowFirstException();
        }
    }

    @Test
    @Timeout(180)
    public void delayedNativeFetchWakesOnSharedCommitAndTransactionMarker(@TempDir Path inputs) throws Exception {
        var observer = new HoldDurability();
        try (var cluster = nativeCluster(inputs, observer, 1, 1)) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            var client = new Properties();
            client.setProperty("bootstrap.servers", cluster.bootstrapServers());
            var partition = new TopicPartition(TOPIC, 0);
            Uuid topicId;
            try (var admin = Admin.create(client)) {
                topicId = admin.createTopics(List.of(new NewTopic(TOPIC, Map.of(0, List.of(0)))))
                        .topicId(TOPIC).get(30, TimeUnit.SECONDS);
            }
            var storage = observer.storage.get(30, TimeUnit.SECONDS);
            assertAckAfterPublication(client, observer, storage, "all", 0);
            var cut = new Cut(1, new CompletableFuture<>(), new CompletableFuture<>());
            observer.armed.set(cut);
            try (var append = new NativeKafkaRequest(cluster, 0, produce((short) -1, "publish-wake"))) {
                assertEquals(2, cut.durable().get(20, TimeUnit.SECONDS).acknowledgedBookies());
                assertEquals(1, storage.capture().root().frontiers().highWatermark());
                int controlCalls = observer.controlCalls.get();
                try (var waiting = new NativeKafkaRequest(cluster, 0,
                        delayedFetch(partition, topicId, 1, IsolationLevel.READ_UNCOMMITTED, 15_000))) {
                    assertThrows(SocketTimeoutException.class, () -> waiting.receive(250),
                            "Fetch exposed a quorum write before shared publication");
                    assertEquals(controlCalls, observer.controlCalls.get(), "Pending Fetch polled Oxia");
                    cut.release().complete(null);
                    var response = (ProduceResponse) append.receive(10_000);
                    var appended = response.data().responses().iterator().next().partitionResponses().get(0);
                    assertEquals(Errors.NONE, Errors.forCode(appended.errorCode()));
                    assertEquals(1, appended.baseOffset());
                    var observed = ((FetchResponse) waiting.receive(5_000))
                            .responseData(Map.of(topicId, TOPIC), (short) 16).get(partition);
                    assertEquals(2, observed.highWatermark());
                    assertFetchedValues(observed, List.of("publish-wake"));
                    assertEquals(controlCalls, observer.controlCalls.get(), "Shared commit or Fetch performed control I/O");
                }
            } finally {
                observer.armed.set(null);
                cut.release().complete(null);
            }

            var transactional = new Properties();
            transactional.putAll(client);
            transactional.setProperty("key.serializer", StringSerializer.class.getName());
            transactional.setProperty("value.serializer", StringSerializer.class.getName());
            transactional.setProperty("transactional.id", "m6-delayed-fetch");
            try (var producer = new KafkaProducer<String, String>(transactional)) {
                producer.initTransactions();
                producer.beginTransaction();
                assertEquals(2, producer.send(new ProducerRecord<>(TOPIC, 0, "key", "marker-wake"))
                        .get(30, TimeUnit.SECONDS).offset());
                try (var uncommitted = new NativeKafkaRequest(cluster, 0,
                            delayedFetch(partition, topicId, 2, IsolationLevel.READ_UNCOMMITTED, 0));
                        var committed = new NativeKafkaRequest(cluster, 0,
                            delayedFetch(partition, topicId, 2, IsolationLevel.READ_COMMITTED, 15_000))) {
                    var visible = ((FetchResponse) uncommitted.receive(5_000))
                            .responseData(Map.of(topicId, TOPIC), (short) 16).get(partition);
                    assertEquals(3, visible.highWatermark());
                    assertFetchedValues(visible, List.of("marker-wake"));
                    assertThrows(SocketTimeoutException.class, () -> committed.receive(250),
                            "Read-committed Fetch returned while the original transaction was open");
                    assertEquals(3, producer.send(new ProducerRecord<>(TOPIC, 0, "key", "only-hw"))
                            .get(30, TimeUnit.SECONDS).offset());
                    assertThrows(SocketTimeoutException.class, () -> committed.receive(250),
                            "Read-committed Fetch returned when only HW advanced");
                    producer.commitTransaction();
                    var stable = ((FetchResponse) committed.receive(5_000))
                            .responseData(Map.of(topicId, TOPIC), (short) 16).get(partition);
                    assertEquals(5, stable.highWatermark());
                    assertEquals(5, stable.lastStableOffset());
                    assertFetchedValues(stable, List.of("marker-wake", "only-hw"));
                }
            }
            cluster.fatalFaultHandler().maybeRethrowFirstException();
            cluster.nonFatalFaultHandler().maybeRethrowFirstException();
        }
    }

    static FetchRequest delayedFetch(TopicPartition partition, Uuid topicId, long offset,
            IsolationLevel isolation, int maxWait) {
        short version = 16;
        return FetchRequest.Builder.forConsumer(version, maxWait, 1,
                        Map.of(partition, new FetchRequest.PartitionData(topicId, offset,
                                FetchRequest.INVALID_LOG_START_OFFSET, 1024 * 1024, Optional.empty())))
                .isolationLevel(isolation).build(version);
    }

    static void assertFetchedValues(FetchResponseData.PartitionData observed,
            List<String> expected) {
        assertEquals(Errors.NONE, Errors.forCode(observed.errorCode()));
        var actual = new ArrayList<String>();
        for (var batch : FetchResponse.recordsOrFail(observed).batches()) {
            batch.ensureValid();
            if (!batch.isControlBatch()) {
                batch.forEach(record -> actual.add(StandardCharsets.UTF_8.decode(record.value()).toString()));
            }
        }
        assertEquals(expected, actual);
    }

    @Test
    @Timeout(180)
    public void staleQuorumCallbackCannotPublishAfterNativeOwnerChange(@TempDir Path inputs) throws Exception {
        var observer = new HoldDurability();
        try (var cluster = nativeCluster(inputs, observer, 2)) {
            verifyStaleQuorumCallback(cluster, observer, new NewTopic(TOPIC, Map.of(0, List.of(0))), false);
        }
    }

    static void verifyAsyncStaleQuorumCallback(Path inputs, Map<String, Object> settings) throws Exception {
        var observer = new HoldDurability();
        var bootstrap = BootstrapMetadata.fromVersion(MetadataVersion.latestTesting(), "nsip-async-ack")
                .copyWithFeatureRecord("nereus.storage.version", (short) 2);
        var nodes = new TestKitNodes.Builder(bootstrap).setNumBrokerNodes(2).setNumControllerNodes(1).build();
        var builder = new KafkaClusterTestKit.Builder(nodes)
                .setBrokerStorageRuntimeFactory(context -> context.config().brokerId() == 0
                        ? new NereusBrokerStorageRuntime(context, new NereusKafkaOwnedProviderRuntime(context, observer))
                        : NereusBrokerStorageRuntimeFactory.production().create(context))
                .setControllerStorageRuntimeFactory(new NereusControllerStorageRuntimeFactory());
        KafkaBookKeeperM5PolicyRealScenario.configure(builder, inputs, settings, true, "OBJECT_BYTES_DISABLED");
        var topic = new NewTopic(TOPIC, Map.of(0, List.of(0)))
                .configs(Map.of("nereus.storage.profile", "BOOKKEEPER_WAL_ASYNC_OBJECT"));
        try (var cluster = builder.build()) {
            verifyStaleQuorumCallback(cluster, observer, topic, true);
        }
    }

    private static void verifyStaleQuorumCallback(KafkaClusterTestKit cluster, HoldDurability observer,
            NewTopic topic, boolean bothAckModes) throws Exception {
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
        var client = new Properties();
        client.setProperty("bootstrap.servers", cluster.bootstrapServers());
        try (var admin = Admin.create(client)) {
            var topicId = admin.createTopics(List.of(topic))
                    .topicId(TOPIC).get(30, TimeUnit.SECONDS);
            var partition = new TopicPartition(TOPIC, 0);
            var old = observer.storage.get(30, TimeUnit.SECONDS);
            // The provider callback precedes Controller RECOVERED and M5 writer enrollment.
            assertEquals(0L, admin.listOffsets(Map.of(partition, OffsetSpec.latest()))
                    .partitionResult(partition).get(30, TimeUnit.SECONDS).offset());
            assertAckAfterPublication(client, observer, old, "all", 0);
            int acknowledged = 1;
            if (bothAckModes) {
                assertAckAfterPublication(client, observer, old, "1", acknowledged);
                acknowledged++;
            }
            var cut = new Cut(acknowledged, new CompletableFuture<>(), new CompletableFuture<>());
            observer.armed.set(cut);
            try (var wire = new NativeKafkaRequest(cluster, 0, produce((short) -1, "stale-owner"))) {
                assertEquals(2, cut.durable().get(20, TimeUnit.SECONDS).acknowledgedBookies());
                assertEquals(acknowledged, old.capture().root().frontiers().highWatermark());
                admin.alterPartitionReassignments(Map.of(partition,
                                Optional.of(new NewPartitionReassignment(List.of(1)))))
                        .all().get(30, TimeUnit.SECONDS);
                TestUtils.waitForCondition(() -> {
                    var image = cluster.brokers().get(0).metadataCache().getImage().topics().getTopic(topicId);
                    return image != null && image.partitions().get(0).leader == 1;
                }, "Old Broker did not observe a newer native Owner");
                cut.release().complete(null);
                var response = (ProduceResponse) wire.receive(30_000);
                var result = response.data().responses().iterator().next().partitionResponses().get(0);
                var error = Errors.forCode(result.errorCode());
                assertTrue(error == Errors.NOT_LEADER_OR_FOLLOWER || error == Errors.KAFKA_STORAGE_ERROR,
                        "Stale Owner replied with " + error);
                var after = old.capture().root().frontiers();
                assertEquals(acknowledged, after.highWatermark());
                assertEquals(acknowledged, after.readableEndOffset());
                assertEquals(1, KafkaBookKeeperOwnerRelocationRealTest.awaitLeader(admin, 0));
            } finally {
                observer.armed.set(null);
                cut.release().complete(null);
            }
        }
        cluster.fatalFaultHandler().maybeRethrowFirstException();
        cluster.nonFatalFaultHandler().maybeRethrowFirstException();
    }

    @Test
    @Timeout(180)
    public void unknownBookKeeperResponseFencesOldPublicationAndColdOwnerRecoversExactBatch(@TempDir Path inputs)
            throws Exception {
        var observer = new HoldDurability();
        try (var cluster = nativeCluster(inputs, observer, 2)) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            var client = new Properties();
            client.setProperty("bootstrap.servers", cluster.bootstrapServers());
            try (var admin = Admin.create(client)) {
                var topicId = admin.createTopics(List.of(new NewTopic(TOPIC, Map.of(0, List.of(0)))))
                        .topicId(TOPIC).get(30, TimeUnit.SECONDS);
                var old = observer.storage.get(30, TimeUnit.SECONDS);
                assertAckAfterPublication(client, observer, old, "all", 0);
                var cut = new Cut(1, new CompletableFuture<>(), new CompletableFuture<>(), true);
                observer.armed.set(cut);
                try (var wire = new NativeKafkaRequest(cluster, 0, produce((short) -1, "unknown-response"))) {
                    assertEquals(2, cut.durable().get(20, TimeUnit.SECONDS).acknowledgedBookies());
                    assertEquals(1, old.capture().root().frontiers().highWatermark());
                    cut.release().complete(null);
                    assertNativeStorageRejection((ProduceResponse) wire.receive(30_000));
                } finally {
                    observer.armed.set(null);
                    cut.release().complete(null);
                }
                try (var wire = new NativeKafkaRequest(cluster, 0, produce((short) -1, "forbidden-successor"))) {
                    assertNativeStorageRejection((ProduceResponse) wire.receive(30_000));
                }
                var after = old.capture().root().frontiers();
                assertEquals(1, after.highWatermark());
                assertEquals(1, after.readableEndOffset());
                var partition = new TopicPartition(TOPIC, 0);
                short fetchVersion = 16;
                var fetch = FetchRequest.Builder.forConsumer(fetchVersion, 0, 0,
                                Map.of(partition, new FetchRequest.PartitionData(topicId, 0,
                                        FetchRequest.INVALID_LOG_START_OFFSET, 1 << 20, Optional.empty())))
                        .build(fetchVersion);
                try (var wire = new NativeKafkaRequest(cluster, 0, fetch)) {
                    var response = (FetchResponse) wire.receive(30_000);
                    var result = response.responseData(Map.of(topicId, TOPIC), fetchVersion).get(partition);
                    assertEquals(Errors.KAFKA_STORAGE_ERROR.code(), result.errorCode());
                    assertEquals(0, FetchResponse.recordsSize(result));
                }
                admin.alterPartitionReassignments(Map.of(partition,
                                Optional.of(new NewPartitionReassignment(List.of(1)))))
                        .all().get(30, TimeUnit.SECONDS);
                assertEquals(1, KafkaBookKeeperOwnerRelocationRealTest.awaitLeader(admin, 0));
                assertEquals(List.of("acks-all", "unknown-response"),
                        KafkaBookKeeperOwnerRelocationRealTest.consume(client, 2));
                assertWireAppend(cluster, 1, compressed("next-0", "next-1"), 2);
                assertEquals(List.of("acks-all", "unknown-response", "next-0", "next-1"),
                        KafkaBookKeeperOwnerRelocationRealTest.consume(client, 4));
            }
            cluster.fatalFaultHandler().maybeRethrowFirstException();
            cluster.nonFatalFaultHandler().maybeRethrowFirstException();
        }
    }

    private static void assertNativeStorageRejection(ProduceResponse response) {
        var partition = response.data().responses().iterator().next().partitionResponses().get(0);
        var error = Errors.forCode(partition.errorCode());
        assertTrue(error == Errors.KAFKA_STORAGE_ERROR || error == Errors.NOT_LEADER_OR_FOLLOWER,
                "Produce after an unresolved append returned " + error);
    }

    @Test
    @Timeout(180)
    public void nativeFetchPreservesCompleteCompressedBatchesAcrossViewChanges(@TempDir Path inputs) throws Exception {
        try (var cluster = nativeCluster(inputs, new HoldDurability(), 1)) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            var client = new Properties();
            client.setProperty("bootstrap.servers", cluster.bootstrapServers());
            Uuid topicId;
            try (var admin = Admin.create(client)) {
                topicId = admin.createTopics(List.of(new NewTopic(TOPIC, Map.of(0, List.of(0)))))
                        .topicId(TOPIC).get(30, TimeUnit.SECONDS);
            }
            TestUtils.waitForCondition(() -> {
                var local = cluster.brokers().get(0).replicaManager().onlinePartition(new TopicPartition(TOPIC, 0));
                if (local.isEmpty() || local.get().log().isEmpty()) return false;
                return ((NereusUnifiedLog) local.get().log().get()).nereusWritable(local.get().getLeaderEpoch());
            }, "Native Owner did not activate");
            assertWireAppend(cluster, 0, compressed("first-0", "first-1"), 0);
            assertWireAppend(cluster, 0, compressed("second-0", "second-1"), 2);
            assertWireAppend(cluster, 0, compressed("third-0", "third-1"), 4);

            int port = cluster.brokers().get(0).socketServer().boundPort(cluster.nodes().brokerListenerName());
            // An interior offset and a one-byte budget still return the entire first batch.
            assertWireBatch(port, topicId, 1, 0, 6, List.of("first-0", "first-1"));
            assertWireBatch(port, topicId, 2, 2, 6, List.of("second-0", "second-1"));
            // Publishing a new root invalidates the position obtained for offset 4.
            assertWireAppend(cluster, 0, compressed("fourth-0", "fourth-1"), 6);
            assertWireBatch(port, topicId, 4, 4, 8, List.of("third-0", "third-1"));
            assertWireBatch(port, topicId, 6, 6, 8, List.of("fourth-0", "fourth-1"));
            cluster.fatalFaultHandler().maybeRethrowFirstException();
            cluster.nonFatalFaultHandler().maybeRethrowFirstException();
        }
    }

    static MemoryRecords compressed(String first, String second) {
        return MemoryRecords.withRecords(Compression.gzip().build(),
                new SimpleRecord(first.getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord(second.getBytes(StandardCharsets.UTF_8)));
    }

    private static void assertWireAppend(KafkaClusterTestKit cluster, int broker, MemoryRecords records, long expectedOffset)
            throws Exception {
        try (var wire = new NativeKafkaRequest(cluster, broker, produce((short) -1, records))) {
            var response = (ProduceResponse) wire.receive(30_000);
            var result = response.data().responses().iterator().next().partitionResponses().get(0);
            assertEquals(Errors.NONE, Errors.forCode(result.errorCode()));
            assertEquals(expectedOffset, result.baseOffset());
        }
    }

    static void assertWireBatch(int port, Uuid topicId, long requested, long base,
            long highWatermark, List<String> values) throws Exception {
        var partition = new TopicPartition(TOPIC, 0);
        short version = 16;
        var fetch = FetchRequest.Builder.forConsumer(version, 0, 0,
                        Map.of(partition, new FetchRequest.PartitionData(topicId, requested,
                                FetchRequest.INVALID_LOG_START_OFFSET, 1, Optional.empty())))
                .setMaxBytes(1).isolationLevel(IsolationLevel.READ_UNCOMMITTED).build(version);
        try (var wire = new NativeKafkaRequest(port, fetch)) {
            var response = (FetchResponse) wire.receive(30_000);
            var result = response.responseData(Map.of(topicId, TOPIC), version).get(partition);
            assertEquals(Errors.NONE, Errors.forCode(result.errorCode()));
            assertEquals(highWatermark, result.highWatermark());
            var batches = FetchResponse.recordsOrFail(result).batches().iterator();
            assertTrue(batches.hasNext(), "Fetch split or dropped an oversized first batch");
            var batch = batches.next();
            assertEquals(base, batch.baseOffset());
            assertEquals(base + values.size() - 1, batch.lastOffset());
            assertEquals(CompressionType.GZIP, batch.compressionType());
            batch.ensureValid();
            var actual = new ArrayList<String>();
            for (var record : batch) {
                assertEquals(base + actual.size(), record.offset());
                actual.add(StandardCharsets.UTF_8.decode(record.value()).toString());
            }
            assertEquals(values, actual);
            assertFalse(batches.hasNext(), "Fetch exceeded its budget after the first batch");
        }
    }

    private static KafkaClusterTestKit nativeCluster(Path inputs, HoldDurability observer, int brokers) throws Exception {
        return nativeCluster(inputs, observer, brokers, NereusKafkaConfigs.FETCH_OPERATION_MAX_REREADS_DEFAULT);
    }

    private static KafkaClusterTestKit nativeCluster(Path inputs, HoldDurability observer, int brokers,
            int fetchOperationMaxRereads) throws Exception {
        KafkaBookKeeperOwnerRelocationRealTest.writeCapability(inputs, Optional.empty());
        var bootstrap = BootstrapMetadata.fromVersion(MetadataVersion.latestTesting(), "nsip-ack")
                .copyWithFeatureRecord("nereus.storage.version", (short) 2);
        var nodes = new TestKitNodes.Builder(bootstrap).setNumBrokerNodes(brokers).setNumControllerNodes(1).build();
        var builder = new KafkaClusterTestKit.Builder(nodes)
                .setBrokerStorageRuntimeFactory(context -> context.config().brokerId() == 0
                        ? new NereusBrokerStorageRuntime(context, new NereusKafkaOwnedProviderRuntime(context, observer))
                        : NereusBrokerStorageRuntimeFactory.production().create(context))
                .setControllerStorageRuntimeFactory(new NereusControllerStorageRuntimeFactory());
        KafkaBookKeeperOwnerRelocationRealTest.nativeConfig(inputs, System.getProperty("nereus.nsip.bkUri"),
                System.getProperty("nereus.nsip.oxia")).forEach(builder::setConfigProp);
        builder.setConfigProp(NereusKafkaConfigs.FETCH_OPERATION_MAX_REREADS_CONFIG, fetchOperationMaxRereads);
        return builder.build();
    }

    private static void assertAckAfterPublication(Properties client, HoldDurability observer,
            KafkaBookKeeperPartitionV1 storage, String acks, long offset) throws Exception {
        var config = new Properties();
        config.putAll(client);
        config.setProperty("key.serializer", StringSerializer.class.getName());
        config.setProperty("value.serializer", StringSerializer.class.getName());
        config.setProperty("enable.idempotence", "false");
        config.setProperty("acks", acks);
        var cut = new Cut(offset, new CompletableFuture<>(), new CompletableFuture<>());
        int controlCalls = observer.controlCalls.get();
        assertTrue(controlCalls > 0, "Owner admission must exercise the real Oxia call counter");
        try (var producer = new KafkaProducer<String, String>(config)) {
            observer.armed.set(cut);
            try {
                var result = producer.send(new ProducerRecord<>(TOPIC, 0, "key", "acks-" + acks));
                var proof = cut.durable().get(20, TimeUnit.SECONDS);
                assertEquals(2, proof.acknowledgedBookies());
                var pending = storage.capture().root().frontiers();
                assertEquals(offset, pending.highWatermark());
                assertEquals(offset, pending.readableEndOffset());
                assertThrows(TimeoutException.class, () -> result.get(250, TimeUnit.MILLISECONDS),
                        "Produce was acknowledged before the coherent publication");
                cut.release().complete(null);
                assertEquals(offset, result.get(30, TimeUnit.SECONDS).offset());
                assertEquals(offset + 1, storage.capture().root().frontiers().highWatermark());
                assertEquals(controlCalls, observer.controlCalls.get(), "Produce used control metadata before ACK");
            } finally {
                observer.armed.set(null);
                cut.release().complete(null);
            }
        }
    }

    static ProduceRequest produce(short acks, String value) {
        return produce(acks, MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(value.getBytes(StandardCharsets.UTF_8))));
    }

    static ProduceRequest produce(short acks, MemoryRecords raw) {
        var topic = new ProduceRequestData.TopicProduceData()
                .setName(TOPIC)
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(raw)));
        var topics = new ProduceRequestData.TopicProduceDataCollection();
        topics.add(topic);
        return new ProduceRequest(new ProduceRequestData().setAcks(acks).setTimeoutMs(10_000).setTopicData(topics),
                (short) 9);
    }

    private record Cut(long offset, CompletableFuture<AppendQuorumProofV1> durable, CompletableFuture<Void> release,
            boolean unknown) {
        private Cut(long offset, CompletableFuture<AppendQuorumProofV1> durable, CompletableFuture<Void> release) {
            this(offset, durable, release, false);
        }
    }

    private static final class HoldDurability implements NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver {
        private final AtomicReference<Cut> armed = new AtomicReference<>();
        private final CompletableFuture<KafkaBookKeeperPartitionV1> storage = new CompletableFuture<>();
        private final AtomicInteger controlCalls = new AtomicInteger();

        @Override
        public AsyncOxiaClient controlClient(AsyncOxiaClient actual) {
            return (AsyncOxiaClient) Proxy.newProxyInstance(AsyncOxiaClient.class.getClassLoader(),
                    new Class<?>[]{AsyncOxiaClient.class}, (proxy, method, arguments) -> {
                        if (method.getDeclaringClass() != Object.class) controlCalls.incrementAndGet();
                        try {
                            return method.invoke(actual, arguments);
                        } catch (InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                    });
        }

        @Override
        public void afterReady(TopicIdPartition partition, NereusKafkaOwnedProviderRuntime.Opened opened) {
            if (partition.topic().equals(TOPIC)) {
                storage.complete(opened.storage());
            }
        }

        @Override
        public CompletionStage<ProviderMutationResultV1<AppendQuorumProofV1>> afterAppend(
                RunLedgerAppendRequestV1 request, CompletionStage<ProviderMutationResultV1<AppendQuorumProofV1>> actual) {
            var cut = armed.get();
            if (cut == null) {
                return actual;
            }
            var buffer = request.payload().readOnlyBuffer();
            var bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            var decoded = Nbke2CodecV1.decode(bytes, request.handle().ledgerIdentity().ledgerId(), request.expectedEntryId());
            if (!(decoded instanceof Nbke2DataV1 data) || data.baseOffset() != cut.offset()) {
                return actual;
            }
            return actual.whenComplete((result, failure) -> {
                if (failure != null) {
                    cut.durable().completeExceptionally(failure);
                } else if (result.exactProof().isEmpty()) {
                    cut.durable().completeExceptionally(new AssertionError("BK did not persist DATA: " + result.outcome()));
                } else {
                    cut.durable().complete(result.exactProof().orElseThrow());
                }
            }).thenCompose(result -> cut.release().thenApply(ignored ->
                    cut.unknown() ? ProviderMutationResultV1.outcomeUnknown() : result));
        }
    }
}
