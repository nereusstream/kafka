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
import kafka.server.nsip.NativeKafkaRequest;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.server.common.MetadataVersion;
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
import java.nio.file.Path;
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
    public void staleQuorumCallbackCannotPublishAfterNativeOwnerChange(@TempDir Path inputs) throws Exception {
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
                var partition = new TopicPartition(TOPIC, 0);
                var old = observer.storage.get(30, TimeUnit.SECONDS);
                assertAckAfterPublication(client, observer, old, "all", 0);
                var cut = new Cut(1, new CompletableFuture<>(), new CompletableFuture<>());
                observer.armed.set(cut);
                try (var wire = new NativeKafkaRequest(cluster, 0, produce((short) -1, "stale-owner"))) {
                    assertEquals(2, cut.durable().get(20, TimeUnit.SECONDS).acknowledgedBookies());
                    assertEquals(1, old.capture().root().frontiers().highWatermark());
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
                    assertEquals(1, after.highWatermark());
                    assertEquals(1, after.readableEndOffset());
                    assertEquals(1, KafkaBookKeeperOwnerRelocationRealTest.awaitLeader(admin, 0));
                } finally {
                    observer.armed.set(null);
                    cut.release().complete(null);
                }
            }
            cluster.fatalFaultHandler().maybeRethrowFirstException();
            cluster.nonFatalFaultHandler().maybeRethrowFirstException();
        }
    }

    private static KafkaClusterTestKit nativeCluster(Path inputs, HoldDurability observer, int brokers) throws Exception {
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

    private static ProduceRequest produce(short acks, String value) {
        var raw = MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        var topic = new ProduceRequestData.TopicProduceData()
                .setName(TOPIC)
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(raw)));
        var topics = new ProduceRequestData.TopicProduceDataCollection();
        topics.add(topic);
        return new ProduceRequest(new ProduceRequestData().setAcks(acks).setTimeoutMs(10_000).setTopicData(topics),
                (short) 9);
    }

    private record Cut(long offset, CompletableFuture<AppendQuorumProofV1> durable, CompletableFuture<Void> release) {
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
            }).thenCompose(result -> cut.release().thenApply(ignored -> result));
        }
    }
}
