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
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceRequest;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** The native Produce response stays pending after real BK persistence until shared publication. */
public class KafkaBookKeeperAckRealTest {
    private static final String TOPIC = "native-cold";

    @Test
    @Timeout(180)
    public void bothAckModesWaitForSharedCommitAndAckZeroHasNoResponse(@TempDir Path inputs) throws Exception {
        var observer = new HoldDurability();
        KafkaBookKeeperOwnerRelocationRealTest.writeCapability(inputs, Optional.empty());
        var bootstrap = BootstrapMetadata.fromVersion(MetadataVersion.latestTesting(), "nsip-ack")
                .copyWithFeatureRecord("nereus.storage.version", (short) 2);
        var nodes = new TestKitNodes.Builder(bootstrap).setNumBrokerNodes(1).setNumControllerNodes(1).build();
        var builder = new KafkaClusterTestKit.Builder(nodes)
                .setBrokerStorageRuntimeFactory(context -> new NereusBrokerStorageRuntime(
                        context, new NereusKafkaOwnedProviderRuntime(context, observer)))
                .setControllerStorageRuntimeFactory(new NereusControllerStorageRuntimeFactory());
        KafkaBookKeeperOwnerRelocationRealTest.nativeConfig(inputs, System.getProperty("nereus.nsip.bkUri"),
                System.getProperty("nereus.nsip.oxia")).forEach(builder::setConfigProp);
        try (var cluster = builder.build()) {
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
            try (var request = new NativeKafkaRequest(cluster, 0, fireAndForget())) {
                TestUtils.waitForCondition(() -> storage.capture().root().frontiers().highWatermark() == 3,
                        "acks=0 Produce did not commit");
                assertThrows(SocketTimeoutException.class, () -> request.receive(250),
                        "acks=0 Produce sent a response");
            }
            assertEquals(List.of("acks-1", "acks-all", "acks-0"),
                    KafkaBookKeeperOwnerRelocationRealTest.consume(client, 3));
            cluster.fatalFaultHandler().maybeRethrowFirstException();
            cluster.nonFatalFaultHandler().maybeRethrowFirstException();
        }
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
            } finally {
                observer.armed.set(null);
                cut.release().complete(null);
            }
        }
    }

    private static ProduceRequest fireAndForget() {
        var raw = MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord("acks-0".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        var topic = new ProduceRequestData.TopicProduceData()
                .setName(TOPIC)
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(raw)));
        var topics = new ProduceRequestData.TopicProduceDataCollection();
        topics.add(topic);
        return new ProduceRequest(new ProduceRequestData().setAcks((short) 0).setTimeoutMs(10_000).setTopicData(topics),
                (short) 9);
    }

    private record Cut(long offset, CompletableFuture<AppendQuorumProofV1> durable, CompletableFuture<Void> release) {
    }

    private static final class HoldDurability implements NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver {
        private final AtomicReference<Cut> armed = new AtomicReference<>();
        private final CompletableFuture<KafkaBookKeeperPartitionV1> storage = new CompletableFuture<>();

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
