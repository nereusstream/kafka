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
package kafka.server.nsip;

import kafka.log.nereus.NereusUnifiedLog;
import kafka.server.nereus.NereusBrokerStorageRuntimeFactory;
import kafka.server.nereus.NereusControllerStorageRuntimeFactory;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Actual Kafka protocol, Controller and Brokers, over the admitted real BK/Oxia backend. */
public class KafkaBookKeeperOwnerRelocationRealTest {
    @Test
    @Timeout(value = 240)
    public void coldOwnerContinuesProduceFetchAndTransactionAcrossTwoNativeRelocations() throws Exception {
        String uri = System.getProperty("nereus.nsip.bkUri");
        String oxia = System.getProperty("nereus.nsip.oxia");
        if (uri == null || oxia == null) throw new IllegalStateException("real BK URI and Oxia address are required");
        Path inputs = Files.createTempDirectory("nereus-nsip-native-");
        writeCapability(inputs);
        var bootstrap = BootstrapMetadata.fromVersion(MetadataVersion.latestTesting(), "nsip-native")
                .copyWithFeatureRecord("nereus.storage.version", (short) 2);
        try (var delayedMetadata = new DelayedMetadata(oxia)) {
            var nodes = new TestKitNodes.Builder(bootstrap)
                    .setPerServerProperties(
                            Map.of(1, Map.of("nereus.kafka.storage.oxia.service.address", delayedMetadata.address())))
                    .setNumBrokerNodes(3)
                    .setNumControllerNodes(1)
                    .build();
            var builder = new KafkaClusterTestKit.Builder(nodes)
                    .setBrokerStorageRuntimeFactory(new NereusBrokerStorageRuntimeFactory())
                    .setControllerStorageRuntimeFactory(new NereusControllerStorageRuntimeFactory());
            nativeConfig(inputs, uri, oxia).forEach(builder::setConfigProp);
            try (KafkaClusterTestKit cluster = builder.build()) {
                cluster.format();
                cluster.startup();
                cluster.waitForReadyBrokers();
                var client = new Properties();
                client.put("bootstrap.servers", cluster.bootstrapServers());
                try (Admin admin = Admin.create(client)) {
                    admin.createTopics(List.of(
                                    new NewTopic("native-cold", Map.of(0, List.of(0))),
                                    new NewTopic("__transaction_state", Map.of(0, List.of(0)))
                                            .configs(Map.of("cleanup.policy", "compact")),
                                    new NewTopic("__consumer_offsets", Map.of(0, List.of(0)))
                                            .configs(Map.of("cleanup.policy", "compact"))))
                            .all()
                            .get(30, TimeUnit.SECONDS);
                    Properties producerConfig = new Properties();
                    producerConfig.putAll(client);
                    producerConfig.put("key.serializer", StringSerializer.class.getName());
                    producerConfig.put("value.serializer", StringSerializer.class.getName());
                    producerConfig.put("enable.idempotence", "true");
                    producerConfig.put("acks", "all");
                    producerConfig.put("request.timeout.ms", "10000");
                    producerConfig.put("delivery.timeout.ms", "30000");
                    Properties transactionConfig = new Properties();
                    transactionConfig.putAll(producerConfig);
                    transactionConfig.put("transactional.id", "native-ongoing");
                    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig);
                            KafkaProducer<String, String> transaction = new KafkaProducer<>(transactionConfig)) {
                        assertEquals(
                                0,
                                producer.send(new ProducerRecord<>("native-cold", 0, "key", "before"))
                                        .get(30, TimeUnit.SECONDS)
                                        .offset());
                        transaction.initTransactions();
                        transaction.beginTransaction();
                        assertEquals(
                                1,
                                transaction
                                        .send(new ProducerRecord<>("native-cold", 0, "key", "transaction"))
                                        .get(30, TimeUnit.SECONDS)
                                        .offset());
                        assertEquals(List.of("before"), consume(client, 1));
                        commitGroupOffset(client, 1);
                        int first = leader(admin);
                        assertEquals(0, first);
                        var lost = lostResponseAppend(cluster, first);
                        awaitCondition(
                                () -> log(cluster, first).logEndOffset() == 3,
                                "disconnected Produce did not reach shared commit");
                        delayedMetadata.pauseResponses();
                        cluster.brokers().get(first).shutdown();
                        int pending = awaitLeader(admin, first);
                        assertEquals(1, pending);
                        delayedMetadata.awaitHeldResponse();
                        awaitCondition(
                                () -> cluster.brokers()
                                        .get(pending)
                                        .replicaManager()
                                        .onlinePartition(new TopicPartition("native-cold", 0))
                                        .isDefined(),
                                "pending native partition was not published");
                        var pendingPartition = cluster.brokers()
                                .get(pending)
                                .replicaManager()
                                .onlinePartition(new TopicPartition("native-cold", 0))
                                .get();
                        var pendingLog = log(cluster, pending);
                        assertFalse(pendingLog.nereusWritable(pendingPartition.getLeaderEpoch()));
                        admin.alterPartitionReassignments(Map.of(
                                        new TopicPartition("native-cold", 0),
                                                java.util.Optional.of(
                                                        new org.apache.kafka.clients.admin.NewPartitionReassignment(
                                                                List.of(2))),
                                        new TopicPartition("__transaction_state", 0),
                                                java.util.Optional.of(
                                                        new org.apache.kafka.clients.admin.NewPartitionReassignment(
                                                                List.of(2))),
                                        new TopicPartition("__consumer_offsets", 0),
                                                java.util.Optional.of(
                                                        new org.apache.kafka.clients.admin.NewPartitionReassignment(
                                                                List.of(2)))))
                                .all()
                                .get(30, TimeUnit.SECONDS);
                        int second = awaitLeader(admin, pending);
                        assertEquals(2, second);
                        awaitCondition(
                                () -> log(cluster, second).highWatermark() == 3,
                                "replacement did not recover shared prefix");
                        delayedMetadata.resumeResponses();
                        awaitCondition(() -> !pendingPartition.isLeader(), "superseded recovery retained leadership");
                        assertFalse(pendingLog.nereusWritable(1));
                        var retry = (ProduceResponse) request(cluster, second, lost, true);
                        var retryPartition = retry.data()
                                .responses()
                                .iterator()
                                .next()
                                .partitionResponses()
                                .get(0);
                        assertEquals(Errors.NONE.code(), retryPartition.errorCode());
                        assertEquals(2, retryPartition.baseOffset());
                        assertEquals(3, log(cluster, second).logEndOffset());
                        assertEquals(1, readGroupOffset(client));
                        transaction.commitTransaction();
                        assertEquals(
                                4,
                                producer.send(new ProducerRecord<>("native-cold", 0, "key", "after-first"))
                                        .get(30, TimeUnit.SECONDS)
                                        .offset());
                        assertEquals(
                                List.of("before", "transaction", "lost-response", "after-first"), consume(client, 4));
                        cluster.brokers().get(second).shutdown();
                        int third = awaitLeader(admin, second);
                        assertNotEquals(second, third);
                        assertEquals(
                                5,
                                producer.send(new ProducerRecord<>("native-cold", 0, "key", "after-second"))
                                        .get(30, TimeUnit.SECONDS)
                                        .offset());
                        assertEquals(
                                List.of("before", "transaction", "lost-response", "after-first", "after-second"),
                                consume(client, 5));
                        assertEquals(1, readGroupOffset(client));
                    }
                    cluster.fatalFaultHandler().maybeRethrowFirstException();
                    cluster.nonFatalFaultHandler().maybeRethrowFirstException();
                }
            }
        } finally {
            try (var files = Files.walk(inputs)) {
                for (var file :
                        files.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(file);
            }
        }
    }

    @Test
    @Timeout(value = 240)
    public void smallRunsCheckpointAcrossNativeEpoch256AndRepeatedColdTakeover() throws Exception {
        String uri = System.getProperty("nereus.nsip.bkUri");
        String oxia = System.getProperty("nereus.nsip.oxia");
        if (uri == null || oxia == null) throw new IllegalStateException("real BK/Oxia inputs are required");
        Path inputs = Files.createTempDirectory("nereus-nsip-rollover-");
        writeCapability(inputs);
        var bootstrap = BootstrapMetadata.fromVersion(MetadataVersion.latestTesting(), "nsip-checkpoint")
                .copyWithFeatureRecord("nereus.storage.version", (short) 2);
        var nodes = new TestKitNodes.Builder(bootstrap)
                .setNumBrokerNodes(3)
                .setNumControllerNodes(1)
                .build();
        var builder = new KafkaClusterTestKit.Builder(nodes)
                .setBrokerStorageRuntimeFactory(new NereusBrokerStorageRuntimeFactory())
                .setControllerStorageRuntimeFactory(new NereusControllerStorageRuntimeFactory());
        nativeConfig(inputs, uri, oxia).forEach(builder::setConfigProp);
        builder.setConfigProp("nereus.kafka.storage.bookkeeper.max.entries.per.ledger", "8");
        builder.setConfigProp("nereus.kafka.storage.bookkeeper.max.bytes.per.ledger", "1048576");
        builder.setConfigProp("message.max.bytes", "4096");
        try (KafkaClusterTestKit cluster = builder.build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            var client = new Properties();
            client.put("bootstrap.servers", cluster.bootstrapServers());
            try (Admin admin = Admin.create(client)) {
                admin.createTopics(List.of(
                                new NewTopic("native-cold", Map.of(0, List.of(0))),
                                new NewTopic("__transaction_state", Map.of(0, List.of(0)))
                                        .configs(Map.of("cleanup.policy", "compact")),
                                new NewTopic("__consumer_offsets", Map.of(0, List.of(0)))
                                        .configs(Map.of("cleanup.policy", "compact"))))
                        .all()
                        .get(30, TimeUnit.SECONDS);
                var topicId = admin.describeTopics(List.of("native-cold"))
                        .allTopicNames()
                        .get()
                        .get("native-cold")
                        .topicId();
                var controller = (org.apache.kafka.controller.QuorumController)
                        cluster.controllers().values().iterator().next().controller();
                org.apache.kafka.controller.NereusNativeLeaderEpochTestBridge.advance(controller, topicId, 255)
                        .get(30, TimeUnit.SECONDS);
                awaitCondition(() -> log(cluster, 0).nereusWritable(255), "Owner epoch 256 did not activate");
                var properties = new Properties();
                properties.putAll(client);
                properties.put("key.serializer", StringSerializer.class.getName());
                properties.put("value.serializer", StringSerializer.class.getName());
                properties.put("enable.idempotence", "true");
                properties.put("acks", "all");
                properties.put("transactional.id", "native-checkpoint-transaction");
                try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
                    producer.initTransactions();
                    producer.beginTransaction();
                    var expected = new java.util.ArrayList<String>();
                    for (int offset = 0; offset < 40; offset++) {
                        String value = "checkpoint-" + offset;
                        expected.add(value);
                        assertEquals(
                                offset,
                                producer.send(new ProducerRecord<>("native-cold", 0, "key", value))
                                        .get(30, TimeUnit.SECONDS)
                                        .offset());
                    }
                    assertEquals(40, log(cluster, 0).highWatermark());
                    assertEquals(0, log(cluster, 0).lastStableOffset());
                    org.apache.kafka.controller.NereusNativeLeaderEpochTestBridge.advance(controller, topicId, 256)
                            .get(30, TimeUnit.SECONDS);
                    awaitCondition(
                            () -> log(cluster, 0).nereusWritable(256),
                            "Owner epoch 257 did not activate from checkpoint and tail");
                    assertEquals(40, log(cluster, 0).highWatermark());
                    assertEquals(0, log(cluster, 0).lastStableOffset());
                    producer.commitTransaction();
                    assertEquals(expected, consume(client, 40));
                    commitGroupOffset(client, 40);
                    producer.beginTransaction();
                    for (int offset = 41; offset < 55; offset++) {
                        assertEquals(
                                offset,
                                producer.send(new ProducerRecord<>("native-cold", 0, "key", "aborted-" + offset))
                                        .get(30, TimeUnit.SECONDS)
                                        .offset());
                    }
                    cluster.brokers().get(0).shutdown();
                    int next = awaitLeader(admin, 0);
                    awaitCondition(
                            () -> log(cluster, next).highWatermark() == 55, "second cold Owner did not recover tail");
                    assertEquals(41, log(cluster, next).lastStableOffset());
                    producer.abortTransaction();
                    producer.beginTransaction();
                    assertEquals(
                            56,
                            producer.send(new ProducerRecord<>("native-cold", 0, "key", "after-checkpoint"))
                                    .get(30, TimeUnit.SECONDS)
                                    .offset());
                    producer.commitTransaction();
                    expected.add("after-checkpoint");
                    assertEquals(expected, consume(client, 41));
                    assertEquals(40, readGroupOffset(client));
                }
                cluster.fatalFaultHandler().maybeRethrowFirstException();
                cluster.nonFatalFaultHandler().maybeRethrowFirstException();
            }
        } finally {
            try (var files = Files.walk(inputs)) {
                for (var file :
                        files.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(file);
            }
        }
    }

    private static void writeCapability(Path inputs) throws Exception {
        Path capability = inputs.resolve("capability.properties");
        Path password = inputs.resolve("password");
        Files.write(password, new byte[0]);
        Files.writeString(capability, """
            providerScopeSha256=0000000000000000000000000000000000000000000000000000000000000001
            clientSourceCommit=cd06340851d6d657b7c7546df01df365c18980de
            clientArtifactSha256=8e64f2b7436bb814705f611eb0ac48d64d90de7a50d295905c459d89bc3f9d8f
            serverSourceCommit=cd06340851d6d657b7c7546df01df365c18980de
            serverImageManifestSha256=c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d
            explicitEntryIdsSupported=true
            fencingSupported=true
            recoverySupported=true
            protocolMode=V3
            clientFrameLimitBytes=5242880
            serverFrameLimitBytes=5242880
            maximumAddPayloadBytes=5242771
            ensembleSize=3
            writeQuorumSize=3
            ackQuorumSize=2
            digestType=CRC32C
            connectMillis=10000
            addMillis=5000
            readMillis=5000
            recoveryMillis=30000
            credentialIdentityVersion=bk-k0-no-auth:v1
            configurationDigest=eaf41c4b42b767b8ea6e86023a784425b8073f174dbade92b4249c8f3d301dbd
            policyCatalogDigest=40e2a412af649dfdfbcb02060d8c686c32ee9212f5f72f810e60c503d3778985
            """);
    }

    private static Map<String, Object> nativeConfig(Path inputs, String uri, String oxia) {
        Path capability = inputs.resolve("capability.properties");
        Path password = inputs.resolve("password");
        Map<String, Object> config = Map.ofEntries(
                Map.entry("nereus.kafka.storage.enabled", "true"),
                Map.entry("nereus.kafka.storage.cluster", Uuid.randomUuid().toString()),
                Map.entry("nereus.kafka.storage.profile", "BOOKKEEPER_WAL_ONLY"),
                Map.entry("nereus.kafka.storage.oxia.service.address", oxia),
                Map.entry(
                        "nereus.kafka.storage.cache.dir",
                        inputs.resolve("cache").toString()),
                Map.entry("nereus.kafka.bookkeeper.capability.file", capability.toString()),
                Map.entry("nereus.kafka.storage.bookkeeper.metadata.service.uri", uri),
                Map.entry(
                        "nereus.kafka.storage.bookkeeper.deployment.id",
                        Uuid.randomUuid().toString()),
                Map.entry("nereus.kafka.storage.bookkeeper.cluster.alias", "nsip-real-bk"),
                Map.entry(
                        "nereus.kafka.storage.bookkeeper.provider.scope.sha256",
                        "0000000000000000000000000000000000000000000000000000000000000001"),
                Map.entry("nereus.kafka.storage.bookkeeper.ensemble.size", "3"),
                Map.entry("nereus.kafka.storage.bookkeeper.write.quorum.size", "3"),
                Map.entry("nereus.kafka.storage.bookkeeper.ack.quorum.size", "2"),
                Map.entry("nereus.kafka.storage.bookkeeper.password.file", password.toString()),
                Map.entry("nereus.kafka.storage.bookkeeper.password.version", "bk-k0-no-auth:v1"),
                Map.entry("nereus.kafka.storage.recovery.timeout.ms", "60000"),
                Map.entry("default.replication.factor", "1"),
                Map.entry("min.insync.replicas", "1"),
                Map.entry("offsets.topic.replication.factor", "1"),
                Map.entry("offsets.topic.num.partitions", "1"),
                Map.entry("transaction.state.log.replication.factor", "1"),
                Map.entry("transaction.state.log.min.isr", "1"),
                Map.entry("transaction.state.log.num.partitions", "1"),
                Map.entry("share.coordinator.state.topic.replication.factor", "1"),
                Map.entry("share.coordinator.state.topic.min.isr", "1"),
                Map.entry("log.cleaner.enable", "false"),
                Map.entry("auto.create.topics.enable", "false"),
                Map.entry("broker.session.timeout.ms", "3000"),
                Map.entry("broker.heartbeat.interval.ms", "500"));
        return config;
    }

    private static NereusUnifiedLog log(KafkaClusterTestKit cluster, int broker) {
        return (NereusUnifiedLog) cluster.brokers()
                .get(broker)
                .replicaManager()
                .localLog(new TopicPartition("native-cold", 0))
                .get();
    }

    private static void awaitCondition(BooleanSupplier condition, String failure) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            try {
                if (condition.getAsBoolean()) return;
            } catch (java.util.NoSuchElementException absentPartition) {
                // ReplicaManager publication follows the actual Controller assignment asynchronously.
            }
            Thread.sleep(25);
        }
        throw new AssertionError(failure);
    }

    private static Properties groupProperties(Properties client) {
        var config = new Properties();
        config.putAll(client);
        config.put("key.deserializer", StringDeserializer.class.getName());
        config.put("value.deserializer", StringDeserializer.class.getName());
        config.put("group.id", "durable-native-group");
        config.put("enable.auto.commit", "false");
        return config;
    }

    private static void commitGroupOffset(Properties client, long offset) {
        try (var consumer = new KafkaConsumer<String, String>(groupProperties(client))) {
            consumer.commitSync(
                    Map.of(new TopicPartition("native-cold", 0), new OffsetAndMetadata(offset)),
                    Duration.ofSeconds(30));
        }
    }

    private static long readGroupOffset(Properties client) {
        try (var consumer = new KafkaConsumer<String, String>(groupProperties(client))) {
            return consumer.committed(java.util.Set.of(new TopicPartition("native-cold", 0)), Duration.ofSeconds(30))
                    .get(new TopicPartition("native-cold", 0))
                    .offset();
        }
    }

    private static ProduceRequest lostResponseAppend(KafkaClusterTestKit cluster, int broker) throws Exception {
        var initialized = (InitProducerIdResponse) request(
                cluster,
                broker,
                new InitProducerIdRequest.Builder(new InitProducerIdRequestData()
                                .setTransactionalId(null)
                                .setTransactionTimeoutMs(30000))
                        .build((short) 4),
                true);
        assertEquals(Errors.NONE.code(), initialized.data().errorCode());
        var raw = MemoryRecords.withIdempotentRecords(
                Compression.NONE,
                initialized.data().producerId(),
                initialized.data().producerEpoch(),
                0,
                new SimpleRecord(
                        "key".getBytes(StandardCharsets.UTF_8), "lost-response".getBytes(StandardCharsets.UTF_8)));
        var topic = new ProduceRequestData.TopicProduceData()
                .setName("native-cold")
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData()
                        .setIndex(0)
                        .setRecords(raw)));
        var topics = new ProduceRequestData.TopicProduceDataCollection();
        topics.add(topic);
        var request = new ProduceRequest(
                new ProduceRequestData().setAcks((short) -1).setTimeoutMs(10000).setTopicData(topics), (short) 9);
        request(cluster, broker, request, false);
        return request;
    }

    private static AbstractResponse request(
            KafkaClusterTestKit cluster, int broker, AbstractRequest request, boolean receive) throws Exception {
        var header = new RequestHeader(request.apiKey(), request.version(), "nsip-disconnect", 1);
        try (var socket = new Socket(
                "127.0.0.1",
                cluster.brokers()
                        .get(broker)
                        .socketServer()
                        .boundPort(cluster.nodes().brokerListenerName()))) {
            socket.setSoTimeout(30000);
            var bytes = request.serializeWithHeader(header);
            var output = new DataOutputStream(socket.getOutputStream());
            output.writeInt(bytes.remaining());
            output.write(bytes.array(), bytes.arrayOffset() + bytes.position(), bytes.remaining());
            output.flush();
            if (!receive) return null;
            var input = new DataInputStream(socket.getInputStream());
            int length = input.readInt();
            assertTrue(length > 0 && length < 1048576);
            byte[] response = new byte[length];
            input.readFully(response);
            return AbstractResponse.parseResponse(ByteBuffer.wrap(response), header);
        }
    }

    /** Delays actual Oxia response bytes for one Broker; all durable operations still execute on the real server. */
    private static final class DelayedMetadata implements AutoCloseable {
        private final ServerSocket listener;
        private final String host;
        private final int port;
        private final ExecutorService workers = Executors.newCachedThreadPool(task -> {
            var thread = new Thread(task, "nsip-oxia-transport");
            thread.setDaemon(true);
            return thread;
        });
        private final List<Socket> sockets = new CopyOnWriteArrayList<>();
        private volatile CountDownLatch responseGate = new CountDownLatch(0);
        private final CountDownLatch held = new CountDownLatch(1);
        private volatile boolean closed;

        DelayedMetadata(String address) throws Exception {
            var parts = address.split(":");
            host = parts[0];
            port = Integer.parseInt(parts[1]);
            listener = new ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress());
            workers.execute(() -> {
                while (!closed)
                    try {
                        var client = listener.accept();
                        var server = new Socket(host, port);
                        sockets.add(client);
                        sockets.add(server);
                        workers.execute(() -> relay(client, server, false));
                        workers.execute(() -> relay(server, client, true));
                    } catch (java.io.IOException failure) {
                        if (!closed) throw new java.io.UncheckedIOException(failure);
                    }
            });
        }

        String address() {
            return "127.0.0.1:" + listener.getLocalPort();
        }

        void pauseResponses() {
            responseGate = new CountDownLatch(1);
        }

        void resumeResponses() {
            responseGate.countDown();
        }

        void awaitHeldResponse() throws Exception {
            assertTrue(held.await(30, TimeUnit.SECONDS), "real Oxia response was not held");
        }

        private void relay(Socket from, Socket to, boolean response) {
            try {
                byte[] bytes = new byte[8192];
                int length;
                while ((length = from.getInputStream().read(bytes)) >= 0) {
                    var gate = responseGate;
                    if (response && gate.getCount() != 0) {
                        held.countDown();
                        if (!gate.await(30, TimeUnit.SECONDS))
                            throw new java.io.IOException("Oxia response hold exceeded test bound");
                    }
                    to.getOutputStream().write(bytes, 0, length);
                    to.getOutputStream().flush();
                }
            } catch (java.io.IOException failure) {
                // Broker channel closure is expected during owner relocation.
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
            } finally {
                try {
                    from.close();
                } catch (java.io.IOException ignored) {
                }
                try {
                    to.close();
                } catch (java.io.IOException ignored) {
                }
            }
        }

        @Override
        public void close() throws Exception {
            closed = true;
            resumeResponses();
            listener.close();
            for (var socket : sockets) socket.close();
            workers.shutdownNow();
            assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static int leader(Admin admin) throws Exception {
        var partition = admin.describeTopics(List.of("native-cold"))
                .allTopicNames()
                .get(10, TimeUnit.SECONDS)
                .get("native-cold")
                .partitions()
                .get(0);
        assertEquals(1, partition.replicas().size());
        assertEquals(1, partition.isr().size());
        return partition.leader() == null ? -1 : partition.leader().id();
    }

    private static int awaitLeader(Admin admin, int previous) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            int owner = leader(admin);
            if (owner >= 0 && owner != previous) return owner;
            Thread.sleep(50);
        }
        throw new AssertionError("Controller did not relocate the sole native replica");
    }

    private static List<String> consume(Properties client, int count) {
        Properties config = new Properties();
        config.putAll(client);
        config.put("key.deserializer", StringDeserializer.class.getName());
        config.put("value.deserializer", StringDeserializer.class.getName());
        config.put("isolation.level", "read_committed");
        config.put("enable.auto.commit", "false");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.assign(List.of(new TopicPartition("native-cold", 0)));
            consumer.seekToBeginning(consumer.assignment());
            var values = new java.util.ArrayList<String>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (values.size() < count && System.nanoTime() < deadline)
                consumer.poll(Duration.ofMillis(200)).forEach(record -> values.add(record.value()));
            assertEquals(count, values.size());
            return values;
        }
    }
}
