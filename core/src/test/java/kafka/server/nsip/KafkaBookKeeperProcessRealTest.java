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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.server.config.NereusKafkaConfigs;
import org.apache.kafka.snapshot.SnapshotPath;
import org.apache.kafka.snapshot.Snapshots;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Boots the production launcher in separate JVMs; no injected runtime or test Controller. */
public class KafkaBookKeeperProcessRealTest {
    private static final String TOPIC = "native-cold";
    private static final String CONTROLLER_RESTART_TOPIC = "after-controller-restart";

    @Test
    @Timeout(240)
    public void productionLauncherRecoversSnapshotAndRelocatesToColdBroker(
            @TempDir(cleanup = CleanupMode.ON_SUCCESS) Path work) throws Exception {
        try {
            runProductionNodes(work);
        } finally {
            copyProcessLogs(work);
        }
    }

    private static void runProductionNodes(Path work) throws Exception {
        String bkUri = System.getProperty("nereus.nsip.bkUri");
        String oxia = System.getProperty("nereus.nsip.oxia");
        KafkaBookKeeperOwnerRelocationRealTest.writeCapability(work, Optional.empty());
        var common = KafkaBookKeeperOwnerRelocationRealTest.nativeConfig(work, bkUri, oxia);
        int[] ports = allocatePorts();
        String clusterId = Uuid.randomUuid().toString();
        String voters = "2@127.0.0.1:" + ports[2] + ",3@127.0.0.1:" + ports[3] + ",4@127.0.0.1:" + ports[4];
        var controller = new NativeNode(work, common, 2, ports[2], voters, true);
        var secondController = new NativeNode(work, common, 3, ports[3], voters, true);
        var thirdController = new NativeNode(work, common, 4, ports[4], voters, true);
        var controllers = Map.of(2, controller, 3, secondController, 4, thirdController);
        var first = new NativeNode(work, common, 0, ports[0], voters, false);
        var next = new NativeNode(work, common, 1, ports[1], voters, false);
        System.out.println("Native process logs: " + work);
        try (controller; secondController; thirdController; first; next) {
            controller.assertRejectsImplicitFormat(clusterId);
            controller.assertRejectsLegacyFeatureFormat(clusterId);
            for (var voter : controllers.values()) voter.format(clusterId);
            first.format(clusterId);
            next.format(clusterId);
            for (var voter : controllers.values()) voter.start();
            first.start();
            Properties initialClient = client(ports[0]);
            try (Admin admin = Admin.create(initialClient)) {
                waitForBroker(admin, 0);
                Uuid topicId = admin.createTopics(List.of(new NewTopic(TOPIC, Map.of(0, List.of(0)))))
                        .topicId(TOPIC).get(30, TimeUnit.SECONDS);
                assertEquals(0, append(initialClient, "before-process-restart"));
                var leaderAck = new Properties();
                leaderAck.putAll(initialClient);
                leaderAck.setProperty("enable.idempotence", "false");
                leaderAck.setProperty("acks", "1");
                assertEquals(1, append(leaderAck, "leader-ack"));
                var quorum = admin.describeMetadataQuorum().quorumInfo().get(30, TimeUnit.SECONDS);
                long floor = quorum.highWatermark();
                assertTrue(floor > 0);
                for (var voter : controllers.values()) {
                    TestUtils.waitForCondition(() -> voter.hasSnapshotAtOrAfter(floor),
                            "A Controller did not persist a snapshot containing the original topic");
                }
                var formerLeader = controllers.get(quorum.leaderId());
                formerLeader.stop();
                awaitControllerFailover(admin, quorum.leaderId());
                var failoverTopicId = admin.createTopics(List.of(new NewTopic(CONTROLLER_RESTART_TOPIC, Map.of(0, List.of(0)))
                                .configs(Map.of("nereus.storage.profile", "BOOKKEEPER_WAL_ONLY"))))
                        .topicId(CONTROLLER_RESTART_TOPIC).get(30, TimeUnit.SECONDS);
                var failoverQuorum = admin.describeMetadataQuorum().quorumInfo().get(30, TimeUnit.SECONDS);
                long failoverFloor = failoverQuorum.highWatermark();
                assertTrue(failoverFloor > floor);
                formerLeader.assertRejectsDisabledRestart();
                formerLeader.start();
                TestUtils.waitForCondition(() -> formerLeader.hasSnapshotAtOrAfter(failoverFloor),
                        "Restarted Controller did not catch up to the metadata written after failover");
                System.out.printf("Controller election: %d -> %d, restart snapshot floor %d%n",
                        quorum.leaderId(), failoverQuorum.leaderId(), failoverFloor);
                assertEquals(topicId, topicId(admin, TOPIC));
                assertEquals(failoverTopicId, topicId(admin, CONTROLLER_RESTART_TOPIC));
                // This observer has an empty metadata log and cannot reconstruct the topic
                // from a local image. Verify an actual transferred snapshot on disk.
                next.start();
                waitForBroker(admin, 1);
                TestUtils.waitForCondition(() -> next.hasSnapshotAtOrAfter(floor),
                        "Cold Broker did not install a KRaft snapshot from the Controller");
                var assignment = Optional.of(new NewPartitionReassignment(List.of(1)));
                assertRejectedTopic(admin, new NewTopic("rf-two", 1, (short) 2),
                        Errors.INVALID_REPLICATION_FACTOR);
                assertRejectedTopic(admin, new NewTopic("min-isr-two", Map.of(0, List.of(0)))
                        .configs(Map.of("min.insync.replicas", "2")), Errors.INVALID_CONFIG);
                assertRejectedTopic(admin, new NewTopic("__share_group_state", Map.of(0, List.of(0))),
                        Errors.INVALID_CONFIG);
                assertRejectsUnadmittedProfiles(admin);
                admin.alterPartitionReassignments(Map.of(new TopicPartition(TOPIC, 0), assignment,
                                new TopicPartition(CONTROLLER_RESTART_TOPIC, 0), assignment))
                        .all().get(30, TimeUnit.SECONDS);
                assertEquals(1, KafkaBookKeeperOwnerRelocationRealTest.awaitLeader(admin, 0));
                TestUtils.waitForCondition(() -> admin.describeTopics(List.of(CONTROLLER_RESTART_TOPIC))
                                .allTopicNames().get(5, TimeUnit.SECONDS).get(CONTROLLER_RESTART_TOPIC)
                                .partitions().get(0).leader().id() == 1,
                        "Controller restart topic did not leave the original Broker before controlled shutdown");
                first.stop();
                assertTrue(Files.readString(first.directory.resolve("server.log"))
                                .contains("[BrokerServer id=0] shut down completed"),
                        "Original Broker did not report completed storage shutdown");
                Properties relocated = client(ports[1]);
                assertEquals(2, append(relocated, "after-cold-relocation"));
                assertEquals(List.of("before-process-restart", "leader-ack", "after-cold-relocation"),
                        KafkaBookKeeperOwnerRelocationRealTest.consume(relocated, 3));
                try (Admin relocatedAdmin = Admin.create(relocated)) {
                    assertEquals(topicId, topicId(relocatedAdmin, TOPIC));
                }
                first.assertRejectsForeignProviderScope();
            }
        }
    }

    public static void copyProcessLogs(Path work) throws Exception {
        String evidence = System.getProperty("nereus.nsip.processEvidenceDir");
        if (evidence == null) return;
        Path destination = Files.createDirectory(Path.of(evidence));
        for (String input : List.of("capability.properties", "object-capability", "m5-audit-policy.properties")) {
            var capability = work.resolve(input);
            if (Files.isRegularFile(capability)) Files.copy(capability, destination.resolve(input));
        }
        for (int id = 0; id < 5; id++) {
            Path source = work.resolve("node-" + id);
            if (!Files.isDirectory(source)) continue;
            Path node = Files.createDirectory(destination.resolve(source.getFileName()));
            for (String name : List.of("server.log", "launcher.log", "controller.log", "format.log", "server.properties",
                    "implicit-format.log", "legacy-feature-format.log", "disabled-restart.log", "disabled.properties",
                    "shutdown-threads.log",
                    "foreign-scope.log", "foreign-scope.properties", "foreign-object.log", "foreign-object.properties",
                    "unauthorized-vault.log", "unauthorized-vault.properties")) {
                var file = source.resolve(name);
                if (Files.isRegularFile(file)) Files.copy(file, node.resolve(name));
            }
        }
    }

    public static int[] allocatePorts() throws Exception {
        var loopback = InetAddress.getByName("127.0.0.1");
        try (var first = new ServerSocket(0, 1, loopback);
                var next = new ServerSocket(0, 1, loopback);
                var controller = new ServerSocket(0, 1, loopback);
                var secondController = new ServerSocket(0, 1, loopback);
                var thirdController = new ServerSocket(0, 1, loopback)) {
            return new int[]{first.getLocalPort(), next.getLocalPort(), controller.getLocalPort(),
                secondController.getLocalPort(), thirdController.getLocalPort()};
        }
    }

    public static void waitForBroker(Admin admin, int id) throws Exception {
        TestUtils.waitForCondition(() -> admin.describeCluster().nodes().get(5, TimeUnit.SECONDS).stream()
                .anyMatch(node -> node.id() == id), "Broker did not register: " + id);
    }

    private static void awaitControllerFailover(Admin admin, int previousLeader) throws Exception {
        TestUtils.waitForCondition(() -> {
            try {
                int leader = admin.describeMetadataQuorum().quorumInfo().get(5, TimeUnit.SECONDS).leaderId();
                return leader >= 0 && leader != previousLeader;
            } catch (ExecutionException | TimeoutException unavailable) {
                return false;
            }
        }, "Controller quorum did not elect a new Leader");
    }

    private static Uuid topicId(Admin admin, String topic) throws Exception {
        return admin.describeTopics(List.of(topic)).allTopicNames().get(30, TimeUnit.SECONDS).get(topic).topicId();
    }

    public static void assertRejectedTopic(Admin admin, NewTopic topic, Errors expected) {
        var failure = assertThrows(ExecutionException.class,
                () -> admin.createTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS));
        assertEquals(expected, Errors.forException(failure.getCause()));
    }

    private static void assertRejectsUnadmittedProfiles(Admin admin) {
        for (String profile : List.of("BOOKKEEPER_WAL_ASYNC_OBJECT", "OBJECT_WAL")) {
            String name = "unadmitted-" + profile;
            var topic = new NewTopic(name, Map.of(0, List.of(0)))
                    .configs(Map.of("nereus.storage.profile", profile));
            var invalid = assertThrows(ExecutionException.class,
                    () -> admin.createTopics(List.of(topic), new CreateTopicsOptions().validateOnly(true))
                            .all().get(30, TimeUnit.SECONDS));
            assertEquals(Errors.INVALID_CONFIG, Errors.forException(invalid.getCause()));
            assertRejectedTopic(admin, topic, Errors.INVALID_CONFIG);
            var absent = assertThrows(ExecutionException.class, () -> topicId(admin, name));
            assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.forException(absent.getCause()));
        }
    }

    public static Properties client(int port) {
        var props = new Properties();
        props.setProperty("bootstrap.servers", "127.0.0.1:" + port);
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());
        props.setProperty("enable.idempotence", "true");
        props.setProperty("acks", "all");
        return props;
    }

    private static long append(Properties client, String value) throws Exception {
        try (var producer = new KafkaProducer<String, String>(client)) {
            return producer.send(new ProducerRecord<>(TOPIC, 0, "key", value)).get(30, TimeUnit.SECONDS).offset();
        }
    }

    public static final class NativeNode implements AutoCloseable {
        private final Path directory;
        private final Path config;
        private Process process;

        public NativeNode(Path work, Map<String, Object> common, int id, int port, String voters, boolean controller)
                throws Exception {
            directory = Files.createDirectory(work.resolve("node-" + id));
            config = directory.resolve("server.properties");
            var props = new Properties();
            props.putAll(common);
            props.setProperty("node.id", Integer.toString(id));
            props.setProperty("process.roles", controller ? "controller" : "broker");
            props.setProperty("listeners", (controller ? "CONTROLLER" : "PLAINTEXT") + "://127.0.0.1:" + port);
            props.setProperty("controller.listener.names", "CONTROLLER");
            props.setProperty("inter.broker.listener.name", "PLAINTEXT");
            props.setProperty("listener.security.protocol.map", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT");
            props.setProperty("controller.quorum.voters", voters);
            props.setProperty("log.dirs", directory.resolve("logs").toString());
            props.setProperty("nereus.kafka.storage.cache.dir", directory.resolve("cache").toString());
            if (controller) {
                props.setProperty("metadata.log.max.record.bytes.between.snapshots", "1");
            }
            try (var output = Files.newOutputStream(config)) {
                props.store(output, null);
            }
        }

        private Process launch(String log, String entryPoint, String... args) throws Exception {
            var command = new ArrayList<>(List.of(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-Dlog4j2.configurationFile=" + System.getProperty("nereus.nsip.processLogConfig"),
                    "-Dkafka.logs.dir=" + directory,
                    "-cp", System.getProperty("nereus.nsip.processClasspath"), entryPoint));
            command.addAll(List.of(args));
            Process launched = new ProcessBuilder(command)
                    .directory(directory.toFile())
                    .redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.appendTo(directory.resolve(log).toFile()))
                    .start();
            System.out.printf("%s: PID %d, %s %s%n", directory.getFileName(), launched.pid(), entryPoint, List.of(args));
            return launched;
        }

        public void format(String clusterId) throws Exception {
            assertExit(0, "format.log", "kafka.tools.StorageTool", "format", "--config",
                    config.toString(), "--cluster-id", clusterId, "--feature", "nereus.storage.version=2");
        }

        void assertRejectsImplicitFormat(String clusterId) throws Exception {
            String log = "implicit-format.log";
            assertExit(1, log, "kafka.tools.StorageTool", "format", "--config", config.toString(),
                    "--cluster-id", clusterId);
            assertTrue(Files.readString(directory.resolve(log)).contains("requires explicit nereus.storage.version=2"));
        }

        void assertRejectsLegacyFeatureFormat(String clusterId) throws Exception {
            String log = "legacy-feature-format.log";
            assertExit(1, log, "kafka.tools.StorageTool", "format", "--config", config.toString(),
                    "--cluster-id", clusterId, "--feature", "nereus.storage.version=1");
            assertTrue(Files.readString(directory.resolve(log)).contains(
                    "nereus.storage.version only supports fresh V2 feature level 2; configured level was 1"));
            assertFalse(Files.exists(directory.resolve("logs/meta.properties")));
        }

        void assertRejectsDisabledRestart() throws Exception {
            Path disabled = withProperty("disabled.properties", "nereus.kafka.storage.enabled", "false");
            String log = "disabled-restart.log";
            assertExit(1, log, "kafka.server.nereus.NereusKafka", disabled.toString());
            assertTrue(Files.readString(directory.resolve(log)).contains("Expected persisted nereus.storage.version=0"));
        }

        void assertRejectsForeignProviderScope() throws Exception {
            Path foreign = withProperty("foreign-scope.properties", "nereus.kafka.storage.bookkeeper.provider.scope.sha256",
                    "0000000000000000000000000000000000000000000000000000000000000002");
            String log = "foreign-scope.log";
            assertExit(1, log, "kafka.server.nereus.NereusKafka", foreign.toString());
            assertTrue(Files.readString(directory.resolve(log)).contains("native BK scope or quorum differs from the admitted capability"));
        }

        public void assertRejectsForeignObjectBucket(String bucket) throws Exception {
            Path foreign = withProperty("foreign-object.properties", "nereus.kafka.storage.object.bucket", bucket);
            String log = "foreign-object.log";
            assertExit(1, log, "kafka.server.nereus.NereusKafka", foreign.toString());
            assertTrue(Files.readString(directory.resolve(log)).contains(
                    "native Kafka Object capability belongs to another Cell or provider configuration"));
        }

        public void assertRejectsUnqualifiedObjectWalToken(Path token) throws Exception {
            Path rejected = withProperty("unauthorized-vault.properties",
                    NereusKafkaConfigs.OBJECT_WAL_VAULT_TOKEN_FILE_CONFIG, token.toString());
            String log = "unauthorized-vault.log";
            assertExit(1, log, "kafka.server.nereus.NereusKafka", rejected.toString());
            assertTrue(Files.readString(directory.resolve(log)).contains("Vault Transit rejected authorization"));
        }

        private Path withProperty(String file, String key, String value) throws Exception {
            var props = new Properties();
            try (var input = Files.newInputStream(config)) {
                props.load(input);
            }
            props.setProperty(key, value);
            Path alternative = directory.resolve(file);
            try (var output = Files.newOutputStream(alternative)) {
                props.store(output, null);
            }
            return alternative;
        }

        private void assertExit(int exit, String log, String entryPoint, String... args) throws Exception {
            Process child = launch(log, entryPoint, args);
            try {
                assertTrue(child.waitFor(30, TimeUnit.SECONDS), "Node did not exit: " + directory.resolve(log));
                assertEquals(exit, child.exitValue(), () -> "Unexpected exit: " + directory.resolve(log));
            } finally {
                if (child.isAlive()) {
                    child.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
                }
            }
        }

        public void start() throws Exception {
            process = launch("launcher.log", "kafka.server.nereus.NereusKafka", config.toString());
        }

        boolean hasSnapshotAtOrAfter(long floor) throws Exception {
            Path metadata = directory.resolve("logs/__cluster_metadata-0");
            if (!Files.isDirectory(metadata)) {
                return false;
            }
            try (var files = Files.list(metadata)) {
                return files.map(Snapshots::parse).flatMap(Optional::stream)
                        .filter(snapshot -> !snapshot.partial() && !snapshot.deleted())
                        .map(SnapshotPath::snapshotId)
                        .anyMatch(snapshot -> snapshot.offset() >= floor);
            }
        }

        public void stop() throws Exception {
            assertTrue(process != null && process.isAlive(), "Node exited before shutdown: " + config);
            process.destroy();
            if (!process.waitFor(30, TimeUnit.SECONDS)) {
                var dumpPath = directory.resolve("shutdown-threads.log");
                var dump = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "jcmd").toString(),
                        Long.toString(process.pid()), "Thread.print", "-l")
                        .redirectErrorStream(true)
                        .redirectOutput(dumpPath.toFile())
                        .start();
                if (!dump.waitFor(5, TimeUnit.SECONDS)) {
                    dump.destroyForcibly();
                }
                fail("Node did not exit: " + config + "; thread dump: " + dumpPath);
            }
        }

        @Override
        public void close() throws Exception {
            if (process != null && process.isAlive()) {
                process.destroy();
                if (!process.waitFor(10, TimeUnit.SECONDS)) {
                    assertTrue(process.destroyForcibly().waitFor(10, TimeUnit.SECONDS), "Node leaked: " + config);
                }
            }
        }
    }
}
