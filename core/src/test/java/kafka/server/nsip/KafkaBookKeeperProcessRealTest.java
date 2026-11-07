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
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.StringSerializer;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        String bkUri = System.getProperty("nereus.nsip.bkUri");
        String oxia = System.getProperty("nereus.nsip.oxia");
        KafkaBookKeeperOwnerRelocationRealTest.writeCapability(work, Optional.empty());
        var common = KafkaBookKeeperOwnerRelocationRealTest.nativeConfig(work, bkUri, oxia);
        int[] ports = allocatePorts();
        String clusterId = Uuid.randomUuid().toString();
        var controller = new NativeNode(work, common, 2, ports[2], ports[2], true);
        var first = new NativeNode(work, common, 0, ports[0], ports[2], false);
        var next = new NativeNode(work, common, 1, ports[1], ports[2], false);
        System.out.println("Native process logs: " + work);
        try (controller; first; next) {
            controller.assertRejectsImplicitFormat(clusterId);
            controller.format(clusterId);
            first.format(clusterId);
            next.format(clusterId);
            controller.start();
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
                long floor = admin.describeMetadataQuorum().quorumInfo().get(30, TimeUnit.SECONDS).highWatermark();
                assertTrue(floor > 0);
                TestUtils.waitForCondition(() -> controller.hasSnapshotAtOrAfter(floor),
                        "Controller did not persist a snapshot containing the original topic");
                controller.stop();
                controller.assertRejectsDisabledRestart();
                controller.start();
                assertEquals(topicId, topicId(admin));
                admin.createTopics(List.of(new NewTopic(CONTROLLER_RESTART_TOPIC, Map.of(0, List.of(0)))))
                        .all().get(30, TimeUnit.SECONDS);
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
                    assertEquals(topicId, topicId(relocatedAdmin));
                }
            }
        }
    }

    private static int[] allocatePorts() throws Exception {
        var loopback = InetAddress.getByName("127.0.0.1");
        try (var first = new ServerSocket(0, 1, loopback);
                var next = new ServerSocket(0, 1, loopback);
                var controller = new ServerSocket(0, 1, loopback)) {
            return new int[]{first.getLocalPort(), next.getLocalPort(), controller.getLocalPort()};
        }
    }

    private static void waitForBroker(Admin admin, int id) throws Exception {
        TestUtils.waitForCondition(() -> admin.describeCluster().nodes().get(5, TimeUnit.SECONDS).stream()
                .anyMatch(node -> node.id() == id), "Broker did not register: " + id);
    }

    private static Uuid topicId(Admin admin) throws Exception {
        return admin.describeTopics(List.of(TOPIC)).allTopicNames().get(30, TimeUnit.SECONDS).get(TOPIC).topicId();
    }

    private static void assertRejectedTopic(Admin admin, NewTopic topic, Errors expected) {
        var failure = assertThrows(ExecutionException.class,
                () -> admin.createTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS));
        assertEquals(expected, Errors.forException(failure.getCause()));
    }

    private static Properties client(int port) {
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

    private static final class NativeNode implements AutoCloseable {
        private final Path directory;
        private final Path config;
        private Process process;

        NativeNode(Path work, Map<String, Object> common, int id, int port, int controllerPort, boolean controller)
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
            props.setProperty("controller.quorum.voters", "2@127.0.0.1:" + controllerPort);
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
            return new ProcessBuilder(command)
                    .directory(directory.toFile())
                    .redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.appendTo(directory.resolve(log).toFile()))
                    .start();
        }

        void format(String clusterId) throws Exception {
            assertExit(0, "format.log", "kafka.tools.StorageTool", "format", "--config",
                    config.toString(), "--cluster-id", clusterId, "--feature", "nereus.storage.version=2");
        }

        void assertRejectsImplicitFormat(String clusterId) throws Exception {
            String log = "implicit-format.log";
            assertExit(1, log, "kafka.tools.StorageTool", "format", "--config", config.toString(),
                    "--cluster-id", clusterId);
            assertTrue(Files.readString(directory.resolve(log)).contains("requires explicit nereus.storage.version=2"));
        }

        void assertRejectsDisabledRestart() throws Exception {
            var props = new Properties();
            try (var input = Files.newInputStream(config)) {
                props.load(input);
            }
            props.setProperty("nereus.kafka.storage.enabled", "false");
            Path disabled = directory.resolve("disabled.properties");
            try (var output = Files.newOutputStream(disabled)) {
                props.store(output, null);
            }
            String log = "disabled-restart.log";
            assertExit(1, log, "kafka.server.nereus.NereusKafka", disabled.toString());
            assertTrue(Files.readString(directory.resolve(log)).contains("Expected persisted nereus.storage.version=0"));
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

        void start() throws Exception {
            process = launch("server.log", "kafka.server.nereus.NereusKafka", config.toString());
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

        void stop() throws Exception {
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
