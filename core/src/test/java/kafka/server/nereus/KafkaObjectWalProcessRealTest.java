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
import kafka.server.nsip.KafkaBookKeeperProcessRealTest;
import kafka.server.nsip.KafkaBookKeeperProcessRealTest.NativeNode;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.server.config.NereusKafkaConfigs;
import org.apache.kafka.test.TestUtils;

import com.nereusstream.domain.codec.DeterministicTopicIdsV1;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectAuthorizationV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.metadata.oxia.v2.objectwal.OxiaCanonicalControlMetadataStore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Runs the actual Kafka entry point and cold partition handoff over all four real storage services. */
public class KafkaObjectWalProcessRealTest {
    static final TopicPartition OBJECT = new TopicPartition("native-cold", 0);
    private static final TopicPartition BK = new TopicPartition("native-bk-contrast", 0);
    private static final TopicPartition OFFSETS = new TopicPartition("__consumer_offsets", 0);
    private static final TopicPartition TRANSACTIONS = new TopicPartition("__transaction_state", 0);
    private static final String GROUP = "object-wal-cold-offsets";
    private static final String TXN_GROUP = "object-wal-transactional-offsets";

    @Test
    @Timeout(240)
    public void productionObjectWalPublishesAndRecoversOnColdBroker(@TempDir Path work) throws Exception {
        var endpoint = URI.create(System.getProperty("nereus.nsip.s3Endpoint"));
        var oxia = System.getProperty("nereus.nsip.oxia");
        try (var minio = KafkaObjectProviderAdmissionRealTest.minio(endpoint);
                 var control = OxiaClientBuilder.create(oxia).asyncClient().get(30, TimeUnit.SECONDS)) {
            var fixture = KafkaObjectWalAdapterRealTest.provision(work, minio, control, KafkaObjectFaultRelay.endpoint(),
                    System.getProperty("nereus.nsip.bkUri"), oxia);
            run(work, fixture, control, minio, KafkaObjectProviderAdmissionRealTest.newVersionedBucket(minio));
        } finally {
            KafkaBookKeeperProcessRealTest.copyProcessLogs(work);
        }
    }

    private static void run(Path work, KafkaObjectWalAdapterRealTest.Provisioned fixture, AsyncOxiaClient control,
            S3Client minio, String foreignBucket) throws Exception {
        var settings = fixture.settings();
        int[] ports = KafkaBookKeeperProcessRealTest.allocatePorts();
        String voters = "2@127.0.0.1:" + ports[2];
        String cluster = Uuid.randomUuid().toString();
        try (var controller = new NativeNode(work, settings, 2, ports[2], voters, true);
                 var original = new NativeNode(work, settings, 0, ports[0], voters, false);
                 var cold = new NativeNode(work, settings, 1, ports[1], voters, false)) {
            for (var node : List.of(controller, original, cold)) node.format(cluster);
            controller.start();
            cold.assertRejectsForeignObjectBucket(foreignBucket);
            var rejectedToken = Files.writeString(work.resolve("unauthorized-vault-token"), "unissued-token");
            cold.assertRejectsUnqualifiedObjectWalToken(rejectedToken);
            original.start();
            var client = KafkaBookKeeperProcessRealTest.client(ports[0]);
            var producerClient = new Properties();
            producerClient.putAll(client);
            producerClient.put("bootstrap.servers", "127.0.0.1:" + ports[0] + ",127.0.0.1:" + ports[1]);
            producerClient.put("transactional.id", "object-wal-native-transaction");
            var txnConsumerClient = consumerProperties(client);
            txnConsumerClient.put("group.id", TXN_GROUP);
            var after = KafkaBookKeeperProcessRealTest.client(ports[1]);
            Uuid topicId;
            int firstKafkaEpoch;
            try (var admin = Admin.create(client);
                     var producer = new KafkaProducer<String, String>(producerClient);
                     var txnGroup = new KafkaConsumer<String, String>(txnConsumerClient)) {
                KafkaBookKeeperProcessRealTest.waitForBroker(admin, 0);
                KafkaBookKeeperProcessRealTest.assertRejectedTopic(admin,
                        new NewTopic("__share_group_state", Map.of(0, List.of(0))), Errors.INVALID_CONFIG);
                var created = admin.createTopics(List.of(topic(OBJECT, "OBJECT_WAL"), topic(BK, "BOOKKEEPER_WAL_ONLY")));
                created.all().get(30, TimeUnit.SECONDS);
                topicId = created.topicId(OBJECT.topic()).get(30, TimeUnit.SECONDS);
                appendWithLostPut(client, admin, minio, (String) settings.get(NereusKafkaConfigs.OBJECT_BUCKET_CONFIG),
                        fixture, control, topicId);
                firstKafkaEpoch = KafkaObjectWalAdapterRealTest.NativeTerminal.capture(
                        fixture, control, topicId, OBJECT.topic(), 0).kafkaLeaderEpoch();
                var replay = KafkaObjectProtocolRealScenario.recoverLostProduceResponse(ports[0], admin);
                producer.initTransactions();
                producer.beginTransaction();
                assertEquals(2, producer.send(new ProducerRecord<>(OBJECT.topic(), 0, "key", "committed"))
                        .get(30, TimeUnit.SECONDS).offset());
                producer.sendOffsetsToTransaction(Map.of(OBJECT, new OffsetAndMetadata(4)), txnGroup.groupMetadata());
                KafkaObjectProtocolRealScenario.commitAndWake(ports[0], topicId, producer);
                producer.beginTransaction();
                assertEquals(4, producer.send(new ProducerRecord<>(OBJECT.topic(), 0, "key", "aborted"))
                        .get(30, TimeUnit.SECONDS).offset());
                producer.sendOffsetsToTransaction(Map.of(OBJECT, new OffsetAndMetadata(5)), txnGroup.groupMetadata());
                assertEquals(List.of("before-0", "before-1", "committed"),
                        KafkaBookKeeperOwnerRelocationRealTest.consume(client, 3));
                assertEquals(List.of("before-0", "before-1", "committed", "aborted"), readUncommitted(client, OBJECT, 4));
                assertOffsets(admin, 5, 4);
                storeGroupOffset(client);
                cold.start();
                KafkaBookKeeperProcessRealTest.waitForBroker(admin, 1);
                var destination = Optional.of(new NewPartitionReassignment(List.of(1)));
                admin.alterPartitionReassignments(Map.of(OBJECT, destination, BK, destination,
                                OFFSETS, destination, TRANSACTIONS, destination))
                        .all().get(30, TimeUnit.SECONDS);
                assertEquals(1, KafkaBookKeeperOwnerRelocationRealTest.awaitLeader(admin, 0));
                awaitBookKeeperOwners(admin);
                original.stop();
                assertEquals(List.of("before-0", "before-1", "committed"),
                        KafkaBookKeeperOwnerRelocationRealTest.consume(after, 3));
                assertEquals(List.of("before-0", "before-1", "committed", "aborted"), readUncommitted(after, OBJECT, 4));
                assertEquals(List.of("bk-control"), readUncommitted(after, BK, 1));
                assertGroupOffset(after, GROUP, 5);
                try (var coldAdmin = Admin.create(after)) {
                    // An ordinary consumer waits for stable offsets while the second transaction is open.
                    assertEquals(4, coldAdmin.listConsumerGroupOffsets(TXN_GROUP)
                            .partitionsToOffsetAndMetadata().get(30, TimeUnit.SECONDS).get(OBJECT).offset());
                    assertOffsets(coldAdmin, 5, 4);
                    KafkaObjectProtocolRealScenario.assertProduceOffset(ports[1], replay, 1);
                    producer.abortTransaction();
                    awaitTransactionMarker(coldAdmin, 6);
                    assertOffsets(coldAdmin, 6, 6);
                    assertGroupOffset(after, TXN_GROUP, 4);
                    producer.beginTransaction();
                    assertEquals(6, producer.send(new ProducerRecord<>(OBJECT.topic(), 0, "key", "after"))
                            .get(30, TimeUnit.SECONDS).offset());
                    producer.sendOffsetsToTransaction(Map.of(OBJECT, new OffsetAndMetadata(8)), txnGroup.groupMetadata());
                    producer.commitTransaction();
                    awaitTransactionMarker(coldAdmin, 8);
                    assertOffsets(coldAdmin, 8, 8);
                    assertGroupOffset(after, TXN_GROUP, 8);
                }
            }
            assertEquals(List.of("before-0", "before-1", "committed", "after"),
                    KafkaBookKeeperOwnerRelocationRealTest.consume(after, 4));
            var terminal = KafkaObjectWalAdapterRealTest.NativeTerminal.capture(fixture, control, topicId, OBJECT.topic(), 1);
            original.start();
            try (var finalAdmin = Admin.create(client)) {
                KafkaBookKeeperProcessRealTest.waitForBroker(finalAdmin, 0);
                var bucket = (String) settings.get(NereusKafkaConfigs.OBJECT_BUCKET_CONFIG);
                var lostCheckpoint = KafkaObjectRunRolloverRealScenario.stopWithLostCheckpoint(cold, bucket, terminal);
                String checkpoint = terminal.requireSealedByOriginalWriter(minio, bucket, 8);
                assertEquals("/" + bucket + "/" + checkpoint, lostCheckpoint.path());
                var versions = minio.listObjectVersions(request -> request.bucket(bucket).prefix(checkpoint)).versions();
                assertEquals(1, versions.size());
                assertEquals(lostCheckpoint.version(), versions.get(0).versionId());
                System.out.printf("Native terminal Kafka checkpoint recovered the original version after %d PUT attempt(s)%n",
                        lostCheckpoint.puts());
                assertEquals(0, KafkaBookKeeperOwnerRelocationRealTest.awaitLeader(finalAdmin, 1));
                assertEquals(List.of("before-0", "before-1", "committed", "after"),
                        KafkaBookKeeperOwnerRelocationRealTest.consume(client, 4));
                assertGroupOffset(client, TXN_GROUP, 8);
                append(client, OBJECT, 8, "after-terminal");
                assertOffsets(finalAdmin, 9, 9);
                var oldRun = KafkaObjectWalAdapterRealTest.NativeTerminal.capture(fixture, control, topicId, OBJECT.topic(), 0);
                long boundary = Math.addExact(8, oldRun.root().bounds().maxExtentCount());
                KafkaObjectRunRolloverRealScenario.fillAndReadBeforeRollover(client, ports[0], boundary, fixture,
                        control, topicId);
                var rolled = KafkaObjectWalAdapterRealTest.NativeTerminal.capture(fixture, control, topicId, OBJECT.topic(), 0);
                assertEquals(oldRun.writerEpoch(), rolled.writerEpoch());
                assertEquals(oldRun.kafkaLeaderEpoch(), rolled.kafkaLeaderEpoch());
                assertEquals(oldRun.ref().shardRunEpoch() + 1, rolled.ref().shardRunEpoch());
                assertEquals(oldRun.ref(), rolled.root().predecessor().orElseThrow().root());
                oldRun.requireSealedByOriginalWriter(minio, (String) settings.get(NereusKafkaConfigs.OBJECT_BUCKET_CONFIG), boundary);
                assertOffsets(finalAdmin, boundary + 1, boundary + 1);
                KafkaObjectProtocolRealScenario.assertRecoveredEpochs(
                        ports[0], firstKafkaEpoch, terminal.kafkaLeaderEpoch(), rolled.kafkaLeaderEpoch(), boundary + 1);
                assertEquals(List.of("run-" + (boundary - 1), "run-" + boundary),
                        readFrom(client, OBJECT, boundary - 1, 2));
                assertEquals(List.of("before-0", "before-1", "committed", "aborted", "after", "after-terminal"),
                        readUncommitted(client, OBJECT, 6));
                verifyWireExits(client, finalAdmin, ports[0], topicId, boundary);
            }
        }
    }

    private static void verifyWireExits(Properties client, Admin finalAdmin, int port, Uuid topicId,
            long boundary) throws Exception {
        KafkaObjectProtocolRealScenario.assertAckZero(port, finalAdmin, boundary + 2);
        KafkaObjectProtocolRealScenario.verifyNonIdempotentRetry(port, finalAdmin, boundary + 2);
        assertEquals(List.of("non-idempotent-retry", "non-idempotent-retry"),
                readFrom(client, OBJECT, boundary + 2, 2));
        KafkaObjectFetchExitRealScenario.verify(port, topicId, client, finalAdmin, boundary + 4);
    }

    static NewTopic topic(TopicPartition partition, String profile) {
        return new NewTopic(partition.topic(), Map.of(partition.partition(), List.of(0)))
                .configs(Map.of("nereus.storage.profile", profile));
    }

    private static void append(Properties client, TopicPartition partition, long expected, String value) throws Exception {
        try (var producer = new KafkaProducer<String, String>(client)) {
            assertEquals(expected, producer.send(new ProducerRecord<>(partition.topic(), partition.partition(), "key", value))
                    .get(30, TimeUnit.SECONDS).offset());
        }
    }

    private static void appendWithLostPut(Properties client, Admin admin, S3Client minio, String bucket,
            KafkaObjectWalAdapterRealTest.Provisioned fixture, AsyncOxiaClient control, Uuid topicId) throws Exception {
        String prefix = KafkaObjectProviderAdmissionRealTest.PREFIX + "/object-wal/";
        awaitInitialOwner(admin);
        var ackOne = new Properties();
        ackOne.putAll(client);
        ackOne.setProperty("acks", "1");
        ackOne.setProperty("enable.idempotence", "false");
        try (var relay = KafkaObjectFaultRelay.lostPut(bucket, prefix);
                 var producer = new KafkaProducer<String, String>(ackOne)) {
            relay.arm("first-put-response-lost");
            var produce = producer.send(new ProducerRecord<>(OBJECT.topic(), 0, "key", "before-0"));
            var persistedVersion = relay.awaitAppliedPut();
            var held = relay.originalPutTrace();
            assertEquals(1, held.puts());
            assertEquals(0, held.fullGets());
            assertTrue(held.path().startsWith("/" + bucket + "/" + prefix));
            String key = held.path().substring(bucket.length() + 2);
            int shard = Integer.parseInt(key.substring(prefix.length()).split("/", 2)[0]);
            var authority = authorization(new OxiaCanonicalControlMetadataStore(control,
                    "/nereus-kafka-object-wal/" + fixture.profile().protocolCellSha256().toHex(), shard), shard);
            var scope = scope(fixture, topicId);
            var before = authority.read(scope).orElseThrow();
            assertEquals(0, before.endOffset());
            assertTrue(before.grants().isEmpty());
            append(client, BK, 0, "bk-control");
            assertFalse(produce.isDone(), "an Object WAL write acknowledged before it could prove the exact PUT");
            relay.losePutResponses();
            assertEquals(0, produce.get(30, TimeUnit.SECONDS).offset());
            var published = authority.read(scope).orElseThrow();
            assertEquals(1, published.endOffset());
            assertEquals(before.fence(), published.fence());
            assertEquals(1, published.grants().size());
            assertEquals(key, published.grants().get(0).object().key());
            assertEquals(0, published.grants().get(0).locator().startOffset());
            assertEquals(1, published.grants().get(0).locator().endOffsetExclusive());
            var reconciled = relay.originalPutTrace();
            assertEquals(held.path(), reconciled.path());
            assertEquals(persistedVersion, reconciled.version());
            assertTrue(reconciled.lists() > held.lists() && reconciled.fullGets() > held.fullGets(),
                    "The client ACK did not include actual provider reconciliation");
            System.out.printf("Original Object ACK put=%d fullGet=%d list=%d committedGrants=%d%n",
                    reconciled.puts(), reconciled.fullGets(), reconciled.lists(), published.grants().size());
            assertOffsets(admin, 1, 1);
            var versions = minio.listObjectVersions(request -> request.bucket(bucket).prefix(prefix)).versions();
            assertEquals(1, versions.size());
            assertEquals(persistedVersion, versions.get(0).versionId());
        }
    }

    static KafkaObjectAuthorizationV1 authorization(OxiaCanonicalControlMetadataStore metadata, int shard) {
        return new KafkaObjectAuthorizationV1(metadata, shard,
                new KafkaObjectAuthorizationV1.Bounds(2048, 1024, 1024L * 64 * 1024 * 1024, 1024));
    }

    static KafkaPartitionFenceV1 scope(KafkaObjectWalAdapterRealTest.Provisioned fixture, Uuid topicId) {
        var incarnation = new KafkaTopicIncarnationIdentity(new KafkaTopicId(new Id128(
                topicId.getMostSignificantBits(), topicId.getLeastSignificantBits())), new KafkaTopicName(OBJECT.topic()));
        var binding = DeterministicTopicIdsV1.deriveBindingId(fixture.profile().protocolCell(), incarnation);
        return new KafkaPartitionFenceV1(binding, incarnation, OBJECT.partition(), 1,
                DeterministicTopicIdsV1.deriveStorageEpochId(binding, 0), 1, 0);
    }

    static void assertOffsets(Admin admin, long end, long stable) throws Exception {
        assertEquals(0, admin.listOffsets(Map.of(OBJECT, OffsetSpec.earliest()))
                .partitionResult(OBJECT).get(30, TimeUnit.SECONDS).offset());
        assertEquals(end, admin.listOffsets(Map.of(OBJECT, OffsetSpec.latest()))
                .partitionResult(OBJECT).get(30, TimeUnit.SECONDS).offset());
        assertEquals(stable, admin.listOffsets(Map.of(OBJECT, OffsetSpec.latest()),
                        new ListOffsetsOptions(IsolationLevel.READ_COMMITTED))
                .partitionResult(OBJECT).get(30, TimeUnit.SECONDS).offset());
    }

    static void awaitInitialOwner(Admin admin) throws Exception {
        awaitInitialOwner(admin, OBJECT);
        assertOffsets(admin, 0, 0);
    }

    static void awaitInitialOwner(Admin admin, TopicPartition partition) throws Exception {
        TestUtils.waitForCondition(() -> {
            try {
                long end = admin.listOffsets(Map.of(partition, OffsetSpec.latest())).partitionResult(partition)
                        .get(5, TimeUnit.SECONDS).offset();
                assertEquals(0, end, "The newly opened Object WAL partition has unexpected data");
                return true;
            } catch (ExecutionException unavailable) {
                if (unavailable.getCause() instanceof RetriableException) return false;
                throw unavailable;
            }
        }, 30_000, "The native Broker did not activate the initial Object WAL Owner for " + partition);
    }

    private static void awaitTransactionMarker(Admin admin, long end) throws Exception {
        TestUtils.waitForCondition(() -> admin.listOffsets(Map.of(OBJECT, OffsetSpec.latest()),
                        new ListOffsetsOptions(IsolationLevel.READ_COMMITTED))
                .partitionResult(OBJECT).get(5, TimeUnit.SECONDS).offset() == end,
                30_000, "The cold Broker did not publish the native transaction marker");
    }

    private static Properties consumerProperties(Properties client) {
        var configuration = new Properties();
        configuration.putAll(client);
        configuration.put("key.deserializer", StringDeserializer.class.getName());
        configuration.put("value.deserializer", StringDeserializer.class.getName());
        configuration.put("enable.auto.commit", "false");
        configuration.put("group.id", GROUP);
        return configuration;
    }

    static List<String> readUncommitted(Properties client, TopicPartition partition, int count) {
        return readFrom(client, partition, 0, count);
    }

    private static List<String> readFrom(Properties client, TopicPartition partition, long offset, int count) {
        var configuration = consumerProperties(client);
        configuration.put("isolation.level", "read_uncommitted");
        try (var consumer = new KafkaConsumer<String, String>(configuration)) {
            consumer.assign(List.of(partition));
            consumer.seek(partition, offset);
            var values = new ArrayList<String>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (values.size() < count && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    values.add(record.value());
                    if (values.size() == count) return values;
                }
            }
            return values;
        }
    }

    private static void storeGroupOffset(Properties client) {
        try (var consumer = new KafkaConsumer<String, String>(consumerProperties(client))) {
            consumer.assign(List.of(OBJECT));
            consumer.commitSync(Map.of(OBJECT, new OffsetAndMetadata(5)), Duration.ofSeconds(30));
        }
    }

    private static void assertGroupOffset(Properties client, String group, long expected) {
        var config = consumerProperties(client);
        config.put("group.id", group);
        try (var consumer = new KafkaConsumer<String, String>(config)) {
            assertEquals(expected, consumer.committed(Set.of(OBJECT), Duration.ofSeconds(30)).get(OBJECT).offset());
        }
    }

    private static void awaitBookKeeperOwners(Admin admin) throws Exception {
        TestUtils.waitForCondition(() -> {
            var described = admin.describeTopics(List.of(BK.topic(), OFFSETS.topic(), TRANSACTIONS.topic()))
                    .allTopicNames().get(5, TimeUnit.SECONDS);
            for (var topic : described.values()) {
                var assignment = topic.partitions().get(0);
                assertEquals(1, assignment.replicas().size());
                assertEquals(1, assignment.isr().size());
                if (assignment.leader() == null || assignment.leader().id() != 1) return false;
            }
            return true;
        }, 30_000, "Kafka BK topics did not move to the native BK owner");
    }
}
