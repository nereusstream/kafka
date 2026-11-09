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

import kafka.server.nereus.KafkaBookKeeperObjectMaterializationRealTest.Fixture;
import kafka.server.nereus.KafkaBookKeeperObjectMaterializationRealTest.ScheduledLimits;
import kafka.server.nsip.KafkaBookKeeperOwnerRelocationRealTest;
import kafka.server.nsip.KafkaBookKeeperProcessRealTest;
import kafka.server.nsip.KafkaBookKeeperProcessRealTest.NativeNode;
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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.metadata.nereus.KafkaTopicBindingAggregateMapperV1;
import org.apache.kafka.test.TestUtils;

import com.nereusstream.domain.aggregate.ProfileOriginV1;
import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaKafkaBookKeeperM4ReadRouteV2;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static kafka.server.nereus.KafkaBookKeeperAckRealTest.assertFetchedValues;
import static kafka.server.nereus.KafkaBookKeeperAckRealTest.delayedFetch;

public class KafkaAsyncObjectProcessRealTest {
    private static final TopicPartition ASYNC = new TopicPartition("native-cold", 0);
    private static final TopicPartition BK = new TopicPartition("native-bk-contrast", 0);
    private static final int SEALED_INPUT_END = 17;

    @Test
    @Timeout(240)
    public void keepsBookKeeperAcksDuringObjectOutageAndRecoversSelectedGeneration(@TempDir Path work) throws Exception {
        try {
            KafkaBookKeeperObjectMaterializationRealTest.writeM6Capability(work);
            var common = KafkaBookKeeperOwnerRelocationRealTest.nativeConfig(work,
                    System.getProperty("nereus.nsip.bkUri"), System.getProperty("nereus.nsip.oxia"));
            KafkaBookKeeperObjectMaterializationRealTest.withObjectFixture(work, common, true,
                    fixture -> runProductionCluster(work, fixture));
        } finally {
            KafkaBookKeeperProcessRealTest.copyProcessLogs(work);
        }
    }

    private static void runProductionCluster(Path work, Fixture fixture) throws Exception {
        var config = KafkaBookKeeperM5PolicyRealScenario.nativeSettings(work, fixture.settings(), true, "OBJECT_BYTES_DISABLED");
        config.put("nereus.kafka.storage.bookkeeper.max.bytes.per.ledger", "1048576");
        config.put("message.max.bytes", "4096");
        config.put("nereus.kafka.storage.retention.check.interval.ms", "1000");
        int[] ports = KafkaBookKeeperProcessRealTest.allocatePorts();
        var clusterId = Uuid.randomUuid().toString();
        String voters = "2@127.0.0.1:" + ports[2];
        try (var controller = new NativeNode(work, config, 2, ports[2], voters, true);
                var original = new NativeNode(work, config, 0, ports[0], voters, false);
                var cold = new NativeNode(work, config, 1, ports[1], voters, false)) {
            for (var node : List.of(controller, original, cold)) node.format(clusterId);
            controller.start();
            original.start();
            var client = KafkaBookKeeperProcessRealTest.client(ports[0]);
            try (var admin = Admin.create(client)) {
                KafkaBookKeeperProcessRealTest.waitForBroker(admin, 0);
                var topic = create(admin, ASYNC, StorageProfileV1.BOOKKEEPER_WAL_ASYNC_OBJECT);
                create(admin, BK, StorageProfileV1.BOOKKEEPER_WAL_ONLY);
                var route = route(fixture, topic);
                append(client, ASYNC, 0, SEALED_INPUT_END);
                append(client, BK, 0, 1);
                admitMaterialization(route, fixture);
                var selected = awaitSelection(route);
                verifyOutage(fixture, client);
                var foreignEndpoint = URI.create(System.getProperty("nereus.nsip.s3Endpoint"));
                try (var minio = KafkaObjectProviderAdmissionRealTest.minio(foreignEndpoint)) {
                    cold.assertRejectsForeignObjectBucket(KafkaObjectProviderAdmissionRealTest.newVersionedBucket(minio));
                }
                cold.start();
                KafkaBookKeeperProcessRealTest.waitForBroker(admin, 1);
                var destination = Optional.of(new NewPartitionReassignment(List.of(1)));
                admin.alterPartitionReassignments(Map.of(ASYNC, destination, BK, destination)).all().get(30, TimeUnit.SECONDS);
                assertEquals(1, KafkaBookKeeperOwnerRelocationRealTest.awaitLeader(admin, 0));
                awaitLeader(admin, BK, 1);
                original.stop();
                var after = KafkaBookKeeperProcessRealTest.client(ports[1]);
                verifyColdSelection(fixture, route, selected, after);
                verifyColdProtocol(fixture, after, ports[1], topic);
            }
        }
    }

    private static Uuid create(Admin admin, TopicPartition partition, StorageProfileV1 profile) throws Exception {
        var topic = new NewTopic(partition.topic(), Map.of(partition.partition(), List.of(0)))
                .configs(Map.of("nereus.storage.profile", profile.name()));
        return admin.createTopics(List.of(topic)).topicId(partition.topic()).get(30, TimeUnit.SECONDS);
    }

    private static void awaitLeader(Admin admin, TopicPartition partition, int broker) throws Exception {
        TestUtils.waitForCondition(() -> admin.describeTopics(List.of(partition.topic())).allTopicNames().get(5, TimeUnit.SECONDS)
                        .get(partition.topic()).partitions().get(partition.partition()).leader().id() == broker,
                30_000, "the BK-only comparison partition did not move to the cold Broker");
    }

    private static OxiaKafkaBookKeeperM4ReadRouteV2 route(Fixture fixture, Uuid topic) throws Exception {
        var config = fixture.config();
        var policy = NereusBookKeeperCapability.metadataPolicy(config);
        var aggregate = KafkaTopicBindingAggregateMapperV1.toRecord(KafkaTopicBindingAggregateMapperV1.create(
                topic, ASYNC.topic(), policy, StorageProfileV1.BOOKKEEPER_WAL_ASYNC_OBJECT, ProfileOriginV1.TOPIC_EXPLICIT));
        var scope = NereusBookKeeperCapability.nativeScope(new TopicIdPartition(topic, ASYNC), aggregate,
                NereusBookKeeperCapability.loadAndValidate(config));
        return new OxiaKafkaBookKeeperM4ReadRouteV2(fixture.control(), fixture.namespace().identity(), scope);
    }

    private static void admitMaterialization(OxiaKafkaBookKeeperM4ReadRouteV2 route, Fixture fixture) throws Exception {
        assertTrue(fixture.provider().physicalQuota().quota().expand(64L << 20).toCompletableFuture().get(30, TimeUnit.SECONDS));
        ScheduledLimits.install(route, fixture.profile().protocolCellSha256());
    }

    private static BindingReadSelector awaitSelection(OxiaKafkaBookKeeperM4ReadRouteV2 route) throws Exception {
        TestUtils.waitForCondition(() -> route.readSelector().flatMap(BindingReadSelector::fallbackSetSha256).isPresent(),
                45_000, "production Broker did not select its admitted native Object generation");
        return route.readSelector().orElseThrow();
    }

    private static void verifyOutage(Fixture fixture, Properties client) throws Exception {
        try (var relay = KafkaObjectFaultRelay.wholeBucket(fixture.bucket())) {
            relay.arm("bucket-unavailable");
            append(client, ASYNC, SEALED_INPUT_END, SEALED_INPUT_END + 1);
            append(client, BK, 1, 2);
            assertAsyncValues(client, SEALED_INPUT_END + 1);
            relay.awaitRequest();
        }
    }

    private static void verifyColdSelection(Fixture fixture, OxiaKafkaBookKeeperM4ReadRouteV2 route,
            BindingReadSelector original, Properties client) throws Exception {
        TestUtils.waitForCondition(() -> route.readSelector().filter(selector -> selector.ownerEpoch() > original.ownerEpoch()).isPresent(),
                30_000, "production Broker did not activate the next native Owner");
        var selected = route.readSelector().orElseThrow();
        assertEquals(original.selectedViewSha256(), selected.selectedViewSha256());
        assertEquals(original.fallbackSetSha256(), selected.fallbackSetSha256());
        var loaded = NereusKafkaObjectRecovery.recover(route, ASYNC.partition(), selected, fixture.provider(),
                Duration.ofSeconds(10)).orElseThrow();
        var object = loaded.validated().generation().payloadObjects().get(0);
        var version = new String(object.immutableProviderVersionToken().orElseThrow().toByteArray(), StandardCharsets.UTF_8);
        try (var relay = new KafkaObjectFaultRelay(fixture.bucket(), object.identity().key(), version)) {
            relay.arm("observe");
            assertAsyncValues(client, SEALED_INPUT_END + 1);
            relay.awaitRequest();
        }
        append(client, ASYNC, SEALED_INPUT_END + 1, SEALED_INPUT_END + 2);
        assertAsyncValues(client, SEALED_INPUT_END + 2);
        append(client, BK, 2, 3);
        try (var admin = Admin.create(client)) {
            assertEquals(3, admin.listOffsets(Map.of(BK, OffsetSpec.latest())).partitionResult(BK).get(30, TimeUnit.SECONDS).offset());
        }
    }

    private static void append(Properties client, TopicPartition partition, int from, int to) throws Exception {
        try (var producer = new KafkaProducer<String, String>(client)) {
            for (int offset = from; offset < to; offset++) {
                assertEquals(offset, producer.send(new ProducerRecord<>(partition.topic(), partition.partition(), "key", "input-" + offset))
                        .get(30, TimeUnit.SECONDS).offset());
            }
        }
    }

    private static void verifyColdProtocol(Fixture fixture, Properties client, int port, Uuid topic) throws Exception {
        int end = SEALED_INPUT_END + 2;
        verifyTailWaitsLocally(fixture, client, port, topic, end);
        try (var admin = Admin.create(client)) {
            KafkaObjectProtocolRealScenario.recoverLostProduceResponse(port, admin, end + 1, "original-producer-retry");
            KafkaObjectProtocolRealScenario.verifyNonIdempotentRetry(port, admin, end + 2);
            KafkaBookKeeperProcessRealTest.assertRejectedTopic(admin,
                    new NewTopic("__transaction_state", Map.of(0, List.of(1)))
                            .configs(Map.of("nereus.storage.profile", "BOOKKEEPER_WAL_ONLY")), Errors.INVALID_CONFIG);
            var transactionState = new NewTopic("__transaction_state", Map.of(0, List.of(1)))
                    .configs(Map.of("cleanup.policy", "compact"));
            admin.createTopics(List.of(transactionState)).all().get(30, TimeUnit.SECONDS);
        }
        var transactional = new Properties();
        transactional.putAll(client);
        transactional.put("transactional.id", "m6-async-cold-transaction");
        try (var producer = new KafkaProducer<String, String>(transactional)) {
            producer.initTransactions();
            producer.beginTransaction();
            long offset = end + 4;
            assertEquals(offset, producer.send(new ProducerRecord<>(ASYNC.topic(), 0, "tx", "committed"))
                    .get(30, TimeUnit.SECONDS).offset());
            KafkaObjectProtocolRealScenario.commitAndWake(port, topic, producer, offset);
        }
    }

    private static void verifyTailWaitsLocally(Fixture fixture, Properties client, int port, Uuid topic, int offset)
            throws Exception {
        try (var relay = KafkaObjectFaultRelay.wholeBucket(fixture.bucket())) {
            relay.arm("bucket-unavailable");
            var request = delayedFetch(ASYNC, topic, offset, IsolationLevel.READ_COMMITTED, 15_000);
            try (var wire = new NativeKafkaRequest(port, request)) {
                assertThrows(SocketTimeoutException.class, () -> wire.receive(250));
                assertEquals(0, relay.hits(), "An end-of-log async Fetch polled the Object Provider");
                var acksOne = new Properties();
                acksOne.putAll(client);
                acksOne.put("enable.idempotence", "false");
                acksOne.put("acks", "1");
                append(acksOne, ASYNC, offset, offset + 1);
                var response = ((FetchResponse) wire.receive(5_000))
                        .responseData(Map.of(topic, ASYNC.topic()), (short) 16).get(ASYNC);
                assertEquals(offset + 1, response.highWatermark());
                assertEquals(offset + 1, response.lastStableOffset());
                assertFetchedValues(response, List.of("input-" + offset));
                assertEquals(0, relay.hits(), "A shared BK commit or the resumed Fetch consulted the Object Provider");
            }
        }
    }

    private static void assertAsyncValues(Properties client, int end) {
        var expected = IntStream.range(0, end).mapToObj(offset -> "input-" + offset).toList();
        assertEquals(expected, KafkaBookKeeperOwnerRelocationRealTest.consume(client, end));
    }
}
