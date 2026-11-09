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

import kafka.server.nsip.NativeKafkaRequest;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.server.config.NereusKafkaConfigs;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaKafkaBookKeeperM4ReadRouteV2;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Layout;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.s3.S3M5PhysicalNamespaceAuthorityV2;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import io.oxia.client.api.AsyncOxiaClient;
import software.amazon.awssdk.services.s3.S3Client;

import static kafka.server.nereus.KafkaBookKeeperAckRealTest.assertFetchedValues;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.awaitInitialOwner;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.readCommitted;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.readUncommitted;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.topic;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Uses a small, complete Object WAL key domain sealed by the real Controller handoff. */
final class KafkaObjectNativeCompactionRealScenario {
    static final TopicPartition COMPACT = new TopicPartition("native-object-compacted", 0);
    private static final List<String> ORIGINAL = Arrays.asList("old-zero", "middle", "old-two",
            "new-three", "new-four", "old-five", null, "committed-seven", "aborted-nine");
    private static final List<String> SELECTED = Arrays.asList("middle", "new-three", "new-four", null, "committed-seven");
    private static final int SEALED_END = KafkaObjectCompactionRealScenario.SEALED_TRANSACTION_END;

    record Seed(Uuid topicId, KafkaObjectCompactionRealScenario.ErasedBatch original) { }

    private KafkaObjectNativeCompactionRealScenario() { }

    static Map<String, Object> configureReadQualification(Path work, KafkaObjectWalAdapterRealTest.Provisioned fixture)
            throws Exception {
        var receipt = Path.of(System.getProperty("nereus.nsip.objectWalReadAdmissionFile")).toRealPath();
        var digest = NereusKafkaM4AdmissionV2.sha(Files.readAllBytes(receipt));
        Files.writeString(work.resolve("capability.properties"), "\nobjectWalReadAdmission.receiptFile=" + receipt
                + "\nobjectWalReadAdmission.receiptSha256=" + digest.toHex() + "\n", StandardOpenOption.APPEND);
        var settings = new HashMap<>(fixture.settings());
        settings.put(NereusKafkaConfigs.RETENTION_CHECK_INTERVAL_MS_CONFIG, "1000");
        return settings;
    }

    static void installCapacity(KafkaObjectWalAdapterRealTest.Provisioned fixture, AsyncOxiaClient control, Uuid topicId) {
        var route = readRoute(fixture, control, topicId, KafkaObjectWalProcessRealTest.OBJECT);
        KafkaBookKeeperObjectMaterializationRealTest.ScheduledLimits.install(route, fixture.profile().protocolCellSha256());
    }

    private static OxiaKafkaBookKeeperM4ReadRouteV2 readRoute(KafkaObjectWalAdapterRealTest.Provisioned fixture,
            AsyncOxiaClient control, Uuid topicId, TopicPartition partition) {
        var scope = KafkaObjectWalProcessRealTest.scope(fixture, topicId, partition);
        var binding = new BindingIdentity(scope.bindingId(),
                Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(scope.topicIncarnation())),
                scope.storageEpochId().digest());
        return OxiaKafkaBookKeeperM4ReadRouteV2.forObjectWal(control, fixture.namespace().identity(),
                scope.partitionId(), binding);
    }

    private static QuotaAdmission requireOriginalOutputBackpressure(KafkaObjectWalAdapterRealTest.Provisioned fixture,
            S3Client minio, Properties client) throws Exception {
        var profile = fixture.profile();
        var binding = profile.physicalBinding();
        var bucket = (String) fixture.settings().get(NereusKafkaConfigs.OBJECT_BUCKET_CONFIG);
        var physical = S3M5PhysicalNamespaceAuthorityV2.connect(minio, bucket, binding.physicalNamespace());
        var quota = fixture.namespace().openAuthorityRoute(physical).toCompletableFuture()
                .get(10, TimeUnit.SECONDS).quota();
        var layout = Layout.withPhysicalMembers(binding.authorityRoot(), binding.physicalNamespace());
        long slotCharge = Math.addExact(layout.reservationCharge(), layout.birthRecordCharge());
        int outputs = 1 + IndexKind.values().length;
        waitForCondition(() -> {
            var snapshot = quota.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS);
            return snapshot.head().births().isPresent() && snapshot.availableCanonicalBytes() < slotCharge;
        }, 30_000, "The actual Object WAL output did not reach the original metadata quota");
        var blocked = quota.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertEquals(8L << 20, blocked.head().capacityBytes());
        var births = blocked.head().births().orElseThrow();
        assertEquals(births.issued(), births.unassigned());
        assertTrue(births.unassigned() > 0 && births.unassigned() < outputs);
        var beforeWrite = minio.listObjectVersions(request -> request.bucket(bucket)
                .prefix(profile.exclusiveNamespacePrefix() + "/m5/materialization/"));
        assertTrue(beforeWrite.versions().isEmpty(), "An incomplete Object WAL output reservation wrote data");
        assertEquals(ORIGINAL, readUncommitted(client, COMPACT, ORIGINAL.size()));
        long exactCapacity = Math.addExact(blocked.chargedCanonicalBytes(),
                Math.multiplyExact(outputs - births.unassigned(), slotCharge));
        return new QuotaAdmission(quota, births.unassigned(), outputs, exactCapacity);
    }

    private record QuotaAdmission(M5GcQuotaCoordinatorV2 quota, long reserved, int outputs, long exactCapacity) {
        private void expand() throws Exception {
            assertTrue(quota.expand(exactCapacity).toCompletableFuture().get(5, TimeUnit.SECONDS));
            System.out.printf("Native Object WAL compact output admission reserved %d of %d original slots before explicit quota expansion to %d bytes%n",
                    reserved, outputs, exactCapacity);
        }
    }

    static Seed seed(Admin admin, Properties client, int port) throws Exception {
        var topic = topic(COMPACT, "OBJECT_WAL").configs(Map.of(
                "nereus.storage.profile", "OBJECT_WAL", "cleanup.policy", "compact"));
        var topicId = admin.createTopics(List.of(topic)).topicId(COMPACT.topic()).get(30, TimeUnit.SECONDS);
        awaitInitialOwner(admin, COMPACT);
        var original = KafkaObjectCompactionRealScenario.seedExistingTransactions(port, COMPACT, topicId, client);
        assertEquals(ORIGINAL, readUncommitted(client, COMPACT, ORIGINAL.size()));
        return new Seed(topicId, original);
    }

    static void selectWhileOriginalFetchIsHeld(KafkaObjectWalAdapterRealTest.Provisioned fixture, AsyncOxiaClient control,
            S3Client minio, Properties client, int port, Seed seed) throws Exception {
        var admission = requireOriginalOutputBackpressure(fixture, minio, client);
        var terminal = KafkaObjectWalAdapterRealTest.NativeTerminal.capture(fixture, control, seed.topicId(), COMPACT.topic(), 1);
        var authorization = KafkaObjectWalProcessRealTest.authorization(terminal.metadata(), terminal.ref().shardId());
        var head = authorization.read(KafkaObjectWalProcessRealTest.scope(fixture, seed.topicId(), COMPACT)).orElseThrow();
        var first = head.grants().get(0);
        assertEquals(0, first.locator().startOffset());
        assertEquals(3, first.locator().endOffsetExclusive());
        var bucket = (String) fixture.settings().get(NereusKafkaConfigs.OBJECT_BUCKET_CONFIG);
        try (var relay = new KafkaObjectFaultRelay(bucket, first.object().key(), "*")) {
            relay.arm("held-unversioned");
            requirePinnedSelection(relay, admission, readRoute(fixture, control, seed.topicId(), COMPACT),
                    client, port, seed.topicId());
        }
        try (var producer = new KafkaProducer<String, String>(client)) {
            assertEquals(SEALED_END, producer.send(new ProducerRecord<>(COMPACT.topic(), 0, "raw-tail", "after-selection"))
                    .get(30, TimeUnit.SECONDS).offset());
        }
    }

    private static void requirePinnedSelection(KafkaObjectFaultRelay relay, QuotaAdmission admission,
            OxiaKafkaBookKeeperM4ReadRouteV2 route, Properties client, int port, Uuid topicId)
            throws Exception {
        short version = 16;
        var request = FetchRequest.Builder.forConsumer(version, 0, 1, Map.of(COMPACT,
                new FetchRequest.PartitionData(topicId, 0, FetchRequest.INVALID_LOG_START_OFFSET, 1, Optional.empty())))
                .setMaxBytes(1).isolationLevel(IsolationLevel.READ_COMMITTED).build(version);
        try (var original = new NativeKafkaRequest(port, request)) {
            relay.awaitAppliedRange();
            admission.expand();
            waitForCondition(() -> route.readSelector().flatMap(selector -> selector.fallbackSetSha256()).isPresent(),
                    30_000, "Native Object WAL did not select its semantic manifest");
            assertEquals(SELECTED, readCommitted(client, COMPACT, SELECTED.size()));
            try (var selected = new NativeKafkaRequest(port, request)) {
                var newest = ((FetchResponse) selected.receive(10_000))
                        .responseData(Map.of(topicId, COMPACT.topic()), version).get(COMPACT);
                assertEquals(Errors.NONE, Errors.forCode(newest.errorCode()));
                assertFetchedValues(newest, List.of("middle"));
            }
            relay.release();
            var response = ((FetchResponse) original.receive(10_000)).responseData(Map.of(topicId, COMPACT.topic()), version).get(COMPACT);
            assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));
            assertFetchedValues(response, List.of("old-zero", "middle", "old-two"));
        }
    }

    static void requireSelected(Admin admin, Properties client, int port, Seed seed) throws Exception {
        assertEquals(SEALED_END + 1, admin.listOffsets(Map.of(COMPACT, OffsetSpec.latest())).partitionResult(COMPACT)
                .get(5, TimeUnit.SECONDS).offset());
        KafkaObjectCompactionRealScenario.verifySelectedSemantics(port, COMPACT, seed.topicId(), SEALED_END + 1, client, seed.original());
        var expected = new ArrayList<>(SELECTED);
        expected.add("after-selection");
        assertEquals(expected, readCommitted(client, COMPACT, expected.size()));
    }
}
