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

import kafka.server.KafkaConfig;
import kafka.server.nsip.KafkaBookKeeperOwnerRelocationRealTest;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.server.common.MetadataVersion;

import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Caps;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3;
import com.nereusstream.storage.object.materialization.M5CellProviderProfileV2;
import com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PayloadKind;
import com.nereusstream.storage.object.s3.S3C1ObjectProviderTransport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import io.oxia.client.api.OxiaClientBuilder;

import static kafka.server.nereus.KafkaBookKeeperNativeInputRealScenario.BOUNDS;
import static kafka.server.nereus.KafkaBookKeeperNativeInputRealScenario.budget;
import static kafka.server.nereus.KafkaBookKeeperNativeInputRealScenario.withCurrentMetadataImage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verified Object output from native sealed BK bytes, before any selector publication. */
public class KafkaBookKeeperObjectMaterializationRealTest {
    @Test
    @Timeout(240)
    public void storesAndVerifiesTheRealNativeSealedPrefix(@TempDir Path work) throws Exception {
        writeM6Capability(work);
        var ready = new CompletableFuture<NereusKafkaOwnedProviderRuntime.Opened>();
        var bootstrap = BootstrapMetadata.fromVersion(MetadataVersion.latestTesting(), "nsip-object-bytes")
                .copyWithFeatureRecord("nereus.storage.version", (short) 2);
        var nodes = new TestKitNodes.Builder(bootstrap).setNumBrokerNodes(1).setNumControllerNodes(1).build();
        var observer = new NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver() {
            @Override
            public void afterReady(TopicIdPartition partition, NereusKafkaOwnedProviderRuntime.Opened opened) {
                if (partition.topic().equals("native-cold")) ready.complete(opened);
            }

            @Override
            public void afterRecovery(TopicIdPartition partition, Throwable failure) {
                if (partition.topic().equals("native-cold") && failure != null) ready.completeExceptionally(failure);
            }
        };
        var builder = new KafkaClusterTestKit.Builder(nodes)
                .setControllerStorageRuntimeFactory(new NereusControllerStorageRuntimeFactory())
                .setBrokerStorageRuntimeFactory(context -> new NereusBrokerStorageRuntime(context,
                        new NereusKafkaOwnedProviderRuntime(context, observer)));
        var nativeConfig = KafkaBookKeeperOwnerRelocationRealTest.nativeConfig(work,
                System.getProperty("nereus.nsip.bkUri"), System.getProperty("nereus.nsip.oxia"));
        KafkaBookKeeperM5PolicyRealScenario.configure(builder, work, nativeConfig, true, "OBJECT_BYTES_DISABLED");
        builder.setConfigProp("nereus.kafka.storage.bookkeeper.max.bytes.per.ledger", "1048576");
        builder.setConfigProp("message.max.bytes", "4096");
        try (var cluster = builder.build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            var client = new Properties();
            client.put("bootstrap.servers", cluster.bootstrapServers());
            try (var admin = Admin.create(client)) {
                admin.createTopics(List.of(new NewTopic("native-cold", Map.of(0, List.of(0)))))
                        .all().get(30, TimeUnit.SECONDS);
            }
            var opened = ready.get(30, TimeUnit.SECONDS);
            int produced = 0;
            while (sealedRuns(opened) < 2 && produced < 32) {
                KafkaBookKeeperRootVerifierRealScenario.produce(client, "input-" + produced, produced);
                produced++;
            }
            assertTrue(sealedRuns(opened) >= 2, "native Produce did not seal two real BK sources");
            materialize(cluster, opened, work);
            assertEquals(IntStream.range(0, produced).mapToObj(offset -> "input-" + offset).toList(),
                    KafkaBookKeeperOwnerRelocationRealTest.consume(client, produced));
            cluster.fatalFaultHandler().maybeRethrowFirstException();
            cluster.nonFatalFaultHandler().maybeRethrowFirstException();
        }
    }

    private static void writeM6Capability(Path work) throws Exception {
        var summary = Path.of(System.getProperty("nereus.nsip.m4RunSummary")).toRealPath();
        var source = Path.of(System.getProperty("nereus.nsip.m4SourceRoot")).toRealPath();
        KafkaBookKeeperOwnerRelocationRealTest.writeCapability(work, Optional.empty());
        Files.writeString(work.resolve("capability.properties"), "\nm6IsolatedAdmission.sourceRoot=" + source
                + "\nm6IsolatedAdmission.runSummaryFile=" + summary
                + "\nm6IsolatedAdmission.runSummarySha256="
                + NereusKafkaM4AdmissionV2.sha(Files.readAllBytes(summary)).toHex() + "\n", StandardOpenOption.APPEND);
    }

    private static long sealedRuns(NereusKafkaOwnedProviderRuntime.Opened opened) {
        return opened.storage().nativeReadSources().sealedRoots().stream()
                .filter(root -> root.kafkaEndOffsetExclusive().orElseThrow() > root.kafkaStartOffset()).count();
    }

    private static void materialize(KafkaClusterTestKit cluster, NereusKafkaOwnedProviderRuntime.Opened opened, Path work)
            throws Exception {
        var endpoint = URI.create(System.getProperty("nereus.nsip.s3Endpoint"));
        var config = cluster.brokers().get(0).config();
        try (var minio = KafkaObjectProviderAdmissionRealTest.minio(endpoint);
                var control = OxiaClientBuilder.create(System.getProperty("nereus.nsip.oxia"))
                        .namespace(config.nereusKafkaStorageConfig().core().oxiaNamespace())
                        .asyncClient().get(30, TimeUnit.SECONDS)) {
            var namespace = OxiaPhysicalMetadataNamespaceV2.provision(control).toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            var bucket = KafkaObjectProviderAdmissionRealTest.newVersionedBucket(minio);
            var physical = com.nereusstream.storage.object.s3.S3M5PhysicalNamespaceAuthorityV2.provision(
                    minio, bucket, namespace.identity());
            var inputs = new HashMap<>(config.originals());
            KafkaObjectProviderAdmissionRealTest.configureObject(inputs, work, endpoint, bucket);
            var objectConfig = new KafkaConfig(inputs, false);
            var cell = NereusBookKeeperCapability.metadataPolicy(objectConfig).cellIdentity();
            var refs = new M5CellProviderProfileV2.References("local-s3",
                    KafkaObjectProviderAdmissionRealTest.CREDENTIAL_SCOPE, "NONE", "kafka-cell-quota",
                    "kafka-owner", "version-match-delete");
            var physicalBinding = namespace.bind(physical).toCompletableFuture().get(30, TimeUnit.SECONDS);
            var profile = new M5CellProviderProfileV2(physicalBinding, cell, KafkaObjectProviderAdmissionRealTest.PREFIX,
                    refs, S3C1ObjectProviderTransport.ADAPTER_VERSION,
                    NereusKafkaObjectProvider.configurationSha256(objectConfig.nereusKafkaStorageConfig().core()),
                    1 << 20, 1 << 20, 100, 1024);
            Files.write(work.resolve("object-capability"), profile.encode().toByteArray());
            namespace.openCellProviderScopes(physical).thenCompose(registry -> registry.prepare(profile).admit())
                    .toCompletableFuture().get(30, TimeUnit.SECONDS);
            namespace.openAuthorityRoute(physical).thenCompose(quota -> quota.initialize(4L << 20))
                    .toCompletableFuture().get(30, TimeUnit.SECONDS);
            try (var provider = NereusKafkaObjectProvider.open(objectConfig, cell, namespace)) {
                Runnable verifyNoOutputs = () -> {
                    var objects = minio.listObjectVersions(list -> list.bucket(bucket)
                            .prefix(profile.exclusiveNamespacePrefix() + "/m5/materialization/"));
                    assertTrue(objects.versions().isEmpty(), "an incomplete quota reservation wrote Object data");
                };
                verifyOutputs(opened, provider, profile, verifyNoOutputs);
            }
        }
    }

    private static void verifyOutputs(NereusKafkaOwnedProviderRuntime.Opened opened,
            NereusKafkaObjectProvider provider, M5CellProviderProfileV2 profile, Runnable verifyNoOutputs) throws Exception {
        var metadata = opened.nativeM5BindingRoute().materializationMetadata(profile.protocolCellSha256());
        var admission = new M5MaterializationAdmissionV1(metadata, profile.protocolCellSha256());
        assertEquals(M5MaterializationAdmissionV1.Outcome.APPLIED_EXACT,
                admission.install(new M5MaterializationAdmissionV1.Caps(1, 4L << 20, 4L << 20, 32, 32, 16, 48)));
        var limits = new Caps(BOUNDS.payloadBytes(), BOUNDS.batches(), BOUNDS.records(), BOUNDS.records(),
                BOUNDS.decodedBytes(), BOUNDS.decodedBytes(), BOUNDS.decodedBytes(), BOUNDS.records(), BOUNDS.records());
        var caps = KafkaBookKeeperSelectedInputRealScenario.caps(limits, 1);
        var allowance = new NereusKafkaNativeInputV2.CaptureAllowance(limits, 0, 0, 4096, 0, 4096, 64);
        var captured = withCurrentMetadataImage(
                () -> opened.captureM5Inputs(budget(opened), BOUNDS, caps, allowance), true);
        var prepared = withCurrentMetadataImage(() -> captured.prepareObjectMaterialization(provider));
        var registered = captured.registerObjectMaterialization(prepared, provider).toCompletableFuture()
                .get(30, TimeUnit.SECONDS);
        verifyExhaustion(opened, captured, registered, provider, verifyNoOutputs);
        var verified = captured.storeObjectMaterialization(registered, provider, Duration.ofSeconds(10))
                .toCompletableFuture().get(30, TimeUnit.SECONDS);
        assertEquals(PayloadKind.KAFKA_BATCH_PRESERVING_V1, verified.generation().generation().payloadKind());
        assertEquals(captured.extents().size(), verified.generation().generation().payloadObjects().size());
        assertEquals(captured.frontiers().candidateEndOffsetExclusive(),
                verified.generation().generation().coverage().exclusiveEnd());
        assertEquals(registered.pending().outputs().size(), verified.stored().credits().size());
        assertTrue(verified.stored().credits().stream().allMatch(credit ->
                credit.state() == M5ObjectBirthCreditV3.State.ASSIGNED && credit.resource().isPresent()));
        for (var credit : verified.stored().credits()) {
            var membership = provider.physicalQuota().members().readFact(credit.resource().orElseThrow())
                    .toCompletableFuture().get(30, TimeUnit.SECONDS).inventory();
            assertEquals(profile.protocolCellSha256(), membership.protocolCell());
            assertEquals(List.of(new com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Member(
                    com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain.KAFKA_OFFSET,
                    profile.scopeId().digest(), captured.predecessorSelector().binding())), membership.members());
        }
        var replay = captured.storeObjectMaterialization(registered, provider, Duration.ofSeconds(10))
                .toCompletableFuture().get(30, TimeUnit.SECONDS);
        assertEquals(verified.stored().credits(), replay.stored().credits());
        assertEquals(verified.generation(), replay.generation());
        assertEquals(captured.predecessorSelector(), opened.nativeM5BindingRoute().readSelector().orElseThrow());
        captured.discard().toCompletableFuture().get(30, TimeUnit.SECONDS);
    }

    private static void verifyExhaustion(NereusKafkaOwnedProviderRuntime.Opened opened, NereusKafkaNativeInputV2.Captured captured,
            NereusKafkaObjectMaterialization.Registered registered, NereusKafkaObjectProvider provider,
            Runnable verifyNoOutputs) throws Exception {
        var exhausted = assertThrows(ExecutionException.class, () ->
                captured.storeObjectMaterialization(registered, provider, Duration.ofSeconds(10))
                        .toCompletableFuture().get(30, TimeUnit.SECONDS));
        assertInstanceOf(NereusKafkaObjectMaterialization.QuotaExhaustedException.class, exhausted.getCause());
        verifyNoOutputs.run();
        var quota = provider.physicalQuota().quota();
        var blocked = quota.snapshot().toCompletableFuture().get(30, TimeUnit.SECONDS);
        assertTrue(blocked.head().births().orElseThrow().unassigned() > 0);
        assertEquals(captured.predecessorSelector(), opened.nativeM5BindingRoute().readSelector().orElseThrow());
        // This is an explicit operator expansion after the insufficient-quota case has passed.
        assertTrue(quota.expand(64L << 20).toCompletableFuture().get(30, TimeUnit.SECONDS));
    }
}
