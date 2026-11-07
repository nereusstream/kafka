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
import kafka.server.nsip.NativeKafkaRequest;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.server.common.MetadataVersion;

import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Caps;
import com.nereusstream.kafka.bookkeeper.read.KafkaObjectMaterializedPrefixV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private static final String TOPIC = "native-cold";

    @Test
    @Timeout(240)
    public void storesAndVerifiesTheRealNativeSealedPrefix(@TempDir Path work) throws Exception {
        writeM6Capability(work);
        var ready = new CompletableFuture<NereusKafkaOwnedProviderRuntime.Opened>();
        var heldFetch = new HeldFetch();
        var bootstrap = BootstrapMetadata.fromVersion(MetadataVersion.latestTesting(), "nsip-object-bytes")
                .copyWithFeatureRecord("nereus.storage.version", (short) 2);
        var nodes = new TestKitNodes.Builder(bootstrap).setNumBrokerNodes(1).setNumControllerNodes(1).build();
        var observer = new NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver() {
            @Override
            public void afterReady(TopicIdPartition partition, NereusKafkaOwnedProviderRuntime.Opened opened) {
                if (partition.topic().equals(TOPIC)) ready.complete(opened);
            }

            @Override
            public void afterRecovery(TopicIdPartition partition, Throwable failure) {
                if (partition.topic().equals(TOPIC) && failure != null) ready.completeExceptionally(failure);
            }

            @Override
            public CompletionStage<Void> afterHistoryReaderClose(
                    KafkaRunRootRecordV2 record, ReadPhase phase, CompletionStage<Void> response) {
                return heldFetch.afterClose(phase, response);
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
            Uuid topicId;
            try (var admin = Admin.create(client)) {
                topicId = admin.createTopics(List.of(new NewTopic(TOPIC, Map.of(0, List.of(0)))))
                        .topicId(TOPIC).get(30, TimeUnit.SECONDS);
            }
            var opened = ready.get(30, TimeUnit.SECONDS);
            int produced = 0;
            while (sealedRuns(opened) < 2 && produced < 32) {
                KafkaBookKeeperRootVerifierRealScenario.produce(client, "input-" + produced, produced);
                produced++;
            }
            assertTrue(sealedRuns(opened) >= 2, "native Produce did not seal two real BK sources");
            assertAppendWhileFetchHeld(cluster, client, topicId, heldFetch, produced);
            produced++;
            materialize(cluster, opened, work);
            assertEquals(IntStream.range(0, produced).mapToObj(offset -> "input-" + offset).toList(),
                    KafkaBookKeeperOwnerRelocationRealTest.consume(client, produced));
            cluster.fatalFaultHandler().maybeRethrowFirstException();
            cluster.nonFatalFaultHandler().maybeRethrowFirstException();
        }
    }

    private static void assertAppendWhileFetchHeld(KafkaClusterTestKit cluster, Properties client,
            Uuid topicId, HeldFetch held, int highWatermark) throws Exception {
        var partition = new TopicPartition(TOPIC, 0);
        short version = 16;
        var fetch = FetchRequest.Builder.forConsumer(version, 0, 0, Map.of(partition,
                new FetchRequest.PartitionData(topicId, 0, FetchRequest.INVALID_LOG_START_OFFSET, 1, Optional.empty())))
                .setMaxBytes(1).isolationLevel(IsolationLevel.READ_COMMITTED).build(version);
        held.armed.set(true);
        try (var wire = new NativeKafkaRequest(cluster, 0, fetch)) {
            try {
                held.awaitingResponse.get(30, TimeUnit.SECONDS);
                KafkaBookKeeperRootVerifierRealScenario.produce(client, "input-" + highWatermark, highWatermark);
            } finally {
                held.release.complete(null);
            }
            var response = (FetchResponse) wire.receive(30_000);
            var data = response.responseData(Map.of(topicId, TOPIC), version).get(partition);
            assertEquals(Errors.NONE, Errors.forCode(data.errorCode()));
            assertEquals(highWatermark, data.highWatermark(), "Fetch replaced its captured HW after Produce");
            assertEquals(highWatermark, data.lastStableOffset(), "Fetch replaced its captured LSO after Produce");
            assertEquals(0L, FetchResponse.recordsOrFail(data).batches().iterator().next().baseOffset());
        }
    }

    private static final class HeldFetch {
        private final AtomicBoolean armed = new AtomicBoolean();
        private final CompletableFuture<Void> awaitingResponse = new CompletableFuture<>();
        private final CompletableFuture<Void> release = new CompletableFuture<>();

        private CompletionStage<Void> afterClose(ReadPhase phase, CompletionStage<Void> response) {
            if (phase != ReadPhase.FETCH || !armed.compareAndSet(true, false)) return response;
            return response.thenCompose(ignored -> {
                awaitingResponse.complete(null);
                return release;
            });
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
        verifyPrefixRead(captured, prepared, verified, provider);
        assertEquals(captured.predecessorSelector(), opened.nativeM5BindingRoute().readSelector().orElseThrow());
        captured.discard().toCompletableFuture().get(30, TimeUnit.SECONDS);
    }

    private static void verifyPrefixRead(NereusKafkaNativeInputV2.Captured captured,
            NereusKafkaObjectMaterialization.Prepared prepared, NereusKafkaObjectMaterialization.Verified verified,
            NereusKafkaObjectProvider provider) throws Exception {
        var prefix = new KafkaObjectMaterializedPrefixV1(prepared.plan(), verified.generation(),
                object -> provider.readVersion(object.identity(), object.immutableProviderVersionToken().orElseThrow())
                        .value());
        var coverage = prefix.coverage();
        var full = prefix.read(coverage.inclusiveStart(), coverage.exclusiveEnd() + 1, BOUNDS.payloadBytes());
        var original = captured.batches().stream().map(batch -> batch.canonicalBody()).toList();
        assertEquals(original, full.batches().stream()
                .map(KafkaObjectMaterializedPrefixV1.SelectedBatch::rawAssignedRecordBatch).toList());
        assertTrue(full.canReadTail());
        var first = prefix.read(coverage.inclusiveStart(), coverage.exclusiveEnd(), 1);
        assertEquals(List.of(full.batches().get(0)), first.batches());
        assertTrue(!first.canReadTail());
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
