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
import org.apache.kafka.clients.admin.NewPartitionReassignment;
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
import org.apache.kafka.test.TestUtils;

import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Caps;
import com.nereusstream.kafka.bookkeeper.read.KafkaObjectMaterializedPrefixV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.materialization.M5CellProviderProfileV2;
import com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCoordinatorV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PayloadKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.GenerationObject;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.TaskState;
import com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2;
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import com.nereusstream.storage.object.s3.S3C1ObjectProviderTransport;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;

import static kafka.server.nereus.KafkaBookKeeperNativeInputRealScenario.BOUNDS;
import static kafka.server.nereus.KafkaBookKeeperNativeInputRealScenario.budget;
import static kafka.server.nereus.KafkaBookKeeperNativeInputRealScenario.withCurrentMetadataImage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verified Object output and native Fetch after the exact selector transition on real BK/Oxia/MinIO. */
public class KafkaBookKeeperObjectMaterializationRealTest {
    private static final String TOPIC = "native-cold";

    @ParameterizedTest
    @ValueSource(strings = {"known", "cold", "hot", "automatic"})
    @Timeout(240)
    public void storesAndVerifiesTheRealNativeSealedPrefix(String outcome, @TempDir Path work) throws Exception {
        writeM6Capability(work);
        var nativeConfig = KafkaBookKeeperOwnerRelocationRealTest.nativeConfig(work,
                System.getProperty("nereus.nsip.bkUri"), System.getProperty("nereus.nsip.oxia"));
        withObjectFixture(work, nativeConfig, fixture -> runColdCluster(work, fixture, new PublicationFault(outcome)));
    }

    private static void runColdCluster(Path work, Fixture fixture, PublicationFault fault) throws Exception {
        var first = new CompletableFuture<NereusKafkaOwnedProviderRuntime.Opened>();
        var cold = new CompletableFuture<NereusKafkaOwnedProviderRuntime.Opened>();
        var heldFetch = new HeldFetch();
        var bootstrap = BootstrapMetadata.fromVersion(MetadataVersion.latestTesting(), "nsip-object-bytes")
                .copyWithFeatureRecord("nereus.storage.version", (short) 2);
        var nodes = new TestKitNodes.Builder(bootstrap).setNumBrokerNodes(2).setNumControllerNodes(1).build();
        var builder = new KafkaClusterTestKit.Builder(nodes)
                .setControllerStorageRuntimeFactory(new NereusControllerStorageRuntimeFactory())
                .setBrokerStorageRuntimeFactory(context -> {
                    var ready = context.config().brokerId() == 0 ? first : cold;
                    return new NereusBrokerStorageRuntime(context, new NereusKafkaOwnedProviderRuntime(
                            context, observer(ready, heldFetch, ready == first ? fault : new PublicationFault("known"))));
                });
        KafkaBookKeeperM5PolicyRealScenario.configure(builder, work, fixture.settings(), true, "OBJECT_BYTES_DISABLED");
        builder.setConfigProp("nereus.kafka.storage.bookkeeper.max.bytes.per.ledger", "1048576");
        builder.setConfigProp("message.max.bytes", "4096");
        if (fault.automatic) builder.setConfigProp("nereus.kafka.storage.retention.check.interval.ms", "1000");
        try (var cluster = builder.build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            var client = new Properties();
            client.put("bootstrap.servers", cluster.bootstrapServers());
            Uuid topicId;
            try (var admin = Admin.create(client)) {
                var topic = new NewTopic(TOPIC, Map.of(0, List.of(0)))
                        .configs(Map.of("nereus.storage.profile", "BOOKKEEPER_WAL_ASYNC_OBJECT"));
                topicId = admin.createTopics(List.of(topic)).topicId(TOPIC).get(30, TimeUnit.SECONDS);
            }
            var opened = first.get(30, TimeUnit.SECONDS);
            int produced = 0;
            while (sealedRuns(opened) < 2 && produced < 32) {
                KafkaBookKeeperRootVerifierRealScenario.produce(client, "input-" + produced, produced);
                produced++;
            }
            assertTrue(sealedRuns(opened) >= 2, "native Produce did not seal two real BK sources");
            assertAppendWhileFetchHeld(cluster, client, topicId, heldFetch, produced);
            produced++;
            ColdTakeover takeover = () -> relocate(cluster, client, cold);
            if (fault.automatic) {
                verifyAutomatic(opened, fixture, client, produced, heldFetch, takeover, fault);
            } else {
                verifyOutputs(opened, fixture, client, produced, new FetchProbe(cluster, topicId, heldFetch, fault), takeover);
            }
            cluster.fatalFaultHandler().maybeRethrowFirstException();
            cluster.nonFatalFaultHandler().maybeRethrowFirstException();
        }
    }

    private static NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver observer(
            CompletableFuture<NereusKafkaOwnedProviderRuntime.Opened> ready, HeldFetch heldFetch, PublicationFault fault) {
        return new NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver() {
            @Override
            public AsyncOxiaClient controlClient(AsyncOxiaClient client) {
                return fault.wrap(client);
            }

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
                return heldFetch.afterClose(record, phase, response);
            }
        };
    }

    private static NereusKafkaOwnedProviderRuntime.Opened relocate(KafkaClusterTestKit cluster, Properties client,
            CompletableFuture<NereusKafkaOwnedProviderRuntime.Opened> cold) throws Exception {
        var partition = new TopicPartition(TOPIC, 0);
        assertTrue(cluster.brokers().get(1).replicaManager().onlinePartition(partition).isEmpty(),
                "destination already owns local partition state");
        try (var admin = Admin.create(client)) {
            var next = Optional.of(new NewPartitionReassignment(List.of(1)));
            admin.alterPartitionReassignments(Map.of(partition, next)).all().get(30, TimeUnit.SECONDS);
            assertEquals(1, KafkaBookKeeperOwnerRelocationRealTest.awaitLeader(admin, 0));
        }
        return cold.get(30, TimeUnit.SECONDS);
    }

    private static void assertAppendWhileFetchHeld(KafkaClusterTestKit cluster, Properties client,
            Uuid topicId, HeldFetch held, int highWatermark) throws Exception {
        var hold = held.arm();
        try (var wire = fetch(cluster, topicId)) {
            try {
                hold.entered().get(30, TimeUnit.SECONDS);
                KafkaBookKeeperRootVerifierRealScenario.produce(client, "input-" + highWatermark, highWatermark);
            } finally {
                hold.release().complete(null);
            }
            assertPinnedFetch(wire, topicId, highWatermark);
        }
    }

    private static NativeKafkaRequest fetch(KafkaClusterTestKit cluster, Uuid topicId) throws Exception {
        var partition = new TopicPartition(TOPIC, 0);
        short version = 16;
        var request = FetchRequest.Builder.forConsumer(version, 0, 0, Map.of(partition,
                new FetchRequest.PartitionData(topicId, 0, FetchRequest.INVALID_LOG_START_OFFSET, 1, Optional.empty())))
                .setMaxBytes(1).isolationLevel(IsolationLevel.READ_COMMITTED).build(version);
        return new NativeKafkaRequest(cluster, 0, request);
    }

    private static void assertPinnedFetch(NativeKafkaRequest wire, Uuid topicId, long highWatermark) throws Exception {
        var response = (FetchResponse) wire.receive(30_000);
        var data = response.responseData(Map.of(topicId, TOPIC), (short) 16).get(new TopicPartition(TOPIC, 0));
        assertEquals(Errors.NONE, Errors.forCode(data.errorCode()));
        assertEquals(highWatermark, data.highWatermark(), "Fetch replaced its captured HW");
        assertEquals(highWatermark, data.lastStableOffset(), "Fetch replaced its captured LSO");
        assertEquals(0L, FetchResponse.recordsOrFail(data).batches().iterator().next().baseOffset());
    }

    private record FetchProbe(KafkaClusterTestKit cluster, Uuid topicId, HeldFetch held, PublicationFault fault) { }

    private static final class PublicationFault {
        private final boolean enabled;
        private final boolean recoverHot;
        private final boolean automatic;
        private final AtomicReference<Sha256Digest> manifest = new AtomicReference<>();
        private final CompletableFuture<M5PendingOutputInventoryV2> pendingApplied = new CompletableFuture<>();
        private final CompletableFuture<Void> pendingReadDenied = new CompletableFuture<>();
        private volatile String pendingBlocked;
        private volatile String blocked;

        private PublicationFault(String outcome) {
            recoverHot = outcome.equals("hot");
            enabled = recoverHot || outcome.equals("cold");
            automatic = outcome.equals("automatic");
        }

        private AsyncOxiaClient wrap(AsyncOxiaClient client) {
            if (!enabled && !automatic) return client;
            return (AsyncOxiaClient) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] {AsyncOxiaClient.class}, (proxy, method, args) -> {
                        String key = args != null && args.length > 0 && args[0] instanceof String value ? value : "";
                        if (method.getName().equals("get") && key.equals(blocked)) {
                            return CompletableFuture.failedFuture(new TimeoutException("original selector reread hidden"));
                        }
                        if (method.getName().equals("get") && key.equals(pendingBlocked)) {
                            pendingReadDenied.complete(null);
                            return CompletableFuture.failedFuture(new TimeoutException("original pending inventory reread hidden"));
                        }
                        Object response;
                        try {
                            response = method.invoke(client, args);
                        } catch (InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                        if (!method.getName().equals("put")) return response;
                        if (selected(key, args)) {
                            return ((CompletionStage<?>) response).thenCompose(applied -> {
                                blocked = key;
                                return CompletableFuture.failedFuture(new TimeoutException("applied Object selector response lost"));
                            }).toCompletableFuture();
                        }
                        return pendingResponse(key, args, response);
                    });
        }

        private Object pendingResponse(String key, Object[] args, Object response) {
            if (!automatic || pendingApplied.isDone() || !key.contains("/read-m5/") || !key.contains("/pending-outputs/")) {
                return response;
            }
            var original = M5PendingOutputInventoryV2.decode(CanonicalBytes.copyOf((byte[]) args[1]));
            return ((CompletionStage<?>) response).thenCompose(applied -> {
                pendingBlocked = key;
                pendingApplied.complete(original);
                return CompletableFuture.failedFuture(new TimeoutException("applied pending inventory response lost"));
            }).toCompletableFuture();
        }

        private void verifyOriginalWork(NereusKafkaOwnedProviderRuntime.Opened owner, Fixture fixture,
                NereusKafkaObjectMaterialization.Verified verified, boolean retained) throws Exception {
            var cell = fixture.profile().protocolCellSha256();
            var metadata = owner.nativeM5BindingRoute().materializationMetadata(cell);
            var plan = verified.stored().task().prepared().plan();
            var taskId = plan.taskIdSha256();
            var coordinator = new M5MaterializationCoordinatorV1(metadata, 0, plan.sourceCut().identity().binding());
            assertEquals(retained ? TaskState.OUTPUT_VERIFIED : TaskState.PUBLISHED, coordinator.readTask(taskId).orElseThrow().state());
            var charge = new M5MaterializationAdmissionV1(metadata, cell).read().orElseThrow().reservations();
            assertEquals(retained, charge.stream().anyMatch(item -> item.taskIdSha256().equals(taskId)));
            var physical = new M5TargetDeleteAuthorityCoordinatorV1(fixture.provider().physicalQuota());
            for (var credit : verified.stored().credits()) {
                var resource = credit.resource().orElseThrow();
                var fact = physical.inspect(resource.authorityKey()).toCompletableFuture().get(30, TimeUnit.SECONDS)
                        .orElseThrow().fullAuthority().orElseThrow();
                assertEquals(retained ? 1 : 0, fact.activeWriterTickets().size());
                assertEquals(Optional.of(credit), fixture.provider().physicalQuota().quota().readBirth(credit.key())
                        .toCompletableFuture().get(30, TimeUnit.SECONDS));
            }
        }

        private boolean selected(String key, Object[] args) {
            var expected = manifest.get();
            if (expected == null || !key.contains("/read-m4/") || !key.endsWith("/selector")) return false;
            var value = CanonicalBytes.copyOf((byte[]) args[1]);
            return M5BindingAuthorityCodecV1.isAuthorityValue(value)
                    && M5BindingAuthorityCodecV1.decodeAuthority(value).selectorProjection().selectedViewSha256().equals(expected)
                    && manifest.compareAndSet(expected, null);
        }
    }

    private record Hold(CompletableFuture<Void> entered, CompletableFuture<Void> release) { }

    private static final class HeldFetch {
        private final AtomicReference<Hold> next = new AtomicReference<>();
        private final AtomicInteger originalPrefixReads = new AtomicInteger();

        private Hold arm() {
            var hold = new Hold(new CompletableFuture<>(), new CompletableFuture<>());
            assertTrue(next.compareAndSet(null, hold), "a native historical Fetch is already armed");
            return hold;
        }

        private CompletionStage<Void> afterClose(
                KafkaRunRootRecordV2 record, ReadPhase phase, CompletionStage<Void> response) {
            if (phase == ReadPhase.FETCH && record.root().kafkaStartOffset() == 0) originalPrefixReads.incrementAndGet();
            if (phase != ReadPhase.FETCH) return response;
            var hold = next.getAndSet(null);
            if (hold == null) return response;
            return response.thenCompose(ignored -> {
                hold.entered().complete(null);
                return hold.release();
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

    private record Fixture(Map<String, Object> settings, NereusKafkaObjectProvider provider,
            M5CellProviderProfileV2 profile, Runnable verifyNoOutputs, Consumer<GenerationObject> removeVersion) { }

    @FunctionalInterface
    private interface FixtureWork {
        void run(Fixture fixture) throws Exception;
    }

    @FunctionalInterface
    private interface ColdTakeover {
        NereusKafkaOwnedProviderRuntime.Opened relocate() throws Exception;
    }

    private static void withObjectFixture(Path work, Map<String, Object> settings, FixtureWork action) throws Exception {
        var endpoint = URI.create(System.getProperty("nereus.nsip.s3Endpoint"));
        var configInput = new HashMap<>(settings);
        configInput.put("node.id", "0");
        configInput.put("process.roles", "broker");
        configInput.put("controller.quorum.voters", "3000@127.0.0.1:9193");
        configInput.put("controller.listener.names", "CONTROLLER");
        configInput.put("log.dirs", work.resolve("unused-fixture-log").toString());
        var config = new KafkaConfig(configInput, false);
        try (var minio = KafkaObjectProviderAdmissionRealTest.minio(endpoint);
                var control = OxiaClientBuilder.create(System.getProperty("nereus.nsip.oxia"))
                        .namespace(config.nereusKafkaStorageConfig().core().oxiaNamespace())
                        .asyncClient().get(30, TimeUnit.SECONDS)) {
            var namespace = OxiaPhysicalMetadataNamespaceV2.provision(control).toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            var bucket = KafkaObjectProviderAdmissionRealTest.newVersionedBucket(minio);
            var physical = com.nereusstream.storage.object.s3.S3M5PhysicalNamespaceAuthorityV2.provision(
                    minio, bucket, namespace.identity());
            var inputs = new HashMap<>(settings);
            KafkaObjectProviderAdmissionRealTest.configureObject(inputs, work, endpoint, bucket);
            configInput.putAll(inputs);
            var objectConfig = new KafkaConfig(configInput, false);
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
                // Inject one missing immutable version only in this test's exclusive MinIO bucket.
                Consumer<GenerationObject> removeVersion = object -> minio.deleteObject(request -> request.bucket(bucket)
                        .key(object.identity().key()).versionId(new String(object.immutableProviderVersionToken()
                                .orElseThrow().toByteArray(), StandardCharsets.UTF_8)));
                action.run(new Fixture(Map.copyOf(inputs), provider, profile, verifyNoOutputs, removeVersion));
            }
        }
    }

    private static void verifyOutputs(NereusKafkaOwnedProviderRuntime.Opened opened,
            Fixture fixture, Properties client, int produced, FetchProbe probe, ColdTakeover cold) throws Exception {
        var provider = fixture.provider();
        var profile = fixture.profile();
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
        var resumed = captured.registerObjectMaterialization(prepared, provider).toCompletableFuture()
                .get(30, TimeUnit.SECONDS);
        assertEquals(registered.worker().worker(), resumed.worker().worker());
        assertEquals(registered.pending(), resumed.pending());
        verifyExhaustion(opened, captured, registered, provider, fixture.verifyNoOutputs());
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
        publishAndRead(opened, captured, verified, fixture, client, produced, probe, cold);
    }

    private static void verifyAutomatic(NereusKafkaOwnedProviderRuntime.Opened owner, Fixture fixture,
            Properties client, int produced, HeldFetch fetch, ColdTakeover takeover, PublicationFault fault) throws Exception {
        var provider = fixture.provider();
        assertTrue(provider.physicalQuota().quota().expand(64L << 20).toCompletableFuture().get(30, TimeUnit.SECONDS));
        var cell = fixture.profile().protocolCellSha256();
        var metadata = owner.nativeM5BindingRoute().materializationMetadata(cell);
        var admission = new M5MaterializationAdmissionV1(metadata, cell);
        assertEquals(M5MaterializationAdmissionV1.Outcome.APPLIED_EXACT,
                admission.install(new M5MaterializationAdmissionV1.Caps(1, 4L << 20, 4L << 20, 32, 32, 16, 48)));
        var limits = new Caps(BOUNDS.payloadBytes(), BOUNDS.batches(), BOUNDS.records(), BOUNDS.records(),
                BOUNDS.decodedBytes(), BOUNDS.decodedBytes(), BOUNDS.decodedBytes(), BOUNDS.records(), BOUNDS.records());
        var preparation = owner.nativeM5CompactionAdmission();
        assertEquals(com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionAdmissionV1.Outcome.APPLIED_EXACT,
                preparation.install(KafkaBookKeeperSelectedInputRealScenario.caps(limits, 1)));
        var original = fault.pendingApplied.get(30, TimeUnit.SECONDS);
        try {
            fault.pendingReadDenied.get(30, TimeUnit.SECONDS);
            fixture.verifyNoOutputs().run();
        } finally {
            fault.pendingBlocked = null;
        }
        TestUtils.waitForCondition(() -> owner.nativeM5BindingRoute().readSelector().orElseThrow().fallbackSetSha256().isPresent()
                        && admission.read().orElseThrow().reservations().isEmpty()
                        && preparation.read().orElseThrow().reservations().isEmpty(),
                45_000, "native Broker background task did not select and settle its Object generation");
        var selector = owner.nativeM5BindingRoute().readSelector().orElseThrow();
        var selected = NereusKafkaObjectRecovery.recover(owner.nativeM5BindingRoute(), 0, selector, provider,
                Duration.ofSeconds(10)).orElseThrow();
        assertEquals(original, selected.inventory(), "the scheduler changed the original admitted worker or inventory");
        var authority = owner.nativeM5BindingRoute().readBindingAuthority().toCompletableFuture()
                .get(30, TimeUnit.SECONDS).orElseThrow();
        assertTrue(M5BindingAuthorityCodecV1.decodeAuthority(authority.canonicalStoredBytes()).trimMutationTickets().stream()
                .noneMatch(ticket -> ticket.key().endsWith("/pending-outputs/" + original.worker().taskIdSha256().toHex())));
        assertEquals(PayloadKind.KAFKA_BATCH_PRESERVING_V1, selected.validated().generation().payloadKind());
        assertTrue(selected.prefix().coverage().exclusiveEnd() > 0);
        var expected = IntStream.range(0, produced).mapToObj(offset -> "input-" + offset).toList();
        assertEquals(expected, KafkaBookKeeperOwnerRelocationRealTest.consume(client, produced));
        verifyColdSelector(selector, takeover.relocate(), client, expected, fetch);
    }

    private static void publishAndRead(NereusKafkaOwnedProviderRuntime.Opened opened,
            NereusKafkaNativeInputV2.Captured captured, NereusKafkaObjectMaterialization.Verified verified,
            Fixture fixture, Properties client, int produced, FetchProbe probe, ColdTakeover cold) throws Exception {
        var provider = fixture.provider();
        var heldFetch = probe.held();
        var fault = probe.fault();
        var manifest = M5MaterializationCodecV1.manifestSha256(verified.generation().manifestView());
        if (fault.enabled) fault.manifest.set(manifest);
        var reads = Executors.newSingleThreadExecutor();
        var dispatched = new AtomicInteger();
        try {
            var hold = heldFetch.arm();
            try (var wire = fetch(probe.cluster(), probe.topicId())) {
                try {
                    hold.entered().get(30, TimeUnit.SECONDS);
                    var publishing = captured.publishObjectMaterialization(verified, provider, task -> reads.execute(() -> {
                        dispatched.incrementAndGet();
                        task.run();
                    }), Duration.ofSeconds(10)).toCompletableFuture();
                    if (fault.enabled) {
                        assertThrows(ExecutionException.class, () -> publishing.get(30, TimeUnit.SECONDS));
                        assertTrue(fault.blocked != null, "the original selector write was not applied");
                    } else {
                        publishing.get(30, TimeUnit.SECONDS);
                    }
                } finally {
                    hold.release().complete(null);
                }
                assertPinnedFetch(wire, probe.topicId(), produced);
                assertEquals(0, dispatched.get(), "the old pinned Fetch replanned after Object selection");
            }
            fault.blocked = null;
            if (!fault.enabled) captured.discard().toCompletableFuture().get(30, TimeUnit.SECONDS);
            var selector = opened.nativeM5BindingRoute().readSelector().orElseThrow();
            assertEquals(manifest, selector.selectedViewSha256());
            assertTrue(selector.fallbackSetSha256().isPresent());
            var reopened = NereusKafkaObjectRecovery.recover(opened.nativeM5BindingRoute(), 0, selector,
                    provider, Duration.ofSeconds(10)).orElseThrow().prefix();
            assertEquals(selector.selectedViewSha256(), reopened.manifestSha256());
            assertEquals(verified.protections(), reopened.originalFallbackSources());
            if (fault.enabled) {
                fault.verifyOriginalWork(opened, fixture, verified, true);
                if (fault.recoverHot) {
                    captured.publishObjectMaterialization(verified, provider, task -> {
                        throw new AssertionError("reconciliation replaced the original provider executor");
                    }, Duration.ofSeconds(10)).toCompletableFuture().get(30, TimeUnit.SECONDS);
                    captured.discard().toCompletableFuture().get(30, TimeUnit.SECONDS);
                    fault.verifyOriginalWork(opened, fixture, verified, false);
                    assertEquals(selector, opened.nativeM5BindingRoute().readSelector().orElseThrow());
                } else {
                    var expected = IntStream.range(0, produced).mapToObj(offset -> "input-" + offset).toList();
                    var recovered = cold.relocate();
                    verifyColdSelector(selector, recovered, client, expected, heldFetch);
                    fault.verifyOriginalWork(recovered, fixture, verified, false);
                    return;
                }
            }
            long before = sealedRuns(opened);
            int end = produced;
            while (sealedRuns(opened) == before && end < produced + 32) {
                KafkaBookKeeperRootVerifierRealScenario.produce(client, "input-" + end, end);
                end++;
            }
            assertTrue(sealedRuns(opened) > before, "BK did not roll its tail after Object selection");
            assertEquals(selector.selectedViewSha256(),
                    opened.nativeM5BindingRoute().readSelector().orElseThrow().selectedViewSha256());
            var expected = IntStream.range(0, end).mapToObj(offset -> "input-" + offset).toList();
            int originalReads = heldFetch.originalPrefixReads.get();
            assertEquals(expected, KafkaBookKeeperOwnerRelocationRealTest.consume(client, end));
            assertTrue(dispatched.get() > 0, "native Kafka Fetch never dispatched its selected Object reads");
            assertEquals(originalReads, heldFetch.originalPrefixReads.get(), "valid Object prefix was bypassed");
            var recovered = cold.relocate();
            verifyColdSelector(selector, recovered, client, expected, heldFetch);
            var payloads = verified.generation().generation().payloadObjects();
            fixture.removeVersion().accept(payloads.get(payloads.size() - 1));
            var afterLoss = new java.util.ArrayList<>(expected);
            afterLoss.add("input-" + end);
            KafkaBookKeeperRootVerifierRealScenario.produce(client, afterLoss.get(end), end);
            assertEquals(afterLoss, KafkaBookKeeperOwnerRelocationRealTest.consume(client, end + 1));
            assertTrue(heldFetch.originalPrefixReads.get() > originalReads,
                    "missing Object did not restart its read on the captured original BK prefix");
            assertEquals(selector.selectedViewSha256(),
                    recovered.nativeM5BindingRoute().readSelector().orElseThrow().selectedViewSha256());
        } finally {
            reads.shutdown();
            assertTrue(reads.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    private static void verifyColdSelector(
            com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector selector,
            NereusKafkaOwnedProviderRuntime.Opened recovered, Properties client, List<String> expected,
            HeldFetch heldFetch) throws Exception {
        var coldSelector = recovered.nativeM5BindingRoute().readSelector().orElseThrow();
        assertTrue(coldSelector.ownerEpoch() > selector.ownerEpoch());
        assertEquals(selector.selectedViewSha256(), coldSelector.selectedViewSha256());
        assertEquals(selector.fallbackSetSha256(), coldSelector.fallbackSetSha256());
        int originalReads = heldFetch.originalPrefixReads.get();
        assertEquals(expected, KafkaBookKeeperOwnerRelocationRealTest.consume(client, expected.size()));
        assertEquals(originalReads, heldFetch.originalPrefixReads.get(), "cold Fetch bypassed its selected Object prefix");
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
