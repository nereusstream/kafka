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
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
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
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verified Object output and native Fetch after the exact selector transition on real BK/Oxia/MinIO. */
public class KafkaBookKeeperObjectMaterializationRealTest {
    private static final String TOPIC = "native-cold";

    @ParameterizedTest
    @ValueSource(strings = {"known", "cold", "hot", "automatic", "stale-registration", "retirement", "background-retirement", "faults", "semantic"})
    @Timeout(240)
    public void storesAndVerifiesTheRealNativeSealedPrefix(String outcome, @TempDir Path work) throws Exception {
        writeM6Capability(work);
        var nativeConfig = KafkaBookKeeperOwnerRelocationRealTest.nativeConfig(work,
                System.getProperty("nereus.nsip.bkUri"), System.getProperty("nereus.nsip.oxia"));
        var fault = new PublicationFault(outcome);
        withObjectFixture(work, nativeConfig, fault.readFaults, fixture -> runColdCluster(work, fixture, fault));
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
        configureMaterialization(builder, work, fixture, fault);
        try (var cluster = builder.build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            var client = new Properties();
            client.put("bootstrap.servers", cluster.bootstrapServers());
            Uuid topicId;
            try (var admin = Admin.create(client)) {
                var topic = nativeTopic(fault.semantic);
                topicId = admin.createTopics(List.of(topic)).topicId(TOPIC).get(30, TimeUnit.SECONDS);
            }
            var opened = first.get(30, TimeUnit.SECONDS);
            int produced = seedSealedSources(cluster, opened, client, fault.semantic);
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

    private static void configureMaterialization(KafkaClusterTestKit.Builder builder, Path work, Fixture fixture,
            PublicationFault fault) throws Exception {
        KafkaBookKeeperM5PolicyRealScenario.configure(builder, work, fixture.settings(), true,
                fault.retireFallback || fault.backgroundRetirement ? "OBJECT_GC_ENABLED" : "OBJECT_BYTES_DISABLED");
        builder.setConfigProp("nereus.kafka.storage.bookkeeper.max.bytes.per.ledger", "1048576");
        builder.setConfigProp("message.max.bytes", "4096");
        if (fault.automatic) builder.setConfigProp("nereus.kafka.storage.retention.check.interval.ms", "1000");
        if (fault.backgroundRetirement) {
            builder.setConfigProp("nereus.kafka.storage.materialization.source.retirement.grace.ms", "5000");
        }
    }

    private static int seedSealedSources(KafkaClusterTestKit cluster, NereusKafkaOwnedProviderRuntime.Opened owner,
            Properties client, boolean semantic) throws Exception {
        int produced = semantic ? KafkaObjectCompactionRealScenario.seed(cluster, client) : 0;
        int required = semantic ? KafkaObjectCompactionRealScenario.SEALED_TRANSACTION_END : 0;
        while ((sealedRuns(owner) < 2 || owner.storage().nativeReadSources().activeRoot().kafkaStartOffset() < required)
                && produced < 32) {
            KafkaBookKeeperRootVerifierRealScenario.produce(client, "input-" + produced, produced);
            produced++;
        }
        assertTrue(sealedRuns(owner) >= 2, "native Produce did not seal two real BK sources");
        return produced;
    }

    private static NewTopic nativeTopic(boolean semantic) {
        var settings = semantic
                ? Map.of("nereus.storage.profile", "BOOKKEEPER_WAL_ASYNC_OBJECT", "cleanup.policy", "compact",
                        "min.cleanable.dirty.ratio", "0.0")
                : Map.of("nereus.storage.profile", "BOOKKEEPER_WAL_ASYNC_OBJECT");
        return new NewTopic(TOPIC, Map.of(0, List.of(0))).configs(settings);
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
        private final boolean staleRegistration;
        private final boolean retireFallback;
        private final boolean backgroundRetirement;
        private final boolean readFaults;
        private final boolean semantic;
        private final AtomicReference<Sha256Digest> manifest = new AtomicReference<>();
        private final CompletableFuture<M5PendingOutputInventoryV2> pendingApplied = new CompletableFuture<>();
        private final CompletableFuture<Void> pendingReadDenied = new CompletableFuture<>();
        private volatile String pendingBlocked;
        private volatile String blocked;

        private PublicationFault(String outcome) {
            recoverHot = outcome.equals("hot");
            enabled = recoverHot || outcome.equals("cold");
            backgroundRetirement = outcome.equals("background-retirement");
            staleRegistration = outcome.equals("stale-registration");
            automatic = outcome.equals("automatic") || backgroundRetirement || staleRegistration;
            retireFallback = outcome.equals("retirement");
            readFaults = outcome.equals("faults");
            semantic = outcome.equals("semantic");
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

        private void verifyScheduledInventory(NereusKafkaOwnedProviderRuntime.Opened owner, Fixture fixture,
                M5PendingOutputInventoryV2 original, M5PendingOutputInventoryV2 selected) throws Exception {
            if (!staleRegistration) {
                assertEquals(original, selected, "the scheduler changed the original admitted worker or inventory");
                return;
            }
            var old = original.worker().taskIdSha256();
            var cell = fixture.profile().protocolCellSha256();
            var coordinator = new M5MaterializationCoordinatorV1(owner.nativeM5BindingRoute().materializationMetadata(cell),
                    0, original.worker().identity().binding());
            assertFalse(old.equals(selected.worker().taskIdSha256()), "the stale worker was selected");
            assertEquals(original.cancelUnstarted(), coordinator.readPendingOutputs(old).orElseThrow());
            assertEquals(TaskState.CANCELLED_STALE, coordinator.readTask(old).orElseThrow().state());
            fixture.verifyNoOriginalOutputs().accept(original);
            var quota = fixture.provider().physicalQuota().quota();
            var scope = quota.snapshot().toCompletableFuture().get(30, TimeUnit.SECONDS).head().scope();
            for (var slot : original.outputs()) {
                assertTrue(quota.readBirth(M5ObjectBirthCreditV3.key(scope, original.worker(), slot))
                        .toCompletableFuture().get(30, TimeUnit.SECONDS).isEmpty());
            }
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
        private static void verifyFaultMatrix(NereusKafkaOwnedProviderRuntime.Opened owner, Fixture fixture,
                NereusKafkaObjectMaterialization.Verified verified, FetchProbe probe, BindingReadSelector selector, int produced)
                throws Exception {
            var object = verified.generation().generation().payloadObjects().get(0);
            var version = new String(object.immutableProviderVersionToken().orElseThrow().toByteArray(), StandardCharsets.UTF_8);
            try (var relay = new KafkaObjectFaultRelay(fixture.bucket(), object.identity().key(), version)) {
                assertFaultFetch(owner, relay, probe, selector, "unavailable", "UNAVAILABLE", produced);
                assertFaultFetch(owner, relay, probe, selector, "corrupt", "CORRUPT_OR_FORMAT", produced);
                assertFaultFetch(owner, relay, probe, selector, "denied", "NOT_ELIGIBLE", produced);
                assertEquals(selector, owner.nativeM5BindingRoute().readSelector().orElseThrow());
                cancelWireWithObjectResponseHeld(owner, probe, relay, selector);
            }
        }

        private static void assertFaultFetch(NereusKafkaOwnedProviderRuntime.Opened owner, KafkaObjectFaultRelay relay,
                FetchProbe probe, BindingReadSelector selector, String mode, String expected, int produced) throws Exception {
            int before = probe.held().originalPrefixReads.get();
            relay.arm(mode);
            var failure = assertThrows(ExecutionException.class, () -> owner.storage().verifySelectedObjectPrefix(
                    selector.selectedViewSha256()).toCompletableFuture().get(30, TimeUnit.SECONDS));
            var qualified = assertInstanceOf(KafkaObjectMaterializedPrefixV1.ReadFailure.class, failure.getCause());
            assertEquals(expected, qualified.failureClass().name());
            assertEquals(before, probe.held().originalPrefixReads.get());
            try (var wire = fetch(probe.cluster(), probe.topicId())) {
                if (!expected.equals("NOT_ELIGIBLE")) {
                    assertPinnedFetch(wire, probe.topicId(), produced);
                    assertTrue(probe.held().originalPrefixReads.get() > before, "qualified Object failure did not use protected BK");
                } else {
                    var response = (FetchResponse) wire.receive(30_000);
                    var data = response.responseData(Map.of(probe.topicId(), TOPIC), (short) 16).get(new TopicPartition(TOPIC, 0));
                    assertEquals(Errors.KAFKA_STORAGE_ERROR, Errors.forCode(data.errorCode()));
                    assertEquals(before, probe.held().originalPrefixReads.get(), "Object authorization failure used BK fallback");
                }
                relay.awaitRequest();
            }
        }

        private static void cancelWireWithObjectResponseHeld(NereusKafkaOwnedProviderRuntime.Opened owner,
                FetchProbe probe, KafkaObjectFaultRelay relay, BindingReadSelector selector) throws Exception {
            var control = Executors.newSingleThreadExecutor();
            try {
                relay.arm("held");
                try (var wire = fetch(probe.cluster(), probe.topicId())) {
                    relay.awaitRequest();
                } // The client disappears before the accepted Object SDK response can finish.
                var retirement = owner.storage().beginNativeObjectFallbackRetirement(selector.selectedViewSha256());
                owner.storage().closeNativeObjectFallbackAdmission(retirement, control).toCompletableFuture().get(30, TimeUnit.SECONDS);
                var retiring = owner.storage().finishNativeObjectFallbackRetirement(retirement, control).toCompletableFuture();
                try {
                    assertFalse(retiring.isDone(), "a cancelled client released the accepted Object response's pin");
                    var binding = owner.nativeM5BindingRoute();
                    var closed = binding.readSelector().orElseThrow();
                    var batch = closed.activeBatches().stream().filter(row ->
                            row.sharedLastFallbackCapableReadAdmissionEpoch() == selector.readAdmissionEpoch()).findFirst().orElseThrow();
                    for (var source : batch.sources()) {
                        var fact = binding.readProtectionFact(source.sourceIdentitySha256(), source.protectionGeneration())
                                .toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow();
                        assertEquals(ProtectionState.PROTECTED, M4ReadControlCodecV1.decodeProtection(fact.canonicalStoredBytes()).state());
                    }
                } finally {
                    relay.release();
                }
                retiring.get(30, TimeUnit.SECONDS);
            } finally {
                control.shutdown();
                assertTrue(control.awaitTermination(30, TimeUnit.SECONDS));
            }
        }

        private final AtomicReference<Hold> next = new AtomicReference<>();
        private final AtomicInteger originalPrefixReads = new AtomicInteger();
        private final AtomicReference<List<Long>> releasedLedgers = new AtomicReference<>(List.of());
        private final AtomicInteger releasedLedgerReads = new AtomicInteger();
        private final AtomicInteger retainedCheckpointReads = new AtomicInteger();
        private volatile long retainedLedger = -1;

        private void observeRetainedCheckpoint(
                com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1.ObjectBookKeeperSourceTransfer transfer) {
            var tail = transfer.retained().sealedRoots();
            observeRetainedCheckpoint(transfer.durableCheckpoint().orElseThrow().sealedCarrier().ledgerIdentity().ledgerId(),
                    transfer.original().sealedRoots().stream().filter(root -> !tail.contains(root))
                            .map(root -> root.ledgerIdentity().ledgerId()).toList());
        }

        private void observeRetainedCheckpoint(long ledger, List<Long> retired) {
            retainedLedger = ledger;
            releasedLedgers.set(List.copyOf(retired));
            assertFalse(retired.isEmpty());
        }

        private void reclaimReleasedSources(NereusKafkaOwnedProviderRuntime.Opened owner) throws Exception {
            assertTrue(owner.nativeM5GcAdmissionOpen().toCompletableFuture().get(30, TimeUnit.SECONDS));
            try (var scan = owner.openNativeM5SourceScan().toCompletableFuture().get(30, TimeUnit.SECONDS)) {
                var inputs = NereusKafkaM5GcInputsV1.captureReplacement(scan).toCompletableFuture().get(30, TimeUnit.SECONDS);
                var targets = inputs.resources().stream().map(resource -> inputs.handle(resource).ledgerIdentity().ledgerId()).sorted().toList();
                assertEquals(releasedLedgers.get().stream().sorted().toList(), targets);
                assertFalse(targets.contains(retainedLedger));
                scan.reclaimReplacedSources().toCompletableFuture().get(60, TimeUnit.SECONDS);
                for (var resource : inputs.resources()) {
                    var done = scan.physical().read(resource.authorityKey()).toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow();
                    assertTrue(com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2.decode(done).compactDone().isPresent());
                    assertTrue(scan.physical().quota().requireExistingReservation(resource).toCompletableFuture().get(30, TimeUnit.SECONDS).settled());
                    assertEquals(com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureOutcome.DEFINITIVELY_ABSENT,
                            scan.nativeClient().captureExactTarget(inputs.handle(resource)).toCompletableFuture().get(30, TimeUnit.SECONDS).outcome());
                }
            }
        }

        private void beginColdRecoveryObservation() {
            releasedLedgerReads.set(0);
            retainedCheckpointReads.set(0);
        }

        private void assertRetainedRecovery() {
            assertEquals(0, releasedLedgerReads.get(), "cold recovery opened an original released BK ledger");
            assertTrue(retainedCheckpointReads.get() > 0, "cold recovery did not read the retained BK checkpoint");
        }


        private Hold arm() {
            var hold = new Hold(new CompletableFuture<>(), new CompletableFuture<>());
            assertTrue(next.compareAndSet(null, hold), "a native historical Fetch is already armed");
            return hold;
        }

        private CompletionStage<Void> afterClose(
                KafkaRunRootRecordV2 record, ReadPhase phase, CompletionStage<Void> response) {
            long ledger = record.root().ledgerIdentity().ledgerId();
            if (releasedLedgers.get().contains(ledger)) releasedLedgerReads.incrementAndGet();
            if (ledger == retainedLedger && phase == ReadPhase.CHECKPOINT) retainedCheckpointReads.incrementAndGet();
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

    /** Observe the actual targets without holding the background actor's exclusive source scan. */
    private record ScheduledDeletion(OxiaQuotaTargetDeleteStoreV2 metadata, M5BookKeeperNativeCreateClientV2 client,
            List<RunLedgerHandleV1> handles) {
        static ScheduledDeletion observe(NereusKafkaOwnedProviderRuntime.Opened owner, List<KafkaRunRootSnapshotV1> roots)
                throws Exception {
            try (var scan = owner.openNativeM5SourceScan().toCompletableFuture().get(30, TimeUnit.SECONDS)) {
                var client = scan.nativeClient();
                var config = client.capabilitySnapshot().configurationDigest();
                return new ScheduledDeletion(scan.physical(), client, roots.stream().map(root ->
                        new RunLedgerHandleV1(root.providerScopeId(), root.runId(), root.ledgerIdentity(), config)).toList());
            }
        }

        boolean settled() throws Exception {
            for (var handle : handles) {
                var resource = new PhysicalResourceIdV2.BookKeeperLedger(client.spec().namespace(), handle.ledgerIdentity().ledgerId());
                var fact = metadata.read(resource.authorityKey()).toCompletableFuture().get(5, TimeUnit.SECONDS);
                if (fact.filter(value -> com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2.decode(value)
                        .compactDone().isPresent()).isEmpty()
                        || !metadata.quota().requireExistingReservation(resource).toCompletableFuture().get(5, TimeUnit.SECONDS).settled()) {
                    return false;
                }
                assertEquals(com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureOutcome.DEFINITIVELY_ABSENT,
                        client.captureExactTarget(handle).toCompletableFuture().get(5, TimeUnit.SECONDS).outcome());
            }
            return true;
        }

        void requirePresentBeforeCheckpoint() throws Exception {
            for (var handle : handles) {
                assertEquals(com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureOutcome.EXACT_TARGET,
                        client.captureExactTarget(handle).toCompletableFuture().get(5, TimeUnit.SECONDS).outcome(),
                        "BK deletion preceded the independent retained checkpoint");
            }
        }
    }

    private static final class HeldObjectFetch implements Executor, AutoCloseable {
        private final AtomicReference<Hold> next = new AtomicReference<>();
        private final AtomicInteger dispatched = new AtomicInteger();
        private final ExecutorService reads = Executors.newSingleThreadExecutor();

        private Hold arm() {
            var hold = new Hold(new CompletableFuture<>(), new CompletableFuture<>());
            assertTrue(next.compareAndSet(null, hold), "an Object Fetch is already armed");
            return hold;
        }

        private static void rejectReplacement(Runnable request) {
            throw new AssertionError("reconciliation replaced the original provider executor");
        }

        @Override
        public void execute(Runnable request) {
            reads.execute(() -> {
                dispatched.incrementAndGet();
                var hold = next.getAndSet(null);
                if (hold != null) {
                    hold.entered().complete(null);
                    hold.release().join();
                }
                request.run();
            });
        }

        @Override
        public void close() throws InterruptedException {
            reads.shutdown();
            assertTrue(reads.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    static void writeM6Capability(Path work) throws Exception {
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

    record Fixture(Map<String, Object> settings, NereusKafkaObjectProvider provider,
            M5CellProviderProfileV2 profile, String bucket, Runnable verifyNoOutputs,
            Consumer<M5PendingOutputInventoryV2> verifyNoOriginalOutputs, Consumer<GenerationObject> removeVersion,
            KafkaConfig config, AsyncOxiaClient control, OxiaPhysicalMetadataNamespaceV2 namespace) { }

    @FunctionalInterface
    interface FixtureWork {
        void run(Fixture fixture) throws Exception;
    }

    @FunctionalInterface
    private interface ColdTakeover {
        NereusKafkaOwnedProviderRuntime.Opened relocate() throws Exception;
    }

    static void withObjectFixture(Path work, Map<String, Object> settings, boolean readFaults,
            FixtureWork action) throws Exception {
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
            KafkaObjectProviderAdmissionRealTest.configureObject(inputs, work,
                    readFaults ? KafkaObjectFaultRelay.endpoint() : endpoint, bucket);
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
                            .prefix(profile.exclusiveNamespacePrefix() + "/m5/"));
                    assertTrue(objects.versions().isEmpty(), "an incomplete quota reservation wrote Object data");
                };
                Consumer<M5PendingOutputInventoryV2> verifyNoOriginalOutputs = original -> {
                    var prefix = profile.exclusiveNamespacePrefix() + "/m5/materialization/" + original.worker().taskIdSha256().toHex() + "/";
                    var versions = minio.listObjectVersions(list -> list.bucket(bucket).prefix(prefix));
                    assertTrue(versions.versions().isEmpty(), "the cancelled original task reached the Object store");
                };
                // Inject one missing immutable version only in this test's exclusive MinIO bucket.
                Consumer<GenerationObject> removeVersion = object -> minio.deleteObject(request -> request.bucket(bucket)
                        .key(object.identity().key()).versionId(new String(object.immutableProviderVersionToken()
                                .orElseThrow().toByteArray(), StandardCharsets.UTF_8)));
                action.run(new Fixture(Map.copyOf(inputs), provider, profile, bucket, verifyNoOutputs, verifyNoOriginalOutputs,
                        removeVersion, objectConfig, control, namespace));
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
        assertEquals(probe.fault().semantic ? PayloadKind.KAFKA_SEMANTIC_COMPACTED_V1 : PayloadKind.KAFKA_BATCH_PRESERVING_V1,
                verified.generation().generation().payloadKind());
        assertEquals(probe.fault().semantic ? 1 : captured.extents().size(), verified.generation().generation().payloadObjects().size());
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
        if (!probe.fault().semantic) verifyPrefixRead(captured, prepared, verified, provider);
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
            if (fault.staleRegistration) produced = produceThroughRollover(owner, client, produced);
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
        fault.verifyScheduledInventory(owner, fixture, original, selected.inventory());
        var authority = owner.nativeM5BindingRoute().readBindingAuthority().toCompletableFuture()
                .get(30, TimeUnit.SECONDS).orElseThrow();
        assertTrue(M5BindingAuthorityCodecV1.decodeAuthority(authority.canonicalStoredBytes()).trimMutationTickets().stream()
                .noneMatch(ticket -> ticket.key().endsWith("/pending-outputs/" + original.worker().taskIdSha256().toHex())));
        assertEquals(PayloadKind.KAFKA_BATCH_PRESERVING_V1, selected.validated().generation().payloadKind());
        assertTrue(selected.prefix().coverage().exclusiveEnd() > 0);
        if (fault.backgroundRetirement) {
            verifyScheduledRetirement(owner, selector, selected.prefix().coverage().exclusiveEnd(), client, produced, fetch, takeover);
            return;
        }
        var expected = IntStream.range(0, produced).mapToObj(offset -> "input-" + offset).toList();
        assertEquals(expected, KafkaBookKeeperOwnerRelocationRealTest.consume(client, produced));
        verifyColdSelector(selector, takeover.relocate(), client, expected, fetch);
    }

    private static void verifyScheduledRetirement(NereusKafkaOwnedProviderRuntime.Opened owner, BindingReadSelector original,
            long coveredEnd, Properties client, int produced, HeldFetch fetch, ColdTakeover takeover) throws Exception {
        var raw = owner.storage().nativeReadSources().sealedRoots().stream().filter(root -> root.kafkaStartOffset() < coveredEnd).toList();
        assertFalse(raw.isEmpty());
        var deletion = ScheduledDeletion.observe(owner, raw);
        TestUtils.waitForCondition(() -> owner.nativeM5BindingRoute().readSelector().orElseThrow().mode() == SelectorMode.PREFERRED_ONLY,
                30_000, "background retirement did not close old fallback admission");
        int afterRollover = produceThroughRollover(owner, client, produced);
        TestUtils.waitForCondition(() -> owner.storage().nativeReadSources().sealedRoots().stream().noneMatch(raw::contains),
                30_000, "background retirement did not transfer the drained raw prefix");
        long carrier = owner.storage().nativeReadSources().activeRoot().ledgerIdentity().ledgerId();
        deletion.requirePresentBeforeCheckpoint();
        int end = produceThroughRollover(owner, client, afterRollover);
        fetch.observeRetainedCheckpoint(carrier, raw.stream().map(root -> root.ledgerIdentity().ledgerId()).toList());
        TestUtils.waitForCondition(deletion::settled,
                45_000, "background retirement did not settle exact native BK deletion and quota");
        var selected = owner.nativeM5BindingRoute().readSelector().orElseThrow();
        assertEquals(original.selectedViewSha256(), selected.selectedViewSha256());
        var expected = IntStream.range(0, end).mapToObj(offset -> "input-" + offset).toList();
        assertEquals(expected, KafkaBookKeeperOwnerRelocationRealTest.consume(client, end));
        fetch.beginColdRecoveryObservation();
        verifyColdSelector(selected, takeover.relocate(), client, expected, fetch);
        fetch.assertRetainedRecovery();
    }

    private static void publishAndRead(NereusKafkaOwnedProviderRuntime.Opened opened,
            NereusKafkaNativeInputV2.Captured captured, NereusKafkaObjectMaterialization.Verified verified,
            Fixture fixture, Properties client, int produced, FetchProbe probe, ColdTakeover cold) throws Exception {
        var provider = fixture.provider();
        var heldFetch = probe.held();
        var fault = probe.fault();
        var manifest = M5MaterializationCodecV1.manifestSha256(verified.generation().manifestView());
        if (fault.enabled) fault.manifest.set(manifest);
        try (var reads = new HeldObjectFetch()) {
            var hold = heldFetch.arm();
            try (var wire = fetch(probe.cluster(), probe.topicId())) {
                try {
                    hold.entered().get(30, TimeUnit.SECONDS);
                    var publishing = captured.publishObjectMaterialization(verified, provider, reads,
                            Duration.ofSeconds(10)).toCompletableFuture();
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
                assertEquals(0, reads.dispatched.get(), "the old pinned Fetch replanned after Object selection");
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
            if (verifySpecialSelection(opened, verified, fixture, client, produced, probe, selector, cold, reads)) return;
            if (fault.enabled) {
                fault.verifyOriginalWork(opened, fixture, verified, true);
                if (fault.recoverHot) {
                    captured.publishObjectMaterialization(verified, provider, HeldObjectFetch::rejectReplacement,
                            Duration.ofSeconds(10)).toCompletableFuture().get(30, TimeUnit.SECONDS);
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
            int end = produceThroughRollover(opened, client, produced);
            assertEquals(selector.selectedViewSha256(),
                    opened.nativeM5BindingRoute().readSelector().orElseThrow().selectedViewSha256());
            var expected = IntStream.range(0, end).mapToObj(offset -> "input-" + offset).toList();
            int originalReads = heldFetch.originalPrefixReads.get();
            assertEquals(expected, KafkaBookKeeperOwnerRelocationRealTest.consume(client, end));
            assertTrue(reads.dispatched.get() > 0, "native Kafka Fetch never dispatched its selected Object reads");
            assertEquals(originalReads, heldFetch.originalPrefixReads.get(), "valid Object prefix was bypassed");
            var recovered = cold.relocate();
            verifyColdSelector(selector, recovered, client, expected, heldFetch);
            var payloads = verified.generation().generation().payloadObjects();
            fixture.removeVersion().accept(payloads.get(payloads.size() - 1));
            assertThrows(ExecutionException.class, () -> recovered.storage()
                    .verifySelectedObjectPrefix(selector.selectedViewSha256()).toCompletableFuture().get(30, TimeUnit.SECONDS));
            var afterLoss = new java.util.ArrayList<>(expected);
            afterLoss.add("input-" + end);
            KafkaBookKeeperRootVerifierRealScenario.produce(client, afterLoss.get(end), end);
            assertEquals(afterLoss, KafkaBookKeeperOwnerRelocationRealTest.consume(client, end + 1));
            assertTrue(heldFetch.originalPrefixReads.get() > originalReads,
                    "missing Object did not restart its read on the captured original BK prefix");
            assertEquals(selector.selectedViewSha256(),
                    recovered.nativeM5BindingRoute().readSelector().orElseThrow().selectedViewSha256());
        }
    }

    private static boolean verifySpecialSelection(NereusKafkaOwnedProviderRuntime.Opened owner,
            NereusKafkaObjectMaterialization.Verified verified, Fixture fixture, Properties client,
            int produced, FetchProbe probe, BindingReadSelector selector, ColdTakeover cold, HeldObjectFetch reads) throws Exception {
        var fault = probe.fault();
        if (fault.semantic) {
            verifySemanticSelection(owner, verified, fixture, client, produced, probe, selector, cold);
        } else if (fault.readFaults) {
            HeldFetch.verifyFaultMatrix(owner, fixture, verified, probe, selector, produced);
        } else if (fault.retireFallback) {
            retireWhileReadHeld(owner, probe, reads, produced, selector, fixture.provider(), client, cold);
        } else {
            return false;
        }
        return true;
    }

    private static void verifySemanticSelection(NereusKafkaOwnedProviderRuntime.Opened owner,
            NereusKafkaObjectMaterialization.Verified verified, Fixture fixture, Properties client,
            int produced, FetchProbe probe, BindingReadSelector selector, ColdTakeover cold) throws Exception {
        var cluster = probe.cluster();
        var topic = probe.topicId();
        assertTrue(verified.generation().generation().coverage().exclusiveEnd() >= KafkaObjectCompactionRealScenario.SEALED_TRANSACTION_END,
                "transaction control batches were left on the BK tail instead of the semantic Object output");
        owner.storage().verifySelectedObjectPrefix(selector.selectedViewSha256()).toCompletableFuture().get(30, TimeUnit.SECONDS);
        int reads = probe.held().originalPrefixReads.get();
        KafkaObjectCompactionRealScenario.verify(cluster, topic, 0, produced, client);
        var recovered = cold.relocate();
        var after = recovered.nativeM5BindingRoute().readSelector().orElseThrow();
        assertTrue(after.ownerEpoch() > selector.ownerEpoch());
        assertEquals(selector.selectedViewSha256(), after.selectedViewSha256());
        KafkaObjectCompactionRealScenario.verify(cluster, topic, 1, produced, client);
        assertEquals(reads, probe.held().originalPrefixReads.get(), "compacted Fetch used the original raw BK prefix");
        fixture.removeVersion().accept(verified.generation().generation().payloadObjects().get(0));
        KafkaObjectCompactionRealScenario.verifyMissingFailsClosed(cluster, topic, 1);
        assertEquals(reads, probe.held().originalPrefixReads.get(), "missing compacted Object exposed unfiltered BK bytes");
    }

    private static int produceThroughRollover(NereusKafkaOwnedProviderRuntime.Opened owner, Properties client, int produced)
            throws Exception {
        long before = sealedRuns(owner);
        int end = produced;
        while (sealedRuns(owner) == before && end < produced + 32) {
            KafkaBookKeeperRootVerifierRealScenario.produce(client, "input-" + end, end);
            end++;
        }
        assertTrue(sealedRuns(owner) > before, "BK did not roll its tail after Object selection");
        return end;
    }

    private static void retireWhileReadHeld(NereusKafkaOwnedProviderRuntime.Opened owner,
            FetchProbe probe, HeldObjectFetch reads, int produced, BindingReadSelector selected,
            NereusKafkaObjectProvider provider, Properties client, ColdTakeover cold) throws Exception {
        var route = owner.nativeM5BindingRoute();
        var raw = owner.storage().nativeReadSources();
        var control = Executors.newSingleThreadExecutor();
        owner.storage().verifySelectedObjectPrefix(selected.selectedViewSha256()).toCompletableFuture().get(30, TimeUnit.SECONDS);
        var hold = reads.arm();
        try (var wire = fetch(probe.cluster(), probe.topicId())) {
            hold.entered().get(30, TimeUnit.SECONDS);
            var retirement = owner.storage().beginNativeObjectFallbackRetirement(selected.selectedViewSha256());
            owner.storage().closeNativeObjectFallbackAdmission(retirement, control).toCompletableFuture().get(30, TimeUnit.SECONDS);
            var retiring = owner.storage().finishNativeObjectFallbackRetirement(retirement, control).toCompletableFuture();
            int end;
            try {
                var closed = route.readSelector().orElseThrow();
                assertEquals(SelectorMode.PREFERRED_ONLY, closed.mode());
                end = produceThroughRollover(owner, client, produced);
                var rolled = route.readSelector().orElseThrow();
                assertEquals(closed.readAdmissionEpoch(), rolled.readAdmissionEpoch());
                assertTrue(rolled.sourceGeneration() > closed.sourceGeneration(), "BK tail generation did not advance");
                assertFalse(retiring.isDone(), "a captured Object read did not retain its old fallback through BK rollover");
                var batch = closed.activeBatches().stream().filter(row ->
                        row.sharedLastFallbackCapableReadAdmissionEpoch() == selected.readAdmissionEpoch()).findFirst().orElseThrow();
                for (var source : batch.sources()) {
                    var fact = route.readProtectionFact(source.sourceIdentitySha256(), source.protectionGeneration())
                            .toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow();
                    assertEquals(ProtectionState.PROTECTED, M4ReadControlCodecV1.decodeProtection(fact.canonicalStoredBytes()).state());
                }
            } finally {
                hold.release().complete(null);
            }
            assertPinnedFetch(wire, probe.topicId(), produced);
            var result = retiring.get(30, TimeUnit.SECONDS);
            var sealed = owner.storage().nativeReadSources().sealedRoots();
            assertEquals(raw.sealedRoots(), sealed.subList(0, raw.sealedRoots().size()),
                    "read retirement dropped native BK recovery references");
            for (var source : result.batch().sources()) {
                var fact = route.readProtectionFact(source.sourceIdentitySha256(), source.protectionGeneration())
                        .toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow();
                var released = M4ReadControlCodecV1.decodeProtection(fact.canonicalStoredBytes());
                assertEquals(ProtectionState.RELEASED, released.state());
                assertEquals(Optional.of(result.batch().batchIdSha256()), released.releasedByBatchSha256());
            }
            assertEquals(selected.selectedViewSha256(), result.selector().selectedViewSha256());
            assertEquals(selected.readAdmissionEpoch() + 1, result.selector().readAdmissionEpoch());
            assertEquals(result.selector(), route.readSelector().orElseThrow());
            verifyTransferredCheckpoint(owner, probe, result, provider, client, end, cold);
        } finally {
            hold.release().complete(null);
            control.shutdown();
            assertTrue(control.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    private static void verifyTransferredCheckpoint(NereusKafkaOwnedProviderRuntime.Opened owner, FetchProbe probe,
            com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperNativeFallbackRetirementV2.Result released,
            NereusKafkaObjectProvider provider, Properties client, int end, ColdTakeover cold) throws Exception {
        var before = owner.storage().nativeInputSources().recoveryCheckpoint().orElseThrow();
        var transfer = owner.storage().transferRetiredObjectBookKeeperPrefix(released);
        assertEquals(before.components(), transfer.checkpoint().components());
        assertEquals(before.vector(), transfer.checkpoint().vector());
        assertEquals(transfer.retained(), owner.storage().nativeReadSources());
        assertTrue(transfer.retained().sealedRoots().size() < transfer.original().sealedRoots().size());
        assertTrue(transfer.durableCheckpoint().isEmpty(), "a local transfer was mistaken for a durable checkpoint");
        int produced = produceThroughRollover(owner, client, end);
        var checkpoint = transfer.durableCheckpoint().orElseThrow();
        assertTrue(owner.storage().nativeReadSources().sealedRoots().contains(checkpoint.sealedCarrier()));
        assertEquals(transfer.retained().activeRoot().runId(), checkpoint.sealedCarrier().runId());
        assertEquals(transfer.retained().activeRoot().ledgerIdentity(), checkpoint.sealedCarrier().ledgerIdentity());
        assertTrue(checkpoint.checkpointEntry() > 0 && !checkpoint.checkpointSha256().isZero() && !checkpoint.footerSha256().isZero());
        var selector = owner.nativeM5BindingRoute().readSelector().orElseThrow();
        assertEquals(released.selector().selectedViewSha256(), selector.selectedViewSha256());
        var expected = IntStream.range(0, produced).mapToObj(offset -> "input-" + offset).toList();
        assertEquals(expected, KafkaBookKeeperOwnerRelocationRealTest.consume(client, produced));
        NereusKafkaOwnedProviderRuntime.SelectedRecoveryMetadata.persistObjectScope(owner, provider, transfer, Duration.ofSeconds(10))
                .toCompletableFuture().get(30, TimeUnit.SECONDS);
        probe.held().observeRetainedCheckpoint(transfer);
        probe.held().reclaimReleasedSources(owner);
        assertEquals(expected, KafkaBookKeeperOwnerRelocationRealTest.consume(client, produced));
        probe.held().beginColdRecoveryObservation();
        verifyColdSelector(selector, cold.relocate(), client, expected, probe.held());
        probe.held().assertRetainedRecovery();
    }

    private static void verifyColdSelector(
            BindingReadSelector selector,
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
