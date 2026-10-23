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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.server.common.MetadataVersion;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperM4BrokerReadOwnerV2;
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperSequentialReadRequestV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperClosedHistoryRecoveryV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.kafka.KafkaBookKeeperOwnerAuthorityV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.BoundPhysicalDeleteAuthorityRouteV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterTicketV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;

import io.oxia.client.api.AsyncOxiaClient;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Real Runtime admission failures; synthetic fence eligibility proves rejection only, never deletion authority. */
public final class KafkaBookKeeperSourcePublicationRealScenario {
    private KafkaBookKeeperSourcePublicationRealScenario() { }

    public static void run(Path receipt, String mode) throws Exception {
        var inputs = Files.createTempDirectory("nereus-nsip-source-publication-");
        var fault = new Fault(mode);
        boolean verified = false;
        try {
            KafkaBookKeeperOwnerRelocationRealTest.writeCapability(inputs, Optional.of(receipt));
            try (var cluster = cluster(inputs, fault)) {
                cluster.format();
                cluster.startup();
                cluster.waitForReadyBrokers();
                try {
                    exercise(cluster, fault);
                    verified = true;
                } finally {
                    fault.oldClose.complete(null);
                }
            } catch (java.util.concurrent.ExecutionException | org.apache.kafka.server.fault.FaultHandlerException failure) {
                if (!verified) throw failure;
                assertTrue(containsFailure(failure, fault.rejected.join().getMessage()),
                        "shutdown reported an unrelated failure: " + failure);
            }
        } finally {
            fault.oldClose.complete(null);
            try (var files = Files.walk(inputs)) {
                for (var file : files.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(file);
            }
        }
    }

    private static KafkaClusterTestKit cluster(Path inputs, Fault fault) throws Exception {
        var bootstrap = BootstrapMetadata.fromVersion(MetadataVersion.latestTesting(), "nsip-source-publication")
                .copyWithFeatureRecord("nereus.storage.version", (short) 2);
        var nodes = new TestKitNodes.Builder(bootstrap).setNumBrokerNodes(3).setNumControllerNodes(1).build();
        var builder = new KafkaClusterTestKit.Builder(nodes)
                .setControllerStorageRuntimeFactory(new NereusControllerStorageRuntimeFactory())
                .setBrokerStorageRuntimeFactory(context -> new NereusBrokerStorageRuntime(context,
                        new NereusKafkaOwnedProviderRuntime(context, fault.observer(context.config().brokerId()))));
        KafkaBookKeeperOwnerRelocationRealTest.nativeConfig(inputs, System.getProperty("nereus.nsip.bkUri"),
                System.getProperty("nereus.nsip.oxia")).forEach(builder::setConfigProp);
        builder.setConfigProp("nereus.kafka.storage.bookkeeper.max.reads.inflight", "3");
        builder.setConfigProp("nereus.kafka.storage.bookkeeper.max.entries.per.ledger", "8");
        builder.setConfigProp("nereus.kafka.storage.bookkeeper.max.bytes.per.ledger", "1048576");
        builder.setConfigProp("message.max.bytes", "4096");
        return builder.build();
    }

    private static void exercise(KafkaClusterTestKit cluster, Fault fault) throws Exception {
        var client = new Properties();
        client.put("bootstrap.servers", cluster.bootstrapServers());
        try (var admin = Admin.create(client)) {
            var partition = new TopicPartition("native-cold", 0);
            admin.createTopics(List.of(new NewTopic(partition.topic(), Map.of(0, List.of(0)))))
                    .all().get(30, TimeUnit.SECONDS);
            for (int offset = 0; offset < 5; offset++)
                KafkaBookKeeperRootVerifierRealScenario.produce(client, "before-" + offset, offset);
            var old = fault.oldOwner.get(30, TimeUnit.SECONDS);
            var owner = readOwner(old);
            var captured = owner.selectorRuntime().currentAuthority().get();
            var read = old.read(new KafkaBookKeeperSequentialReadRequestV1(
                    0, KafkaReadIsolationV1.READ_UNCOMMITTED, 128, Optional.empty())).toCompletableFuture();
            fault.oldHeld.get(10, TimeUnit.SECONDS);
            assertEquals(com.nereusstream.storage.object.read.BindingReadHazardPoolV1.ScanOutcome.PINNED,
                    owner.hazards().scan(captured.bindingId(), captured.sourceGeneration()));
            KafkaBookKeeperRootVerifierRealScenario.reassign(admin, partition, 1, 0);
            var failure = fault.rejected.get(30, TimeUnit.SECONDS);
            assertTrue(containsFailure(failure, fault.expectedFailure()), "missing source admission fault: " + failure);
            assertEquals(0, fault.ready.get(), "failed admission exposed a local read owner");
            assertEquals(0, fault.fetchSdk.get(), "failed admission opened a new historical source reader");
            assertEquals(com.nereusstream.storage.object.read.BindingReadHazardPoolV1.ScanOutcome.PINNED,
                    owner.hazards().scan(captured.bindingId(), captured.sourceGeneration()));
            fault.verifyTickets();
            fault.verifyReferences();
            fault.verifySelector();
            if (fault.membershipFailure()) fault.verifyMemberStorage();
            else if (!fault.mode.equals("FENCED")) fault.verifyCleanupRetry();
            assertTrue(!read.isDone(), "fenced old observer terminated before the actual held SDK close");
            fault.oldClose.complete(null);
            var oldFailure = assertThrows(java.util.concurrent.ExecutionException.class, () -> read.get(10, TimeUnit.SECONDS));
            assertTrue(containsFailure(oldFailure, "native BK partition owner is fenced"),
                    "old read reported another failure after its SDK close");
            owner.drain().toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertEquals(com.nereusstream.storage.object.read.BindingReadHazardPoolV1.ScanOutcome.CLEAN,
                    owner.hazards().scan(captured.bindingId(), captured.sourceGeneration()));
        }
    }

    private static KafkaBookKeeperM4BrokerReadOwnerV2 readOwner(KafkaBookKeeperPartitionV1 storage) throws Exception {
        var field = KafkaBookKeeperPartitionV1.class.getDeclaredField("m4ReadOwner");
        field.setAccessible(true);
        return (KafkaBookKeeperM4BrokerReadOwnerV2) field.get(storage);
    }

    private static final class Fault {
        private final String mode;
        private final CompletableFuture<KafkaBookKeeperPartitionV1> oldOwner = new CompletableFuture<>();
        private final CompletableFuture<Void> oldHeld = new CompletableFuture<>();
        private final CompletableFuture<Void> oldClose = new CompletableFuture<>();
        private final CompletableFuture<Throwable> rejected = new CompletableFuture<>();
        private final CompletableFuture<Void> fenceInstalled = new CompletableFuture<>();
        private final AtomicBoolean holdOnce = new AtomicBoolean(true);
        private final AtomicBoolean fenceOnce = new AtomicBoolean(true);
        private final AtomicBoolean committed = new AtomicBoolean();
        private final AtomicBoolean blockCleanup = new AtomicBoolean(true);
        private final AtomicBoolean blockSelectorReads = new AtomicBoolean(true);
        private final AtomicBoolean memberFaultOnce = new AtomicBoolean(true);
        private final AtomicInteger publications = new AtomicInteger();
        private final AtomicInteger ready = new AtomicInteger();
        private final AtomicInteger fetchSdk = new AtomicInteger();
        private final AtomicInteger nativeCalls = new AtomicInteger();
        private final AtomicReference<ProofBoundWriterTicketV1> ticket = new AtomicReference<>();
        private final Set<PhysicalResourceIdV2> acquired = ConcurrentHashMap.newKeySet();
        private volatile BoundPhysicalDeleteAuthorityRouteV2 route;
        private volatile KafkaBookKeeperPartitionV1.NativeReadSources sources;
        private volatile NereusKafkaOwnedProviderRuntime.Opened opened;
        private volatile PhysicalResourceIdV2 fenced;
        private volatile AsyncOxiaClient actualOxia;
        private volatile String selectorKey;
        private volatile String memberKey;
        private CanonicalBytes memberBytes;
        private long memberVersion;

        private Fault(String mode) {
            this.mode = mode;
        }

        private boolean membershipFailure() {
            return mode.startsWith("MEMBER_");
        }

        private String expectedFailure() {
            return switch (mode) {
                case "FENCED" -> "native M4 source reference admission was rejected";
                case "UNKNOWN" -> "committed source selector reread unknown";
                case "CLEANUP" -> "native invocation terminated but its exact ticket release is unresolved";
                case "MEMBER_MISSING" -> "native physical member inventory is missing";
                case "MEMBER_UNKNOWN" -> "native source member read unavailable";
                case "MEMBER_REFRESH" -> "native physical member inventory changed during capture";
                default -> throw new IllegalArgumentException(mode);
            };
        }

        private NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver observer(int broker) {
            return new NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver() {
                @Override
                public BoundPhysicalDeleteAuthorityRouteV2 writerAuthorityRoute(BoundPhysicalDeleteAuthorityRouteV2 actual) {
                    if (broker != 1) return actual;
                    route = actual;
                    return authorityClient(actual);
                }

                @Override
                public AsyncOxiaClient controlClient(AsyncOxiaClient actual) {
                    if (broker == 1) actualOxia = actual;
                    return broker == 1 ? selectorClient(actual) : actual;
                }

                @Override
                public void beforeNativeSourcePublication(NereusKafkaOwnedProviderRuntime.Opened owner,
                        KafkaBookKeeperPartitionV1.NativeReadSources selected) {
                    if (broker == 1) {
                        opened = owner;
                        sources = selected;
                        if (membershipFailure()) prepareMemberFault();
                    }
                }

                @Override
                public void afterReady(org.apache.kafka.common.TopicIdPartition partition, KafkaBookKeeperPartitionV1 storage) {
                    if (broker == 0) oldOwner.complete(storage);
                    if (broker == 1) ready.incrementAndGet();
                }

                @Override
                public void afterRecovery(org.apache.kafka.common.TopicIdPartition partition, Throwable failure) {
                    if (broker == 1 && failure != null) rejected.complete(failure);
                }

                @Override
                public CompletionStage<Void> afterHistoryReaderClose(KafkaRunRootRecordV2 record,
                        KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase phase, CompletionStage<Void> actual) {
                    if (broker == 0 && phase == KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase.FETCH
                            && holdOnce.compareAndSet(true, false)) return actual.thenCompose(ignored -> {
                                oldHeld.complete(null);
                                return oldClose;
                            });
                    return actual;
                }

                @Override
                public BookKeeperCellSession historyReaderSession(KafkaRunRootRecordV2 record,
                        KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase phase, BookKeeperCellSession session) {
                    if (broker == 1 && phase == KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase.FETCH) fetchSdk.incrementAndGet();
                    return countedSession(broker, session);
                }

                @Override
                public BookKeeperCellSession birthReaderSession(BookKeeperCellSession session) {
                    return countedSession(broker, session);
                }

                @Override
                public CompletionStage<Void> afterRootVerifierClose(KafkaRunRootRecordV2 record, CompletionStage<Void> actual) {
                    if (broker == 1) nativeCalls.incrementAndGet();
                    return actual;
                }

                @Override
                public CompletionStage<ProviderMutationResultV1<RunLedgerHandleV1>> afterCreate(
                        CompletionStage<ProviderMutationResultV1<RunLedgerHandleV1>> actual) {
                    if (broker == 1) nativeCalls.incrementAndGet();
                    return actual;
                }
            };
        }

        private BookKeeperCellSession countedSession(int broker, BookKeeperCellSession actual) {
            if (broker != 1) return actual;
            return (BookKeeperCellSession) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] {BookKeeperCellSession.class}, (proxy, method, args) -> {
                        if (!method.getName().equals("providerScopeId") && !method.getName().equals("capabilitySnapshot"))
                            nativeCalls.incrementAndGet();
                        return invoke(actual, method, args);
                    });
        }

        private BoundPhysicalDeleteAuthorityRouteV2 authorityClient(BoundPhysicalDeleteAuthorityRouteV2 actual) {
            return (BoundPhysicalDeleteAuthorityRouteV2) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] {BoundPhysicalDeleteAuthorityRouteV2.class}, (proxy, method, args) -> {
                        if (!method.getName().equals("compareAndSet")) return invoke(actual, method, args);
                        var next = M5TargetDeleteAuthorityCodecV1.decodeAuthority((CanonicalBytes) args[2]);
                        var reference = next.activeWriterTickets().stream()
                                .filter(value -> value.writerClass() == ProofBoundWriterClassV1.REFERENCE_SHARED_PHYSICAL_MEMBER_V1)
                                .findFirst();
                        if (reference.isPresent() && sources != null) {
                            ticket.compareAndSet(null, reference.orElseThrow());
                            acquired.add(next.target().resourceId());
                            if (mode.equals("FENCED") && acquired.size() == 2 && fenceOnce.compareAndSet(true, false))
                                return fenceTarget(next.target().resourceId()).whenComplete((ignored, failure) -> {
                                    if (failure == null) fenceInstalled.complete(null);
                                    else fenceInstalled.completeExceptionally(failure);
                                }).thenCompose(ignored -> {
                                    try {
                                        return (CompletionStage<?>) invoke(actual, method, args);
                                    } catch (Throwable failure) {
                                        return CompletableFuture.failedFuture(failure);
                                    }
                                });
                        }
                        if (mode.equals("CLEANUP") && ticket.get() != null && blockCleanup.get()
                                && acquired.contains(next.target().resourceId())
                                && removesOriginalTicket(args, next))
                            return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("source invocation cleanup refused"));
                        return invoke(actual, method, args);
                    });
        }

        private boolean removesOriginalTicket(Object[] args,
                com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1 next) {
            if (((Optional<?>) args[0]).isEmpty()) return false;
            var previous = (com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue)
                    ((Optional<?>) args[0]).orElseThrow();
            return M5TargetDeleteAuthorityCodecV1.decodeAuthority(previous.canonicalStoredBytes()).activeWriterTickets()
                    .stream().anyMatch(value -> value.operationIdSha256().equals(ticket.get().operationIdSha256()))
                    && next.activeWriterTickets().stream().noneMatch(value ->
                            value.operationIdSha256().equals(ticket.get().operationIdSha256()));
        }

        private AsyncOxiaClient selectorClient(AsyncOxiaClient actual) {
            return (AsyncOxiaClient) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] {AsyncOxiaClient.class}, (proxy, method, args) -> {
                        boolean selector = args != null && args.length > 0 && args[0] instanceof String key
                                && key.contains("/read-m4/") && key.endsWith("/selector");
                        if (selector) selectorKey = (String) args[0];
                        if (selector && method.getName().equals("get") && mode.equals("UNKNOWN") && committed.get()
                                && blockSelectorReads.get())
                            return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("committed source selector reread unknown"));
                        if (isFaultedMemberRead(method, args))
                            return faultMemberRead(actual, method, args);
                        var result = invoke(actual, method, args);
                        if (selector && method.getName().equals("put") && ticket.get() != null) {
                            publications.incrementAndGet();
                            return ((CompletionStage<?>) result).thenCompose(value -> {
                                committed.set(true);
                                return mode.equals("UNKNOWN") ? CompletableFuture.failedFuture(
                                        new java.util.concurrent.TimeoutException("committed source selector reply lost"))
                                        : CompletableFuture.completedFuture(value);
                            }).toCompletableFuture();
                        }
                        return result;
                    });
        }

        private boolean isFaultedMemberRead(java.lang.reflect.Method method, Object[] args) {
            return method.getName().equals("get") && args != null && args.length > 0
                    && args[0].equals(memberKey) && !mode.equals("MEMBER_MISSING")
                    && memberFaultOnce.compareAndSet(true, false);
        }

        private void prepareMemberFault() {
            try {
                var resource = resources().get(0);
                var binding = route.requireActiveResource(resource).toCompletableFuture().get(10, TimeUnit.SECONDS);
                memberKey = binding.authorityRoot() + "/v2/physical-members-m5-v2/" + resource.sha256().toHex() + "/inventory-v2";
                var stored = actualOxia.get(memberKey).get(10, TimeUnit.SECONDS);
                memberBytes = CanonicalBytes.copyOf(stored.value());
                memberVersion = stored.version().versionId();
                if (mode.equals("MEMBER_MISSING"))
                    actualOxia.delete(memberKey, Set.of(io.oxia.client.api.options.DeleteOption.IfVersionIdEquals(memberVersion)))
                            .get(10, TimeUnit.SECONDS);
            } catch (Exception failure) {
                throw new java.util.concurrent.CompletionException(failure);
            }
        }

        private Object faultMemberRead(AsyncOxiaClient actual, java.lang.reflect.Method method, Object[] args) throws Throwable {
            return ((CompletionStage<?>) invoke(actual, method, args)).thenCompose(value -> {
                if (mode.equals("MEMBER_UNKNOWN")) return CompletableFuture.failedFuture(
                        new java.util.concurrent.TimeoutException("native source member read unavailable"));
                return actual.put(memberKey, memberBytes.toByteArray(),
                        Set.of(io.oxia.client.api.options.PutOption.IfVersionIdEquals(memberVersion))).thenApply(ignored -> value);
            }).toCompletableFuture();
        }

        private void verifyMemberStorage() throws Exception {
            var stored = actualOxia.get(memberKey).get(10, TimeUnit.SECONDS);
            if (mode.equals("MEMBER_MISSING")) {
                assertEquals(null, stored, "source publication recreated the missing native member record");
                actualOxia.put(memberKey, memberBytes.toByteArray(), Set.of(io.oxia.client.api.options.PutOption.IfRecordDoesNotExist))
                        .get(10, TimeUnit.SECONDS);
                return;
            }
            assertEquals(memberBytes, CanonicalBytes.copyOf(stored.value()));
            if (mode.equals("MEMBER_REFRESH"))
                assertTrue(stored.version().versionId() != memberVersion, "actual same-value member CAS did not advance its version");
            else assertEquals(memberVersion, stored.version().versionId());
        }

        private CompletionStage<Void> fenceTarget(PhysicalResourceIdV2 resource) {
            fenced = resource;
            return route.read(resource.authorityKey()).thenCompose(found -> {
                var before = found.orElseThrow();
                var current = M5TargetDeleteAuthorityCodecV1.decodeAuthority(before.canonicalStoredBytes());
                try {
                    // The fixture supplies semantic eligibility only. Both successor values are real Oxia CAS.
                    var fixture = Class.forName("com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2");
                    BiFunction<String, CanonicalBytes, com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1> facts =
                            (key, bytes) -> new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1(key,
                                    new com.nereusstream.metadata.spi.model.MetadataVersion(CanonicalBytes.copyOf(new byte[] {1})),
                                    Sha256Digest.hash(bytes));
                    var eligibility = (com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2) fixture
                            .getMethod("replacement", PhysicalResourceIdV2.class, long.class, BiFunction.class)
                            .invoke(null, resource, current.authorityRevision() + 1, facts);
                    var qualified = M5TargetDeleteAuthorityStateMachineV1.qualifyEligibility(current, eligibility);
                    var template = (com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1)
                            ((List<?>) fixture.getMethod("phases", PhysicalResourceIdV2.class).invoke(null, resource)).get(1);
                    return route.compareAndSet(Optional.of(before), before.key(), M5TargetDeleteAuthorityCodecV1.encodeAuthority(qualified))
                            .thenCompose(outcome -> {
                                assertEquals(com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1
                                        .MutationOutcome.APPLIED_EXACT, outcome);
                                return route.read(before.key());
                            }).thenCompose(observed -> {
                                var exact = observed.orElseThrow();
                                assertEquals(M5TargetDeleteAuthorityCodecV1.encodeAuthority(qualified), exact.canonicalStoredBytes());
                                var candidate = M5TargetDeleteAuthorityStateMachineV1.prepareIdentityRead(qualified,
                                        digest("negative-source-fence-fixture"), template.readFence().orElseThrow().observationContext());
                                return route.compareAndSet(Optional.of(exact), exact.key(),
                                        M5TargetDeleteAuthorityCodecV1.encodeAuthority(candidate));
                            }).thenApply(outcome -> {
                                assertEquals(com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1
                                        .MutationOutcome.APPLIED_EXACT, outcome);
                                return null;
                            });
                } catch (ReflectiveOperationException failure) {
                    return CompletableFuture.failedFuture(failure);
                }
            });
        }

        private List<PhysicalResourceIdV2> resources() {
            final PhysicalResourceIdV2.Namespace namespace;
            try {
                namespace = ((com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2) field(opened, "source"))
                        .spec().namespace();
            } catch (Exception failure) {
                throw new java.util.concurrent.CompletionException(failure);
            }
            var roots = new java.util.ArrayList<>(sources.sealedRoots());
            roots.add(sources.activeRoot());
            return M5TargetDeleteMultiWriterGuardV2.canonicalTargets(roots.stream().map(root ->
                    new PhysicalResourceIdV2.BookKeeperLedger(namespace, root.ledgerIdentity().ledgerId())).toList());
        }

        private List<Sha256Digest> operations(PhysicalResourceIdV2 resource) throws Exception {
            var exact = route.read(resource.authorityKey()).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
            return M5TargetDeleteAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes()).activeWriterTickets().stream()
                    .filter(value -> value.writerClass() == ProofBoundWriterClassV1.REFERENCE_SHARED_PHYSICAL_MEMBER_V1)
                    .map(ProofBoundWriterTicketV1::operationIdSha256).toList();
        }

        private void verifyTickets() throws Exception {
            if (mode.equals("FENCED")) fenceInstalled.get(10, TimeUnit.SECONDS);
            assertTrue(resources().size() >= 3, "source admission did not include the complete real run set");
            if (membershipFailure()) {
                assertTrue(acquired.isEmpty(), "membership rejection acquired source-publication tickets");
                assertEquals(null, ticket.get());
            }
            boolean noPublication = mode.equals("FENCED") || membershipFailure();
            assertEquals(noPublication ? 0 : 1, publications.get());
            for (var resource : resources()) {
                assertEquals(noPublication ? List.of() : List.of(ticket.get().operationIdSha256()), operations(resource));
                var state = M5TargetDeleteAuthorityCodecV1.decodeAuthority(route.read(resource.authorityKey())
                        .toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow().canonicalStoredBytes()).state();
                assertEquals(resource.equals(fenced) ? "READ_FENCED_V1" : "OPEN_V1", state.name());
            }
        }

        private void verifyCleanupRetry() throws Exception {
            var original = ticket.get();
            var context = new M5TargetDeleteMultiWriterGuardV2.Context(original.writerClass(), original.capabilitySha256(),
                    original.ownerFenceSha256(), original.externalFactsRootSha256());
            var retained = (Set<?>) field(opened, mode.equals("UNKNOWN") ? "failedSourcePublications" : "sourceTicketCleanup");
            assertEquals(1, retained.size());
            var cleanup = retained.iterator().next();
            assertEquals(resources(), field(cleanup, "resources"));
            assertEquals(context, field(cleanup, "context"));
            assertEquals(original.operationIdSha256(), field(cleanup, "operationId"));
            var guard = new M5TargetDeleteMultiWriterGuardV2(new M5TargetDeleteAuthorityCoordinatorV1(route));
            var siblingTerminal = new CompletableFuture<M5TargetDeleteMultiWriterGuardV2.Completion<Boolean>>();
            var siblingAdmitted = new CompletableFuture<Void>();
            var sibling = guard.execute(resources(), context, () -> {
                siblingAdmitted.complete(null);
                return siblingTerminal;
            }).toCompletableFuture();
            siblingAdmitted.get(10, TimeUnit.SECONDS);
            var siblingId = operations(resources().get(0)).stream()
                    .filter(operation -> !operation.equals(original.operationIdSha256())).findFirst().orElseThrow();
            for (var resource : resources())
                assertEquals(Set.of(original.operationIdSha256(), siblingId), Set.copyOf(operations(resource)));
            int before = nativeCalls.get();
            blockCleanup.set(false);
            blockSelectorReads.set(false);
            opened.closeAsync().get(10, TimeUnit.SECONDS);
            assertEquals(before, nativeCalls.get(), "metadata retry redispatched a native reader");
            assertEquals(1, publications.get(), "metadata retry republished the selector");
            assertTrue(retained.isEmpty());
            for (var resource : resources()) assertEquals(List.of(siblingId), operations(resource));
            siblingTerminal.complete(new M5TargetDeleteMultiWriterGuardV2.Completion<>(true, Optional.of(digest("known-no-op-sibling"))));
            assertTrue(sibling.get(10, TimeUnit.SECONDS).unresolvedTargets().isEmpty());
        }

        private void verifyReferences() throws Exception {
            var authority = (KafkaBookKeeperOwnerAuthorityV1) field(opened.storage(), "ownerAuthority");
            var roots = new java.util.ArrayList<>(sources.sealedRoots());
            roots.add(sources.activeRoot());
            for (var root : roots) {
                var closed = authority.readClosedOwner(root.creatorOwnerEpoch()).toCompletableFuture()
                        .get(10, TimeUnit.SECONDS).orElseThrow();
                var link = closed.runs().stream().filter(value -> value.runId().equals(root.runId())).findFirst().orElseThrow();
                var record = authority.readAdmittedRun(link).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
                assertTrue(!record.retired(), "failed source admission retired an admitted run");
                assertEquals(root.ledgerIdentity(), record.root().ledgerIdentity());
                assertTrue(resources().contains(record.resource()));
                route.requireActiveResource(record.resource()).toCompletableFuture().get(10, TimeUnit.SECONDS);
            }
        }

        private void verifySelector() throws Exception {
            assertEquals(null, field(opened.storage(), "m4ReadOwner"), "failed publication installed a local read callback");
            var exact = actualOxia.get(selectorKey).get(10, TimeUnit.SECONDS);
            var durable = com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1
                    .decodeAuthority(CanonicalBytes.copyOf(exact.value())).selectorProjection();
            if (mode.equals("FENCED") || membershipFailure()) {
                assertTrue(durable.ownerEpoch() < sources.activeRoot().creatorOwnerEpoch());
                assertTrue(!durable.selectedViewSha256().equals(sources.selectedViewSha256()));
            } else {
                assertTrue(committed.get());
                assertEquals(sources.activeRoot().creatorOwnerEpoch(), durable.ownerEpoch());
                assertEquals(sources.selectedViewSha256(), durable.selectedViewSha256());
                blockSelectorReads.set(false);
                assertEquals(exact, selectorClient(actualOxia).get(selectorKey).get(10, TimeUnit.SECONDS),
                        "restored exact reread did not confirm the real committed selector");
            }
        }
    }

    private static Object field(Object target, String name) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static boolean containsFailure(Throwable failure, String text) {
        if (failure == null || text == null) return false;
        if (failure.getMessage() != null && failure.getMessage().contains(text)) return true;
        if (containsFailure(failure.getCause(), text)) return true;
        for (var suppressed : failure.getSuppressed()) if (containsFailure(suppressed, text)) return true;
        return false;
    }

    private static Object invoke(Object delegate, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(delegate, args);
        } catch (java.lang.reflect.InvocationTargetException failure) {
            throw failure.getCause();
        }
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }
}
