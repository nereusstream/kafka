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
import kafka.server.storage.BrokerStorageRuntimeFactory;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.nereus.KafkaTopicBindingAggregateMapperV1;
import org.apache.kafka.server.common.MetadataVersion;

import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperRunBirthRecoveryV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.object.gc.BoundPhysicalDeleteAuthorityRouteV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;

import java.nio.file.Files;
import java.nio.file.Path;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A lost BK create callback leaves one protected physical birth while two successor Brokers activate. */
public final class KafkaBookKeeperCreateResponseLossRealScenario {
    private KafkaBookKeeperCreateResponseLossRealScenario() { }

    public static void run(Optional<Path> m4Summary) throws Exception {
        run(m4Summary, false, false, new ReaderCloseFault("NONE"));
    }

    public static void runTicketReleaseResponseLoss(Optional<Path> m4Summary) throws Exception {
        run(m4Summary, true, false, new ReaderCloseFault("NONE"));
    }

    public static void runRecoveredFactsResponseLoss(Optional<Path> m4Summary) throws Exception {
        run(m4Summary, false, true, new ReaderCloseFault("TASK_RACE"));
    }

    public static void runReaderCloseResponseLoss(Optional<Path> m4Summary, String mode) throws Exception {
        run(m4Summary, false, false, new ReaderCloseFault(mode));
    }

    private static void run(Optional<Path> m4Summary, boolean loseTicketRelease, boolean loseNativeFacts,
                            ReaderCloseFault readerFault)
            throws Exception {
        String uri = System.getProperty("nereus.nsip.bkUri");
        String oxia = System.getProperty("nereus.nsip.oxia");
        if (uri == null || oxia == null) throw new IllegalStateException("real BK URI and Oxia address are required");
        Path inputs = Files.createTempDirectory("nereus-nsip-create-response-");
        var releaseOldCreate = new CompletableFuture<ProviderMutationResultV1<RunLedgerHandleV1>>();
        try {
            KafkaBookKeeperOwnerRelocationRealTest.writeCapability(inputs, m4Summary);
            var nodes = responseLossNodes();
            var created = new AtomicReference<RunLedgerHandleV1>();
            var heldCreate = new AtomicReference<ProviderMutationResultV1<RunLedgerHandleV1>>();
            var failReleaseRead = new AtomicBoolean();
            var lost = new CountDownLatch(1);
            var completedRecovery = new CountDownLatch[] {
                new CountDownLatch(1), new CountDownLatch(1), new CountDownLatch(1)
            };
            var recoveryFailures = new AtomicReferenceArray<Throwable>(3);
            BrokerStorageRuntimeFactory factory = context -> {
                var provider = new NereusKafkaOwnedProviderRuntime(context, responses(context.config().brokerId(),
                        loseTicketRelease, loseNativeFacts, readerFault, created, heldCreate, failReleaseRead, lost,
                        completedRecovery, recoveryFailures, releaseOldCreate));
                readerFault.ownProvider(context.config().brokerId(), provider);
                return new NereusBrokerStorageRuntime(context, provider);
            };
            var builder = new KafkaClusterTestKit.Builder(nodes)
                    .setBrokerStorageRuntimeFactory(factory)
                    .setControllerStorageRuntimeFactory(new NereusControllerStorageRuntimeFactory());
            KafkaBookKeeperOwnerRelocationRealTest.nativeConfig(inputs, uri, oxia).forEach(builder::setConfigProp);
            readerFault.configure(builder);
            boolean verified = false;
            boolean unknownCloseReported = false;
            try (KafkaClusterTestKit cluster = builder.build()) {
                cluster.format();
                cluster.startup();
                cluster.waitForReadyBrokers();
                var client = clientProperties(cluster);
                try (Admin admin = Admin.create(client)) {
                    var partition = new TopicPartition("native-cold", 0);
                    admin.createTopics(List.of(new NewTopic("native-cold", Map.of(0, List.of(0)))))
                            .all()
                            .get(30, TimeUnit.SECONDS);
                    assertTrue(lost.await(30, TimeUnit.SECONDS), "actual reserved ledger create did not finish");
                    verifyInitialFailure(loseTicketRelease, completedRecovery[0], failReleaseRead, recoveryFailures);
                    var topicId = admin.describeTopics(List.of("native-cold"))
                            .allTopicNames().get(30, TimeUnit.SECONDS).get("native-cold").topicId();
                    var brokerZero = cluster.brokers().get(0);
                    var aggregate = KafkaTopicBindingAggregateMapperV1.toRecord(brokerZero.metadataCache()
                            .getImage().topics().getTopic(topicId).nereusAggregate().orElseThrow());
                    try (var probe = new NereusClosedOwnerProbe(
                            brokerZero.config(), topicId, partition, aggregate, 1)) {
                        assertEquals(1, probe.owner().owner().ownerEpoch());
                        assertFalse(probe.owner().closed());
                        assertTrue(probe.owner().runs().isEmpty(),
                                "an unresolved create ticket must not select a physical run");
                        assertEquals(1, probe.activeCreateTickets(created.get()),
                                "uncertain create completion must retain its writer ticket before owner fencing");
                        reassign(admin, partition, 1);
                        assertEquals(1, KafkaBookKeeperOwnerRelocationRealTest.awaitLeader(admin, 0));
                        assertTrue(completedRecovery[1].await(30, TimeUnit.SECONDS),
                                "first successor did not finish native recovery");
                        var firstValues = verifyFirstTakeover(
                                probe, created.get(), recoveryFailures.get(1), client, loseNativeFacts, readerFault);
                        reassign(admin, partition, 2);
                        assertEquals(2, KafkaBookKeeperOwnerRelocationRealTest.awaitLeader(admin, 1));
                        assertTrue(completedRecovery[2].await(30, TimeUnit.SECONDS),
                                "second successor did not finish native recovery");
                        assertNull(recoveryFailures.get(2), "second successor could not activate past the orphan");
                        assertUnselectedPhysicalBirth(probe, created.get());
                        assertEquals(0, probe.activeCreateTickets(created.get()));
                        assertTrue(probe.retainedOpenQuota(created.get()));
                        produce(client, "after-second-takeover", firstValues.size());
                        var values = new java.util.ArrayList<>(firstValues);
                        values.add("after-second-takeover");
                        assertEquals(values, KafkaBookKeeperOwnerRelocationRealTest.consume(client, values.size()));
                        verifyDelayedCreate(loseTicketRelease, releaseOldCreate, heldCreate.get(),
                                completedRecovery[0], recoveryFailures, probe, created.get());
                        readerFault.verifyTakeovers(admin, partition, probe, created.get());
                    }
                } finally {
                    releaseOldCreate.complete(ProviderMutationResultV1.outcomeUnknown());
                    readerFault.releaseHeldResponses();
                }
                verified = true;
            } catch (org.apache.kafka.server.fault.FaultHandlerException expected) {
                if (!verified) throw expected;
                assertKnownCloseFailure(expected, loseNativeFacts, readerFault);
            } catch (java.util.concurrent.ExecutionException expected) {
                if (!verified || !readerFault.unknown()) throw expected;
                assertFailureContains(expected, "native recovery reader close response lost");
                unknownCloseReported = true;
            }
            readerFault.verifyShutdown(unknownCloseReported);
        } finally {
            releaseOldCreate.complete(ProviderMutationResultV1.outcomeUnknown());
            readerFault.releaseHeldResponses();
            removeInputs(inputs);
        }
    }

    private static NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver responses(
            int broker, boolean loseTicketRelease, boolean loseNativeFacts, ReaderCloseFault readerFault,
            AtomicReference<RunLedgerHandleV1> created,
            AtomicReference<ProviderMutationResultV1<RunLedgerHandleV1>> heldCreate,
            AtomicBoolean failReleaseRead, CountDownLatch lost, CountDownLatch[] completedRecovery,
            AtomicReferenceArray<Throwable> recoveryFailures,
            CompletableFuture<ProviderMutationResultV1<RunLedgerHandleV1>> releaseOldCreate) {
        return new NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver() {
            @Override
            public BoundPhysicalDeleteAuthorityRouteV2 writerAuthorityRoute(
                    BoundPhysicalDeleteAuthorityRouteV2 exactRoute) {
                return readerFault.authorityRoute(broker, loseTicketRelease && broker == 0
                        ? failOneWriterAuthorityRead(exactRoute, failReleaseRead) : exactRoute);
            }

            @Override
            public void afterRecovery(org.apache.kafka.common.TopicIdPartition identity, Throwable failure) {
                readerFault.afterRecovery(broker, failure);
                if (identity.topicPartition().equals(new TopicPartition("native-cold", 0))
                        && recoveryFailures.compareAndSet(broker, null, failure)) {
                    completedRecovery[broker].countDown();
                }
            }

            @Override
            public CompletionStage<ProviderMutationResultV1<RunLedgerHandleV1>> afterCreate(
                    CompletionStage<ProviderMutationResultV1<RunLedgerHandleV1>> response) {
                if (broker != 0) return response;
                return response.thenCompose(result -> {
                    if (result.outcome() == ProviderMutationOutcomeV1.APPLIED_EXACT) {
                        created.compareAndSet(null, result.exactProof().orElseThrow());
                        if (loseTicketRelease) failReleaseRead.set(true);
                        if (!loseTicketRelease) {
                            heldCreate.set(result);
                            lost.countDown();
                            return releaseOldCreate;
                        }
                        lost.countDown();
                    }
                    return CompletableFuture.completedFuture(result);
                });
            }

            @Override
            public CompletionStage<M5BookKeeperNativeCreateClientV2.RecoveredScopeFacts> afterRecoveredScopeFacts(
                    CompletionStage<M5BookKeeperNativeCreateClientV2.RecoveredScopeFacts> response) {
                if (!loseNativeFacts || broker != 1) return response;
                return response.thenCompose(facts -> {
                    assertEquals(created.get(), facts.recovery().handle());
                    assertEquals(-1, facts.recovery().lastAddConfirmed());
                    return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException(
                            "post-recovery native fact read response lost"));
                });
            }

            @Override
            public CompletionStage<Void> afterBirthReaderClose(CompletionStage<Void> response) {
                return readerFault.afterClose(broker, response);
            }
            @Override
            public BookKeeperCellSession birthReaderSession(BookKeeperCellSession session) {
                return readerFault.trackSession(broker, session);
            }

            @Override
            public void afterBirthReaderTicketCleanup(Throwable failure) {
                readerFault.afterTicketCleanup(broker, failure);
            }
        };
    }

    private static void removeInputs(Path inputs) throws Exception {
        try (var files = Files.walk(inputs)) {
            for (var file : files.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(file);
        }
    }

    private static TestKitNodes responseLossNodes() {
        var bootstrap = BootstrapMetadata.fromVersion(MetadataVersion.latestTesting(), "nsip-create-response")
                .copyWithFeatureRecord("nereus.storage.version", (short) 2);
        return new TestKitNodes.Builder(bootstrap).setNumBrokerNodes(3).setNumControllerNodes(1).build();
    }

    private static Properties clientProperties(KafkaClusterTestKit cluster) {
        var client = new Properties();
        client.put("bootstrap.servers", cluster.bootstrapServers());
        return client;
    }

    private static List<String> verifyFirstTakeover(
            NereusClosedOwnerProbe probe, RunLedgerHandleV1 created, Throwable failure,
            Properties client, boolean loseNativeFacts, ReaderCloseFault readerFault) throws Exception {
        awaitClosed(probe);
        assertUnselectedPhysicalBirth(probe, created);
        assertTrue(probe.retainedOpenQuota(created), "orphan must retain OPEN and its permanent quota reservation");
        if (loseNativeFacts) {
            assertFailureContains(failure, "post-recovery native fact read response lost");
            assertEquals(1, probe.activeCreateTickets(created),
                    "unresolved native fact read must retain the old writer ticket after actual BK recovery");
            return List.of();
        }
        assertNull(failure, "first successor could not activate past the orphan");
        assertEquals(0, probe.activeCreateTickets(created),
                "exact native task fence, recovery and native fact read must reconcile the create ticket");
        readerFault.verifyFirstPin(probe, created);
        if (readerFault.capacity()) return List.of();
        produce(client, "after-first-takeover", 0);
        var values = List.of("after-first-takeover");
        assertEquals(values, KafkaBookKeeperOwnerRelocationRealTest.consume(client, 1));
        return values;
    }

    private static void verifyInitialFailure(boolean loseTicketRelease, CountDownLatch completedRecovery,
                                             AtomicBoolean failReleaseRead, AtomicReferenceArray<Throwable> failures)
            throws Exception {
        if (!loseTicketRelease) return;
        assertTrue(completedRecovery.await(30, TimeUnit.SECONDS), "initial Broker did not finish its failed native open");
        assertFalse(failReleaseRead.get(), "fault must consume the post-create authority reread");
        assertFailureContains(failures.get(0), "run-ledger creation was not established exactly");
    }

    private static void verifyDelayedCreate(
            boolean loseTicketRelease,
            CompletableFuture<ProviderMutationResultV1<RunLedgerHandleV1>> release,
            ProviderMutationResultV1<RunLedgerHandleV1> held,
            CountDownLatch completedRecovery,
            AtomicReferenceArray<Throwable> recoveryFailures,
            NereusClosedOwnerProbe probe, RunLedgerHandleV1 created) throws Exception {
        if (loseTicketRelease) return;
        assertTrue(release.complete(held));
        assertTrue(completedRecovery.await(30, TimeUnit.SECONDS),
                "old Broker did not finish after its delayed create callback");
        assertNotNull(recoveryFailures.get(0), "old Broker must not activate after two successor takeovers");
        assertUnselectedPhysicalBirth(probe, created);
        assertTrue(probe.retainedOpenQuota(created));
    }

    private static void assertKnownCloseFailure(
            org.apache.kafka.server.fault.FaultHandlerException failure, boolean loseNativeFacts,
            ReaderCloseFault readerFault) {
        assertTrue(failure.getMessage().contains("run-ledger creation was not established exactly")
                        || (loseNativeFacts && failure.getMessage().contains("post-recovery native fact read response lost"))
                        || readerFault.knownFailure(failure.getMessage()),
                "cluster close reported a different native failure");
    }

    private static final class ReaderCloseFault {
        private final String mode;
        private final CompletableFuture<Void> release = new CompletableFuture<>();
        private final AtomicInteger realCloses = new AtomicInteger();
        private final CountDownLatch capacityBlocked = new CountDownLatch(1);
        private final AtomicReference<Throwable> capacityFailure = new AtomicReference<>();
        private final CompletableFuture<Void> secondClose = new CompletableFuture<>();
        private final CompletableFuture<Void> metadataRetry = new CompletableFuture<>();
        private final CountDownLatch failedCleanup = new CountDownLatch(1);
        private final CountDownLatch retryEntered = new CountDownLatch(1);
        private final CountDownLatch ticketsConfirmed = new CountDownLatch(1);
        private final AtomicReference<Throwable> cleanupFailure = new AtomicReference<>();
        private final AtomicReference<String> targetKey = new AtomicReference<>();
        private final AtomicReference<Sha256Digest> readerOperation = new AtomicReference<>();
        private final AtomicBoolean failRead = new AtomicBoolean();
        private final AtomicBoolean loseCasResponse = new AtomicBoolean();
        private final AtomicBoolean blockRetry = new AtomicBoolean();
        private final java.util.concurrent.atomic.AtomicIntegerArray nativeCalls =
                new java.util.concurrent.atomic.AtomicIntegerArray(3);
        private NereusKafkaOwnedProviderRuntime provider;
        private final CompletableFuture<Void> taskReaderCloseResponse = new CompletableFuture<>();
        private final CompletableFuture<Void> taskReaderCleanupDone = new CompletableFuture<>();
        private final AtomicBoolean conflictTaskOnce = new AtomicBoolean(true);
        private final CountDownLatch taskCasConflicted = new CountDownLatch(1);

        private ReaderCloseFault(String mode) {
            this.mode = mode;
        }

        private boolean capacity() {
            return mode.equals("CAPACITY") || mode.equals("UNKNOWN_CAPACITY");
        }

        private boolean unknown() {
            return mode.equals("UNKNOWN") || mode.equals("UNKNOWN_CAPACITY");
        }

        private boolean taskRace() {
            return mode.equals("TASK_RACE");
        }

        private boolean metadataCleanup() {
            return mode.equals("CLEANUP_READ") || mode.equals("CLEANUP_LOST_REPLY");
        }

        private void ownProvider(int broker, NereusKafkaOwnedProviderRuntime owned) {
            if (broker == (taskRace() ? 2 : 1)) provider = owned;
        }

        private BookKeeperCellSession trackSession(int broker, BookKeeperCellSession delegate) {
            if (!(metadataCleanup() && broker == 1) && !(taskRace() && broker == 2)) return delegate;
            return (BookKeeperCellSession) Proxy.newProxyInstance(BookKeeperCellSession.class.getClassLoader(),
                    new Class<?>[] {BookKeeperCellSession.class}, (proxy, method, args) -> {
                        int counter = switch (method.getName()) {
                            case "openRunLedger" -> 0;
                            case "fenceAndRecoverRunLedger" -> 1;
                            case "closeAsync" -> 2;
                            default -> -1;
                        };
                        if (counter >= 0) nativeCalls.incrementAndGet(counter);
                        try {
                            return method.invoke(delegate, args);
                        } catch (java.lang.reflect.InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                    });
        }

        private boolean controlsAuthorityRoute(int broker) {
            return (metadataCleanup() && broker == 1) || (taskRace() && broker == 2);
        }

        private BoundPhysicalDeleteAuthorityRouteV2 authorityRoute(int broker, BoundPhysicalDeleteAuthorityRouteV2 delegate) {
            if (!controlsAuthorityRoute(broker)) return delegate;
            return (BoundPhysicalDeleteAuthorityRouteV2) Proxy.newProxyInstance(
                    BoundPhysicalDeleteAuthorityRouteV2.class.getClassLoader(),
                    new Class<?>[] {BoundPhysicalDeleteAuthorityRouteV2.class}, (proxy, method, args) -> {
                        if (method.getName().equals("read") && args[0].equals(targetKey.get())) {
                            if (failRead.compareAndSet(true, false)) {
                                blockRetry.set(true);
                                return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException(
                                        "closed reader ticket cleanup read response lost"));
                            }
                            if (blockRetry.get() && !metadataRetry.isDone()) {
                                retryEntered.countDown();
                                return metadataRetry.thenCompose(ignored -> delegate.read((String) args[0]));
                            }
                        }
                        if (method.getName().equals("compareAndSet")) {
                            var next = M5TargetDeleteAuthorityCodecV1.decodeAuthority((CanonicalBytes) args[2]);
                            var taskConflict = raceTaskCleanup(delegate, method, args, next);
                            if (taskConflict != null) return taskConflict;
                            next.activeWriterTickets().stream()
                                    .filter(ticket -> ticket.writerClass() == ProofBoundWriterClassV1.OWNER_WORKER_LEASE_HANDLE_PIN_V1)
                                    .findFirst().ifPresent(ticket -> {
                                        targetKey.compareAndSet(null, (String) args[1]);
                                        readerOperation.compareAndSet(null, ticket.operationIdSha256());
                                    });
                            if (args[1].equals(targetKey.get())
                                    && next.activeWriterTickets().stream().noneMatch(ticket ->
                                            ticket.operationIdSha256().equals(readerOperation.get()))
                                    && loseCasResponse.compareAndSet(true, false)) {
                                var actual = (CompletionStage<?>) method.invoke(delegate, args);
                                return actual.thenCompose(ignored -> {
                                    failRead.set(true);
                                    return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException(
                                            "closed reader ticket cleanup CAS response lost"));
                                });
                            }
                        }
                        try {
                            return method.invoke(delegate, args);
                        } catch (java.lang.reflect.InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                    });
        }

        private CompletionStage<?> raceTaskCleanup(
                BoundPhysicalDeleteAuthorityRouteV2 delegate, java.lang.reflect.Method method, Object[] args,
                com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1 next) {
            if (!taskRace()
                    || next.activeWriterTickets().stream().anyMatch(ticket -> ticket.writerClass()
                            == ProofBoundWriterClassV1.TASK_PROJECTION_MIGRATION_RECOVERY_V1)
                    || next.activeWriterTickets().stream().noneMatch(ticket -> ticket.writerClass()
                            == ProofBoundWriterClassV1.OWNER_WORKER_LEASE_HANDLE_PIN_V1)
                    || !conflictTaskOnce.compareAndSet(true, false)) return null;
            assertTrue(taskReaderCloseResponse.complete(null));
            // Preserve the exact intercepted CAS arguments while real Oxia first removes the reader pin.
            return taskReaderCleanupDone.thenCompose(ignored -> {
                try {
                    var actual = (CompletionStage<?>) method.invoke(delegate, args);
                    return actual.thenApply(outcome -> {
                        assertEquals("DEFINITIVE_CONFLICT", outcome.toString());
                        taskCasConflicted.countDown();
                        return outcome;
                    });
                } catch (java.lang.reflect.InvocationTargetException failure) {
                    return CompletableFuture.failedFuture(failure.getCause());
                } catch (ReflectiveOperationException failure) {
                    return CompletableFuture.failedFuture(failure);
                }
            });
        }

        private void afterTicketCleanup(int broker, Throwable failure) {
            if (taskRace() && broker == 2) {
                if (failure == null) taskReaderCleanupDone.complete(null);
                else taskReaderCleanupDone.completeExceptionally(failure);
                return;
            }
            if (!metadataCleanup() || broker != 1) return;
            if (failure != null) {
                cleanupFailure.set(failure);
                failedCleanup.countDown();
            } else {
                ticketsConfirmed.countDown();
            }
        }

        private void configure(KafkaClusterTestKit.Builder builder) {
            // Two retained recovery handles plus one transient publication header; a new recovery still cannot fit.
            if (capacity()) builder.setConfigProp("nereus.kafka.storage.bookkeeper.max.reads.inflight", "3");
        }

        private CompletionStage<Void> afterClose(int broker, CompletionStage<Void> actual) {
            if (taskRace()) return broker == 2 ? actual.thenCompose(ignored -> taskReaderCloseResponse) : actual;
            if (metadataCleanup() && broker == 2) return actual.thenCompose(ignored -> secondClose);
            if (mode.equals("NONE") || broker != 1) return actual;
            return actual.thenCompose(ignored -> {
                realCloses.incrementAndGet();
                return unknown()
                        ? CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException(
                                "native recovery reader close response lost"))
                        : release;
            });
        }

        private void afterRecovery(int broker, Throwable failure) {
            if (capacity() && broker == 1 && failure != null) {
                capacityFailure.set(failure);
                capacityBlocked.countDown();
            }
        }

        private void verifyFirstPin(NereusClosedOwnerProbe probe, RunLedgerHandleV1 created) throws Exception {
            if (mode.equals("NONE")) return;
            assertEquals(1, realCloses.get(), "fault must observe a completed real native session close");
            assertEquals(1, probe.activeReaderTickets(created),
                    "CLOSED metadata and terminal old TASK must not discharge the current recovery reader");
            if (metadataCleanup()) {
                if (mode.equals("CLEANUP_READ")) failRead.set(true);
                else loseCasResponse.set(true);
                assertTrue(release.complete(null));
                assertTrue(failedCleanup.await(30, TimeUnit.SECONDS), "confirmed native close did not retain its cleanup failure");
                Throwable failure = cleanupFailure.get();
                while (failure instanceof java.util.concurrent.CompletionException) failure = failure.getCause();
                assertTrue(failure instanceof NereusKafkaRunBirthWriterAdmissionV1.TicketCleanupException);
                assertEquals(readerOperation.get(),
                        ((NereusKafkaRunBirthWriterAdmissionV1.TicketCleanupException) failure).operationId());
                assertEquals(mode.equals("CLEANUP_READ") ? 1 : 0, probe.activeReaderTickets(created));
            }
        }

        private void verifyTakeovers(Admin admin, TopicPartition partition, NereusClosedOwnerProbe probe,
                                     RunLedgerHandleV1 created) throws Exception {
            if (mode.equals("NONE")) return;
            if (taskRace()) {
                assertTrue(taskCasConflicted.await(30, TimeUnit.SECONDS), "actual reader release did not conflict with TASK cleanup");
                assertNativeCallsOnce();
                assertEquals(0, probe.activeReaderTickets(created));
                assertEquals(0, probe.activeCreateTickets(created));
                provider.close();
                assertNativeCallsOnce();
                return;
            }
            if (metadataCleanup()) {
                verifyMetadataRetry(probe, created);
                return;
            }
            assertEquals(1, probe.activeReaderTickets(created),
                    "a later owner's exact read cleanup and late old create callback must preserve the earlier pin");
            if (capacity()) {
                reassign(admin, partition, 1);
                assertEquals(1, KafkaBookKeeperOwnerRelocationRealTest.awaitLeader(admin, 2));
                assertTrue(capacityBlocked.await(30, TimeUnit.SECONDS), "held cleanup did not bound the next recovery");
                assertFailureContains(capacityFailure.get(), "native run birth Cell read capacity is full");
                assertEquals(1, realCloses.get(), "capacity rejection must precede another native reader allocation");
                assertEquals(1, probe.activeReaderTickets(created));
            }
            if (!unknown()) {
                assertTrue(release.complete(null));
                awaitReaderPins(probe, created, 0);
            }
        }

        private void verifyMetadataRetry(NereusClosedOwnerProbe probe, RunLedgerHandleV1 created) throws Exception {
            assertTrue(retryEntered.await(30, TimeUnit.SECONDS), "actual retiring owner did not consume metadata-only retry");
            assertEquals(mode.equals("CLEANUP_READ") ? 2 : 1, probe.activeReaderTickets(created),
                    "failed cleanup and the later invocation must remain distinct");
            assertNativeCallsOnce();
            assertTrue(metadataRetry.complete(null));
            assertTrue(ticketsConfirmed.await(30, TimeUnit.SECONDS), "original reader invocation was not confirmed terminal");
            assertEquals(1, probe.activeReaderTickets(created), "metadata retry removed another owner's reader pin");
            provider.close();
            assertNativeCallsOnce();
            assertTrue(secondClose.complete(null));
            awaitReaderPins(probe, created, 0);
        }

        private void assertNativeCallsOnce() {
            for (int index = 0; index < 3; index++)
                assertEquals(1, nativeCalls.get(index), "metadata-only retry must not open/recover/close native session again");
        }

        private void releaseHeldResponses() {
            release.complete(null);
            secondClose.complete(null);
            metadataRetry.complete(null);
            taskReaderCloseResponse.complete(null);
        }

        private boolean knownFailure(String message) {
            return message.contains("native recovery reader close response lost")
                    || (capacity() && message.contains("native run birth Cell read capacity is full"));
        }

        private void verifyShutdown(boolean unknownCloseReported) {
            if (unknown())
                assertTrue(unknownCloseReported, "an unconfirmed reader close must prevent successful source shutdown");
        }
    }

    private static void awaitReaderPins(NereusClosedOwnerProbe probe, RunLedgerHandleV1 handle, long expected)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            if (probe.activeReaderTickets(handle) == expected) return;
            Thread.sleep(25);
        }
        assertEquals(expected, probe.activeReaderTickets(handle), "exact reader invocation did not settle its pin");
    }

    /** One lost read after the real BK create and durable ticket; all other calls reach real Oxia. */
    private static BoundPhysicalDeleteAuthorityRouteV2 failOneWriterAuthorityRead(
            BoundPhysicalDeleteAuthorityRouteV2 delegate, AtomicBoolean armed) {
        return (BoundPhysicalDeleteAuthorityRouteV2) Proxy.newProxyInstance(
                BoundPhysicalDeleteAuthorityRouteV2.class.getClassLoader(),
                new Class<?>[] {BoundPhysicalDeleteAuthorityRouteV2.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("read")
                            && ((String) args[0]).endsWith("/authority-v2")
                            && armed.compareAndSet(true, false)) {
                        return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException(
                                "post-create physical ticket read response lost"));
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (java.lang.reflect.InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private static void reassign(Admin admin, TopicPartition partition, int broker) throws Exception {
        admin.alterPartitionReassignments(Map.of(
                partition, Optional.of(new NewPartitionReassignment(List.of(broker)))))
                .all()
                .get(30, TimeUnit.SECONDS);
    }

    private static void awaitClosed(NereusClosedOwnerProbe probe) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            if (probe.archived(1)) return;
            Thread.sleep(50);
        }
        throw new AssertionError("failed native owner was not durably closed by successor Broker");
    }

    private static void assertUnselectedPhysicalBirth(NereusClosedOwnerProbe probe, RunLedgerHandleV1 created)
            throws Exception {
        assertTrue(probe.archived(1), "orphan takeover must retain the closed owner archive");
        var births = probe.closedBirths(1);
        assertEquals(1, births.size());
        assertEquals(KafkaBookKeeperRunBirthRecoveryV1.Kind.PHYSICALLY_CREATED_UNSELECTED, births.get(0).kind());
        assertEquals(created, births.get(0).handle());
    }

    private static void produce(Properties client, String value, long expectedOffset) throws Exception {
        var producerConfig = new Properties();
        producerConfig.putAll(client);
        producerConfig.put("key.serializer", StringSerializer.class.getName());
        producerConfig.put("value.serializer", StringSerializer.class.getName());
        producerConfig.put("acks", "all");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig)) {
            assertEquals(expectedOffset, producer.send(new ProducerRecord<>("native-cold", 0, "key", value))
                    .get(30, TimeUnit.SECONDS).offset());
        }
    }

    private static void assertFailureContains(Throwable failure, String expected) {
        assertTrue(failure != null, "native recovery unexpectedly succeeded");
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current.toString().contains(expected)) return;
        }
        throw new AssertionError("native recovery failure did not contain " + expected, failure);
    }
}
