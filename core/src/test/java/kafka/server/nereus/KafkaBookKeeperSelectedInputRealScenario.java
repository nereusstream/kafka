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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperReadCellBudgetV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionAdmissionV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Caps;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperDescriptorV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSemanticCompactorV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionWriterV2.VerifiedPart;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperSequentialReadRequestV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadOutcomeV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5TaskSelectionDecisionV2;
import io.oxia.client.api.AsyncOxiaClient;
import org.apache.kafka.test.TestUtils;

import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import static kafka.server.nereus.KafkaBookKeeperNativeInputRealScenario.withCurrentMetadataImage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Real selected-generation plus raw-tail native input and retained Cell-capacity verification. */
final class KafkaBookKeeperSelectedInputRealScenario {
    private KafkaBookKeeperSelectedInputRealScenario() { }

    static KafkaCompactionAdmissionV1.State afterCapture(String mode, KafkaCompactionAdmissionV1.State formal,
            KafkaCompactionAdmissionV1 admission) {
        return mode.equals("TASK_OUTPUT_PUBLISH_RELEASE_CAPTURE") ? admission.read().orElseThrow() : formal;
    }

    static int captureTasks(String mode) {
        return mode.equals("TASK_OUTPUT_PUBLISH_RELEASE_CAPTURE") || mode.startsWith("TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT") ? 2 : 1;
    }

    /** Retry only through the caller's existing bounded loop, after confirmed discard of a known no-Task attempt. */
    static boolean verifyIneligible(String mode, boolean registered, AsyncOxiaClient actual, String selectorKey,
            NereusKafkaNativeInputV2.Captured captured, KafkaCompactionAdmissionV1 admission,
            KafkaCompactionAdmissionV1.State preparation) throws Exception {
        var failure = assertThrows(java.util.concurrent.ExecutionException.class,
                () -> captured.prepareTask(4096, 65536).toCompletableFuture().get(30, TimeUnit.SECONDS));
        if (KafkaBookKeeperRootVerifierRealScenario.releaseKnownNoTask(registered, captured, admission, failure)) return false;
        String setting = mode.equals("TASK_MIN_LAG") ? "min.compaction.lag.ms" : "min.cleanable.dirty.ratio";
        assertTrue(failure.getCause().getMessage().contains(setting), "Task failed before actual eligibility validation: " + failure);
        assertFalse(registered, "ineligible Task dispatched immutable registration");
        assertEquals(preparation, admission.read().orElseThrow(), "eligibility failure replaced or released preparation capacity");
        var authority = M5BindingAuthorityCodecV1.decodeAuthority(
                CanonicalBytes.copyOf(actual.get(selectorKey).get(30, TimeUnit.SECONDS).value()));
        assertTrue(authority.materializationWorker().isEmpty(), "ineligible Task admitted an output worker");
        captured.discard().toCompletableFuture().get(30, TimeUnit.SECONDS);
        assertTrue(admission.read().orElseThrow().reservations().isEmpty());
        return true;
    }

    static boolean releaseRollover(String mode) {
        return mode.equals("TASK_OUTPUT_PUBLISH_RELEASE_ROLLOVER") || captureTasks(mode) == 2;
    }

    static KafkaCompactionAdmissionV1.CellCaps caps(Caps limits, int tasks) {
        return new KafkaCompactionAdmissionV1.CellCaps(tasks, limits.maximumDirtyBytes() * tasks,
                (long) limits.maximumInputBatches() * tasks, (long) limits.maximumRecords() * tasks,
                (long) limits.maximumDistinctKeys() * tasks, limits.maximumKeyBytes() * tasks,
                limits.maximumOutputBytes() * tasks, limits.maximumIndexBytes() * tasks,
                (long) limits.maximumTransactions() * tasks, (long) limits.maximumTombstones() * tasks,
                60000, tasks, 4096L * tasks, tasks, 4096L * tasks, 64L * tasks);
    }

    static void verifyIfSelected(String mode, BindingIdentity binding, KafkaSealedBookKeeperDescriptorV2 descriptor,
            KafkaCompactionAdmissionV1.State formal, KafkaCompactionAdmissionV1 admission,
            NereusKafkaOwnedProviderRuntime.Opened opened, AsyncOxiaClient actual, String root) throws Exception {
        if (mode.equals("TASK_OUTPUT_PUBLISH_RELEASE_CAPTURE")) verify(binding, descriptor, formal, admission, opened, actual, root);
    }

    static void verify(BindingIdentity binding, KafkaSealedBookKeeperDescriptorV2 descriptor,
            KafkaCompactionAdmissionV1.State formal, KafkaCompactionAdmissionV1 admission,
            NereusKafkaOwnedProviderRuntime.Opened opened, AsyncOxiaClient actual, String root) throws Exception {
        var active = opened.storage().nativeReadSources().activeRoot();
        var bounds = KafkaBookKeeperNativeInputRealScenario.BOUNDS;
        var raw = KafkaBookKeeperReadCellBudgetV2.Usage.forRunSource(bounds);
        var share = new KafkaBookKeeperReadCellBudgetV2.Usage(raw.owners(), raw.readSlots(),
                raw.encodedBytes() + 2 * bounds.nativeBytes(), raw.decodedBytes() + 2 * bounds.decodedBytes());
        var budget = new KafkaBookKeeperReadCellBudgetV2(active.providerScopeId(), share, Map.of(binding, share));
        var limits = new Caps(bounds.payloadBytes(), bounds.batches(), bounds.records(), bounds.records(),
                bounds.decodedBytes(), bounds.decodedBytes(), bounds.decodedBytes(), bounds.records(), bounds.records());
        var allowance = new NereusKafkaNativeInputV2.CaptureAllowance(limits, 0, 0, 4096, 0, 4096, 64);
        var captured = withCurrentMetadataImage(() -> opened.captureM5Inputs(budget, bounds, caps(limits, 2), allowance), true);
        com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.Task registeredTask = null;
        try {
            assertEquals(2, admission.read().orElseThrow().reservations().size(), "selected cache or new capture lost its Cell charge");
            var evidence = opened.captureNativeM5ReplacementEvidence().toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertEquals(admission.read().orElseThrow().reservations(), evidence.tasks().reservations().stream()
                    .map(NereusKafkaReadScopeV2.TaskFact::reservation).toList());
            assertEquals(1, evidence.tasks().reservations().stream().filter(value -> value.inventory().isEmpty()).count(),
                    "actual live preparation disappeared from the persisted Task directory");
            assertEquals(1, evidence.tasks().reservations().stream().filter(value -> value.inventory().isPresent()).count());
            assertTrue(evidence.references().values().stream().flatMap(List::stream)
                    .filter(row -> row.kind() == com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1.MATERIALIZATION_OR_COMPACTION_TASK)
                    .allMatch(row -> row.disposition() == com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceDispositionV1.PRESENT
                            && !row.enumerationComplete()), "actual live preparation was treated as reference-free");
            assertEquals(com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind.KAFKA_BK_COMPACTED_GENERATION_V2,
                    captured.extents().get(0).kind());
            assertEquals(descriptor.descriptorSha256(), captured.extents().get(0).formatRootSha256());
            assertEquals(descriptor.sourceCut().coverage().inclusiveStart(), captured.frontiers().candidateStartOffset());
            var plan = captured.preparePlan().toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertEquals(descriptor.descriptorSha256(), plan.sourceCut().predecessorViewSha256());
            NereusKafkaCompactionPlanV2.buildForTask(captured, plan.sourceCut().identity(), limits, bounds.decodedBytes());
            var semantic = new KafkaSemanticCompactorV1().compileSemantic(plan);
            assertFalse(semantic.outputBatches().isEmpty(), "actual selected/raw capture produced no compacted output");
            var membership = withCurrentMetadataImage(() -> captured.resolve(plan));
            var expectedParts = descriptor.sealedParts().stream().map(part -> (PhysicalResourceIdV2)
                    new PhysicalResourceIdV2.BookKeeperLedger(descriptor.task().namespace(), part.handle().ledgerIdentity().ledgerId())).toList();
            assertEquals(com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.canonicalTargets(expectedParts),
                    membership.get(captured.extents().get(0).sourceIdentitySha256()));
            assertEquals(captured.extents().size(), membership.size());
            assertTrue(captured.extents().size() >= 3, "actual selected generation omitted its two sealed raw tails");
            var prepared = withCurrentMetadataImage(() -> captured.prepareTask(4096, 65536), true);
            registeredTask = prepared.task();
            var registered = opened.captureNativeM5ReplacementEvidence().toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertEquals(2, registered.tasks().reservations().stream().filter(value -> value.inventory().isPresent()).count());
            var pending = registered.tasks().reservations().stream().filter(value -> value.reservation().compactionTaskIdSha256()
                    .equals(prepared.task().taskIdSha256())).findFirst().orElseThrow();
            assertTrue(pending.decision().isEmpty());
            assertTrue(pending.nativeScope().isEmpty(), "Task observation created an SDK scope for an undispatched preparation");
            assertFalse(pending.closed());
            KafkaBookKeeperReadScopeRealScenario.verifyNativeTaskFacts(registered, Optional.of(registeredTask.taskIdSha256()));
            assertTrue(registered.references().values().stream().flatMap(List::stream)
                    .filter(row -> row.kind() == com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1.MATERIALIZATION_OR_COMPACTION_TASK)
                    .allMatch(row -> row.disposition() == com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceDispositionV1.PRESENT
                            && !row.enumerationComplete()), "registered live Task was treated as reference-free");
            assertEquals(0, budget.usage().owners());
            assertEquals(0, budget.usage().encodedBytes());
        } finally {
            captured.discard().toCompletableFuture().get(30, TimeUnit.SECONDS);
        }
        assertEquals(formal.reservations(), admission.read().orElseThrow().reservations(), "discard released the selected cache charge");
        var key = com.nereusstream.storage.object.retention.M5TaskSelectionDecisionV2.key(binding, registeredTask.taskIdSha256());
        var cancelled = com.nereusstream.storage.object.retention.M5TaskSelectionDecisionV2.decode(
                CanonicalBytes.copyOf(actual.get(root + "/" + key).get(30, TimeUnit.SECONDS).value()));
        assertEquals(registeredTask.taskIdSha256(), cancelled.taskId());
        assertEquals(binding, cancelled.binding());
        assertEquals(com.nereusstream.storage.object.retention.M5TaskSelectionDecisionV2.Outcome.SELECTION_CANCELLED, cancelled.outcome());
        assertTrue(cancelled.selectedOutput().isEmpty());
        assertEquals(null, actual.get(root + "/" + com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperTaskTerminalV2.key(registeredTask.taskIdSha256()))
                .get(30, TimeUnit.SECONDS), "undispatched Task acquired a fabricated SDK terminal");
    }

    record Recompaction(KafkaSealedBookKeeperDescriptorV2 descriptor, List<VerifiedPart> parts, List<CanonicalBytes> expected,
            com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1.NativeInputSources before,
            com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperNativeFallbackRetirementV2.Result retirement) { }

    record ReadGate(AtomicBoolean hold, CompletableFuture<Void> entered, CompletableFuture<Void> release) { }

    @FunctionalInterface
    interface TicketCheck {
        void verify() throws Exception;
    }

    /** Loses only the real second publication/scope response and blocks its authoritative confirmation reads. */
    static final class RecompactionFault {
        private final String mode;
        private final Consumer<List<VerifiedPart>> output;
        private final TicketCheck tickets;
        final KafkaBookKeeperReadScopeRealScenario.ReplacementFactFault replacementFacts;
        private final AtomicBoolean unused = new AtomicBoolean(true);
        private final AtomicBoolean blockSelector = new AtomicBoolean();
        private final AtomicBoolean blockScope = new AtomicBoolean();
        private volatile KafkaSealedBookKeeperDescriptorV2 expected;
        private volatile String selectorKey;
        private volatile String scopeKey;
        private AsyncOxiaClient actual;

        RecompactionFault(String mode, Consumer<List<VerifiedPart>> output, TicketCheck tickets) {
            this.mode = mode;
            this.output = output;
            this.tickets = tickets;
            replacementFacts = new KafkaBookKeeperReadScopeRealScenario.ReplacementFactFault(mode);
        }

        AsyncOxiaClient client(AsyncOxiaClient actual, AsyncOxiaClient delegate) {
            this.actual = actual;
            var observed = replacementFacts.client(delegate);
            if (!publicationUnknown() && !scopeUnknown()) return observed;
            return (AsyncOxiaClient) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] {AsyncOxiaClient.class}, (proxy, method, args) -> {
                        String key = args != null && args.length > 0 && args[0] instanceof String value ? value : "";
                        boolean selector = key.contains("/read-m4/") && key.endsWith("/selector");
                        if (selector) selectorKey = key;
                        if (unavailable(method.getName(), key, selector))
                            return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("second native publication/scope read unknown"));
                        Object response;
                        try {
                            response = method.invoke(observed, args);
                        } catch (java.lang.reflect.InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                        if (method.getName().equals("put") && loses(key, selector, args))
                            return ((CompletionStage<?>) response).thenCompose(value -> {
                                if (selector) blockSelector.set(true);
                                else blockScope.set(true);
                                return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("applied second native publication/scope response lost"));
                            }).toCompletableFuture();
                        return response;
                    });
        }

        private boolean unavailable(String method, String key, boolean selector) {
            return method.equals("get") && (selector && blockSelector.get() || key.equals(scopeKey) && blockScope.get());
        }

        private boolean publicationUnknown() {
            return mode.contains("RECOMPACT_PUBLISH_UNKNOWN");
        }

        private boolean scopeUnknown() {
            return mode.contains("RECOMPACT_SCOPE_UNKNOWN");
        }

        private boolean loses(String key, boolean selector, Object[] args) {
            var descriptor = expected;
            if (descriptor == null) return false;
            if (selector && publicationUnknown()) {
                var decision = M5BindingAuthorityCodecV1.decodeAuthority(CanonicalBytes.copyOf((byte[]) args[1])).taskSelectionDecision();
                return decision.filter(value -> value.outcome() == M5TaskSelectionDecisionV2.Outcome.SELECTED
                                && value.taskId().equals(descriptor.task().taskIdSha256())
                                && value.selectedOutput().equals(Optional.of(descriptor.descriptorSha256()))).isPresent()
                        && unused.compareAndSet(true, false);
            }
            if (scopeUnknown() && key.contains("/views/" + descriptor.descriptorSha256().toHex() + "/native-read-scope/")
                    && unused.compareAndSet(true, false)) {
                scopeKey = key;
                return true;
            }
            return false;
        }

        void arm(KafkaSealedBookKeeperDescriptorV2 descriptor, List<VerifiedPart> parts) {
            expected = descriptor;
            output.accept(parts);
        }

        PublicationOutcome publish(NereusKafkaPreparedTaskV2 prepared, KafkaCompactionAdmissionV1.State previous,
                KafkaCompactionAdmissionV1 admission, NereusKafkaOwnedProviderRuntime.Opened opened,
                CompletableFuture<Void> oldDiscard, Properties client) throws Exception {
            Optional<BindingReadSelector> applied = Optional.empty();
            if (publicationUnknown()) {
                var uncertain = publishWithCurrentImage(prepared);
                assertTrue(uncertain instanceof Throwable || uncertain == PublicationOutcome.OUTCOME_UNKNOWN);
                assertFalse(unused.get(), "actual second selector publication response was not lost: " + uncertain);
                assertTrue(blockSelector.get());
                var held = admission.read().orElseThrow();
                assertEquals(2, held.reservations().size(), "second selector UNKNOWN released a cache/Task charge");
                assertTrue(held.reservations().containsAll(previous.reservations()));
                assertTrue(held.reservations().stream().anyMatch(row -> row.compactionTaskIdSha256().equals(prepared.task().taskIdSha256())));
                assertFalse(oldDiscard.isDone(), "unconfirmed second selection released the predecessor cache");
                tickets.verify();
                var request = new KafkaBookKeeperSequentialReadRequestV1(0, KafkaReadIsolationV1.REPLICA, Long.MAX_VALUE, Optional.empty());
                assertTrue(opened.storage().read(request).handle((value, failure) -> fenced(failure)).toCompletableFuture().get(30, TimeUnit.SECONDS),
                        "second selector UNKNOWN resumed a stopped predecessor actor");
                KafkaBookKeeperRootVerifierRealScenario.verifyProduceFenced(opened, client);
                applied = Optional.of(selector());
                assertEquals(expected.descriptorSha256(), applied.orElseThrow().selectedViewSha256(), "fault preceded actual second selector application");
                blockSelector.set(false);
            }
            var outcome = withCurrentMetadataImage(() -> KafkaBookKeeperRootVerifierRealScenario.publicationUntilKnown(prepared));
            if (applied.isPresent()) assertEquals(applied.orElseThrow(), selector(), "second publication retry advanced another selector epoch");
            return outcome;
        }

        private Object publishWithCurrentImage(NereusKafkaPreparedTaskV2 prepared) throws Exception {
            try {
                // Let the existing bounded helper recognize a changed native image before observing the injected loss.
                return withCurrentMetadataImage(prepared::publish);
            } catch (java.util.concurrent.ExecutionException failure) {
                return failure.getCause();
            }
        }

        com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperNativeFallbackRetirementV2.Result retire(
                KafkaSealedBookKeeperDescriptorV2 descriptor, List<CanonicalBytes> expectedBytes,
                com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1.NativeInputSources before,
                KafkaCompactionAdmissionV1.State selected, KafkaCompactionAdmissionV1 admission,
                NereusKafkaOwnedProviderRuntime.Opened opened, Properties client) throws Exception {
            var retirement = opened.retireNativeM5Fallback().toCompletableFuture();
            if (scopeUnknown()) {
                assertThrows(java.util.concurrent.ExecutionException.class, () -> retirement.get(30, TimeUnit.SECONDS));
                assertFalse(unused.get(), "actual second read scope response was not lost");
                assertTrue(blockScope.get());
                KafkaBookKeeperReadScopeRealScenario.verifyFetch(descriptor, expectedBytes, opened, client);
                return KafkaBookKeeperReadScopeRealScenario.reconcileUnknown(actual, scopeKey, blockScope, descriptor, before,
                        selected, admission, opened, client).get(30, TimeUnit.SECONDS);
            }
            return retirement.get(30, TimeUnit.SECONDS);
        }

        private BindingReadSelector selector() throws Exception {
            return M5BindingAuthorityCodecV1.decodeAuthority(CanonicalBytes.copyOf(
                    actual.get(selectorKey).get(30, TimeUnit.SECONDS).value())).selectorProjection();
        }

        void release() {
            blockSelector.set(false);
            blockScope.set(false);
            replacementFacts.release();
        }

        void verifyReplacementRefresh(KafkaSealedBookKeeperDescriptorV2 descriptor,
                NereusKafkaOwnedProviderRuntime.Opened opened) throws Exception {
            if (!mode.contains("RECOMPACT_COLD_REPLACEMENT_")) replacementFacts.verify(actual, descriptor, opened);
        }
    }

    static Optional<Recompaction> recompactIfSelected(String mode, BindingIdentity binding, KafkaSealedBookKeeperDescriptorV2 previous,
            KafkaCompactionAdmissionV1.State formal, KafkaCompactionAdmissionV1 admission,
            NereusKafkaOwnedProviderRuntime.Opened opened, Properties client, KafkaBookKeeperReadScopeRealScenario.VerifyFetch fetch,
            ReadGate reads, Callable<BindingReadSelector> selector, CompletableFuture<Void> oldDiscard,
            RecompactionFault fault) throws Exception {
        if (!mode.startsWith("TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT")) return Optional.empty();
        var bounds = KafkaBookKeeperNativeInputRealScenario.BOUNDS;
        var raw = KafkaBookKeeperReadCellBudgetV2.Usage.forRunSource(bounds);
        var share = new KafkaBookKeeperReadCellBudgetV2.Usage(raw.owners(), raw.readSlots(),
                raw.encodedBytes() + 2 * bounds.nativeBytes(), raw.decodedBytes() + 2 * bounds.decodedBytes());
        var scope = opened.storage().nativeReadSources().activeRoot().providerScopeId();
        var budget = new KafkaBookKeeperReadCellBudgetV2(scope, share, Map.of(binding, share));
        var limits = new Caps(bounds.payloadBytes(), bounds.batches(), bounds.records(), bounds.records(),
                bounds.decodedBytes(), bounds.decodedBytes(), bounds.decodedBytes(), bounds.records(), bounds.records());
        var allowance = new NereusKafkaNativeInputV2.CaptureAllowance(limits, 0, 0, 4096, 0, 4096, 64);
        var preparation = prepareRecompaction(opened, budget, bounds, limits, allowance, admission, formal);
        var captured = preparation.input();
        var prepared = preparation.task();
        var parts = preparation.parts();
        var descriptor = KafkaSealedBookKeeperDescriptorV2.create(prepared.plan(), prepared.semantic(), prepared.layout(),
                parts, captured.recoveryState());
        assertEquals(previous.descriptorSha256(), descriptor.sourceCut().predecessorViewSha256());
        assertTrue(descriptor.sourceCut().coverage().exclusiveEnd() > previous.sourceCut().coverage().exclusiveEnd());
        assertEquals(2, admission.read().orElseThrow().reservations().size());
        assertTrue(admission.read().orElseThrow().reservations().containsAll(formal.reservations()));
        fault.arm(descriptor, parts);
        var request = new KafkaBookKeeperSequentialReadRequestV1(previous.sourceCut().coverage().exclusiveEnd(),
                KafkaReadIsolationV1.REPLICA, Long.MAX_VALUE, Optional.empty());
        reads.hold().set(true);
        var oldRead = opened.storage().read(request).toCompletableFuture();
        CompletableFuture.anyOf(oldRead, reads.entered()).get(30, TimeUnit.SECONDS);
        assertTrue(reads.entered().isDone(), "actual old selected actor did not dispatch its raw-tail SDK read");
        assertFalse(oldRead.isDone());
        var publication = prepared.publish().toCompletableFuture();
        try {
            TestUtils.waitForCondition(() -> opened.storage().read(request).handle((value, failure) -> fenced(failure))
                    .toCompletableFuture().get(5, TimeUnit.SECONDS), 30000, "second publication did not stop old read admission");
            assertFalse(publication.isDone(), "second publication bypassed the actual old SDK drain");
            assertEquals(captured.predecessorSelector(), selector.call(), "held old SDK read allowed selector replacement");
            assertFalse(oldDiscard.isDone(), "held old SDK read released the selected cache charge");
            assertTrue(publication.cancel(true));
            assertTrue(oldRead.cancel(true));
            assertEquals(2, admission.read().orElseThrow().reservations().size(), "observer cancellation freed a held cache");
        } finally {
            reads.release().complete(null);
        }
        var outcome = fault.publish(prepared, formal, admission, opened, oldDiscard, client);
        assertTrue(outcome == PublicationOutcome.APPLIED_EXACT || outcome == PublicationOutcome.EXISTING_EXACT);
        oldDiscard.get(30, TimeUnit.SECONDS);
        var selected = admission.read().orElseThrow();
        assertEquals(1, selected.reservations().size(), "old cache charge remained after actual replacement drain");
        assertEquals(prepared.task().taskIdSha256(), selected.reservations().get(0).compactionTaskIdSha256());
        assertFalse(selected.reservations().containsAll(formal.reservations()));
        var expected = KafkaBookKeeperRootVerifierRealScenario.selectedBytes(prepared, descriptor, opened);
        fetch.verify(descriptor, expected, opened, client);
        var before = opened.storage().nativeInputSources();
        var retired = fault.retire(descriptor, expected, before, selected, admission, opened, client);
        assertEquals(descriptor.descriptorSha256(), retired.selector().selectedViewSha256());
        assertEquals(previous.descriptorSha256(), retired.transfer().original().publicationPreferred().orElseThrow().descriptorSha256());
        assertEquals(captured.extents().stream().map(extent -> extent.sourceIdentitySha256().toHex()).sorted().toList(),
                retired.batch().sources().stream().map(source -> source.sourceIdentitySha256().toHex()).sorted().toList());
        KafkaBookKeeperRootVerifierRealScenario.verifyNativeSourceTransfer(opened.storage(), descriptor, before);
        assertEquals(selected.reservations(), admission.read().orElseThrow().reservations(), "M4 release freed the current cache");
        var tail = opened.storage().read(new KafkaBookKeeperSequentialReadRequestV1(descriptor.sourceCut().coverage().exclusiveEnd(),
                KafkaReadIsolationV1.REPLICA, Long.MAX_VALUE, Optional.empty())).toCompletableFuture().get(30, TimeUnit.SECONDS);
        assertEquals(KafkaBookKeeperReadOutcomeV1.FOUND, tail.outcome());
        assertEquals(0, budget.usage().owners());
        assertEquals(0, budget.usage().encodedBytes());
        var rejection = assertThrows(java.util.concurrent.ExecutionException.class,
                // selectedOnlyAttempt completes its exact discard before exposing a failed preparation.
                () -> withCurrentMetadataImage(() -> selectedOnlyAttempt(opened, budget, bounds, limits, allowance, admission, descriptor)),
                "selected-only input with no dirty source admitted another Task");
        Throwable cause = rejection;
        while (cause.getCause() != null) cause = cause.getCause();
        assertEquals("native M5 task has no cleanable bytes", cause.getMessage());
        assertEquals(selected.reservations(), admission.read().orElseThrow().reservations());
        assertFalse(captured.discard().toCompletableFuture().isDone(), "discard freed the second selected cache before Owner drain");
        return Optional.of(new Recompaction(descriptor, parts, expected, before, retired));
    }

    private record RecompactionPreparation(NereusKafkaNativeInputV2.Captured input, NereusKafkaPreparedTaskV2 task,
            List<VerifiedPart> parts) { }

    private static RecompactionPreparation prepareRecompaction(NereusKafkaOwnedProviderRuntime.Opened opened,
            KafkaBookKeeperReadCellBudgetV2 budget, com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperRunSourceV2.Bounds bounds,
            Caps limits, NereusKafkaNativeInputV2.CaptureAllowance allowance, KafkaCompactionAdmissionV1 admission,
            KafkaCompactionAdmissionV1.State selected) throws Exception {
        for (int attempt = 0; attempt < 3; attempt++) {
            var captured = withCurrentMetadataImage(() -> opened.captureM5Inputs(budget, bounds, caps(limits, 2), allowance), true);
            try {
                var task = captured.prepareTask(4096, 65536).toCompletableFuture().get(30, TimeUnit.SECONDS);
                var parts = task.materialize().toCompletableFuture().get(30, TimeUnit.SECONDS);
                return new RecompactionPreparation(captured, task, parts);
            } catch (java.util.concurrent.ExecutionException failure) {
                Throwable cause = failure;
                while (cause.getCause() != null) cause = cause.getCause();
                if (!"native M5 metadata image changed during capture".equals(cause.getMessage())) throw failure;
                try {
                    captured.discard().toCompletableFuture().get(30, TimeUnit.SECONDS);
                } catch (Exception cleanup) {
                    failure.addSuppressed(cleanup);
                    throw failure;
                }
                assertEquals(selected.reservations(), admission.read().orElseThrow().reservations(),
                        "known second preparation cleanup released the predecessor cache or retained its failed capture");
                assertEquals(0, budget.usage().owners(), "known second materialization cleanup retained native reads");
                assertEquals(0, budget.usage().encodedBytes(), "known second materialization cleanup retained native buffers");
            }
        }
        throw new IllegalStateException("native metadata image changed through three confirmed second preparations");
    }

    private static CompletionStage<NereusKafkaPreparedTaskV2> selectedOnlyAttempt(NereusKafkaOwnedProviderRuntime.Opened opened,
            KafkaBookKeeperReadCellBudgetV2 budget, com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperRunSourceV2.Bounds bounds,
            Caps limits, NereusKafkaNativeInputV2.CaptureAllowance allowance, KafkaCompactionAdmissionV1 admission,
            KafkaSealedBookKeeperDescriptorV2 descriptor) {
        return opened.captureM5Inputs(budget, bounds, caps(limits, 2), allowance).thenCompose(next -> {
            var attempt = CompletableFuture.completedFuture(next).thenCompose(input -> {
                assertEquals(1, input.extents().size(), "released cut retained a covered raw source");
                assertEquals(descriptor.descriptorSha256(), input.extents().get(0).formatRootSha256());
                assertEquals(2, admission.read().orElseThrow().reservations().size(), "actual old local preparation slot was not released");
                return input.prepareTask(4096, 65536);
            });
            return attempt.handle((task, failure) -> next.discard().thenCompose(ignored -> failure == null
                    ? CompletableFuture.completedFuture(task) : CompletableFuture.<NereusKafkaPreparedTaskV2>failedFuture(failure)))
                    .thenCompose(completed -> completed);
        });
    }

    private static boolean fenced(Throwable failure) {
        while (failure instanceof java.util.concurrent.CompletionException
                || failure instanceof java.util.concurrent.ExecutionException) failure = failure.getCause();
        return failure != null && "native M4 read owner is fenced".equals(failure.getMessage());
    }
}
