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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaObjectMaterializedPrefixV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Member;
import com.nereusstream.metadata.oxia.v2.retention.OxiaKafkaBookKeeperM4ReadRouteV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Completion;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Context;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Result;
import com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCoordinatorV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Publishes only a completely verified native Object output under the original BK/Object writer tickets. */
final class NereusKafkaObjectPublication {
    private NereusKafkaObjectPublication() { }

    private record Participant(OxiaQuotaTargetDeleteStoreV2 route, Sha256Digest capability, Member member) { }

    private record Attempt(PublicationOutcome outcome, Exception failure) { }

    private record WriterTickets(M5TargetDeleteMultiWriterGuardV2 guard,
            Map<PhysicalResourceIdV2, Context> contexts, Sha256Digest terminalRoot) {
        private void reconcile(Duration timeout) throws Exception {
            long deadline = System.nanoTime() + timeout.toNanos();
            var pending = contexts;
            while (!pending.isEmpty()) {
                var unresolved = guard.reconcileTerminalWithContexts(pending, terminalRoot)
                        .toCompletableFuture().get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                if (unresolved.size() >= pending.size()) {
                    throw new IllegalStateException("Kafka Object selected publication retains original physical tickets");
                }
                pending = unresolved.stream().collect(Collectors.toUnmodifiableMap(resource -> resource, contexts::get));
            }
        }
    }

    static CompletionStage<Void> publish(NereusKafkaObjectMaterialization.Verified verified,
            NereusKafkaObjectProvider provider, KafkaBookKeeperPartitionV1 storage,
            OxiaQuotaTargetDeleteStoreV2 bookKeeper, Map<Sha256Digest, List<PhysicalResourceIdV2>> membership,
            Runnable requireCurrent, Executor control, Executor providerReads, Duration timeout) throws Exception {
        var task = verified.stored().task();
        var plan = task.prepared().plan();
        var profile = provider.profile();
        var prefix = new KafkaObjectMaterializedPrefixV1(plan, verified.generation(), provider::readForFetch);
        if (!prefix.originalFallbackSources().equals(verified.protections())) {
            throw new IllegalArgumentException("Kafka Object publication lost its exact original fallback set");
        }
        var raw = storage.nativeReadSources();
        var sources = new ArrayList<PhysicalResourceIdV2>();
        for (var extent : plan.sourceCut().sources()) {
            var resources = membership.get(extent.sourceIdentitySha256());
            if (resources == null || resources.size() != 1
                    || !(resources.get(0) instanceof PhysicalResourceIdV2.BookKeeperLedger ledger)
                    || !extent.ledgerIdentitySha256().equals(Optional.of(ledger.sha256()))) {
                throw new IllegalStateException("Kafka Object publication has another original BK source membership");
            }
            sources.add(ledger);
        }
        var versions = verified.stored().credits().stream().map(credit -> credit.resource().orElseThrow()).toList();
        var participants = participants(prefix, provider, raw, bookKeeper, sources, versions);
        var tickets = tickets(prefix, participants, task.worker().worker().nativeWorkerIdentitySha256());
        var binding = plan.sourceCut().identity().binding();
        var coordinator = new M5MaterializationCoordinatorV1(task.metadata(), task.partition(), binding);
        var publication = tickets.guard().executeWithContexts(tickets.contexts(), () -> CompletableFuture.supplyAsync(() ->
                publishExact(verified, provider, storage, prefix, participants, coordinator,
                        requireCurrent, providerReads, timeout, tickets.terminalRoot()), control));
        return publication.thenApplyAsync(result -> {
            finishPublication(task, profile.protocolCellSha256(), result);
            return null;
        }, control);
    }

    /** Only an already selected original manifest can settle its Task and physical writer tickets. */
    static void reconcileSelected(NereusKafkaObjectRecovery.Selected selected, BindingReadSelector selector,
            NereusKafkaObjectProvider provider, KafkaBookKeeperPartitionV1.NativeReadSources raw,
            OxiaQuotaTargetDeleteStoreV2 bookKeeper, PhysicalResourceIdV2.Namespace bookKeeperNamespace,
            OxiaKafkaBookKeeperM4ReadRouteV2 binding, int partition, Duration timeout)
            throws Exception {
        var plan = selected.plan();
        var taskId = plan.taskIdSha256();
        var cell = plan.sourceCut().identity().protocolCellSha256();
        var metadata = binding.materializationMetadata(cell);
        var admission = new M5MaterializationAdmissionV1(metadata, cell);
        if (admission.read().orElseThrow().reservations().stream()
                .noneMatch(reservation -> reservation.taskIdSha256().equals(taskId))) return;
        if (selector.mode() != SelectorMode.PREFERRED_WITH_FALLBACK) {
            throw new IllegalStateException("Kafka Object publication still owns capacity after fallback retirement");
        }
        var prefix = selected.prefix();
        var original = prefix.sourceCut().sources();
        var cover = prefix.coverage();
        var ledgers = raw.sealedRoots().stream().filter(root -> {
            long end = root.kafkaEndOffsetExclusive().orElseThrow();
            return root.kafkaStartOffset() < end && end > cover.inclusiveStart()
                    && root.kafkaStartOffset() < cover.exclusiveEnd();
        }).map(root -> new PhysicalResourceIdV2.BookKeeperLedger(bookKeeperNamespace, root.ledgerIdentity().ledgerId()))
                .toList();
        if (!ledgers.stream().map(PhysicalResourceIdV2.BookKeeperLedger::sha256).toList().equals(
                original.stream().map(extent -> extent.ledgerIdentitySha256().orElseThrow()).toList())) {
            throw new IllegalStateException("Kafka Object publication cannot reconcile another BK source set");
        }
        var participants = participants(prefix, provider, raw, bookKeeper, ledgers, selected.versions());
        var tickets = tickets(prefix, participants, selected.inventory().worker().nativeWorkerIdentitySha256());
        var coordinator = new M5MaterializationCoordinatorV1(metadata, partition, selector.binding());
        var outcome = coordinator.finishSelectedTask(plan, selected.validated(), prefix.originalFallbackSources(), selector);
        if (!exact(outcome)) {
            throw new IllegalStateException("Kafka Object selected Task audit remains unresolved: " + outcome);
        }
        tickets.reconcile(timeout);
        releaseAdmission(admission, taskId);
    }

    /** Called after the original invocation returned; this does not dispatch another selector or Object write. */
    static void reconcileOriginal(NereusKafkaObjectMaterialization.Verified verified,
            NereusKafkaObjectProvider provider, KafkaBookKeeperPartitionV1 storage,
            OxiaQuotaTargetDeleteStoreV2 bookKeeper, PhysicalResourceIdV2.Namespace bookKeeperNamespace,
            OxiaKafkaBookKeeperM4ReadRouteV2 binding, Runnable requireCurrent, Duration timeout) throws Exception {
        var task = verified.stored().task();
        var plan = task.prepared().plan();
        var manifest = M5MaterializationCodecV1.manifestSha256(verified.generation().manifestView());
        requireCurrent.run();
        var selector = binding.readSelector().orElseThrow();
        if (!selector.selectedViewSha256().equals(manifest)
                || selector.ownerEpoch() != plan.sourceCut().identity().ownerEpoch()) {
            throw new IllegalStateException("Kafka Object publication has no exact selected original to reconcile");
        }
        var selected = NereusKafkaObjectRecovery.recover(binding, task.partition(), selector, provider, timeout).orElseThrow();
        if (!selected.plan().equals(plan) || !selected.inventory().equals(task.pending())) {
            throw new IllegalStateException("Kafka Object publication differs from the original local Task");
        }
        requireCurrent.run();
        storage.finishSelectedNativeObjectPublication(manifest);
        reconcileSelected(selected, selector, provider, storage.nativeReadSources(), bookKeeper, bookKeeperNamespace,
                binding, task.partition(), timeout);
    }

    private static WriterTickets tickets(KafkaObjectMaterializedPrefixV1 prefix,
            Map<PhysicalResourceIdV2, Participant> participants, Sha256Digest workerFence) {
        var root = publicationRoot(prefix, participants);
        var routes = new ExactRoutes(participants.entrySet().stream().collect(Collectors.toUnmodifiableMap(
                entry -> entry.getKey().authorityKey(), entry -> entry.getValue().route())));
        var contexts = participants.entrySet().stream().collect(Collectors.toUnmodifiableMap(Map.Entry::getKey,
                entry -> new Context(ProofBoundWriterClassV1.MANIFEST_SELECTOR_GENERATION_REPRESENTATION_V1,
                        entry.getValue().capability(), workerFence, root)));
        var guard = new M5TargetDeleteMultiWriterGuardV2(new M5TargetDeleteAuthorityCoordinatorV1(routes));
        return new WriterTickets(guard, contexts, root);
    }

    private static void finishPublication(NereusKafkaObjectMaterialization.Registered task,
            Sha256Digest cell, Result<Attempt> result) {
        var attempt = result.value().orElseThrow(
                () -> new IllegalStateException("Kafka Object publication physical writer admission was not granted"));
        if (attempt.failure() != null) throw new CompletionException(attempt.failure());
        if (!result.unresolvedTargets().isEmpty() || !exact(attempt.outcome())) {
            throw new IllegalStateException("Kafka Object publication or original ticket cleanup remains unresolved");
        }
        releaseAdmission(new M5MaterializationAdmissionV1(task.metadata(), cell), task.prepared().plan().taskIdSha256());
    }

    private static void releaseAdmission(M5MaterializationAdmissionV1 admission, Sha256Digest taskId) {
        var released = admission.release(taskId);
        if (released != M5MaterializationAdmissionV1.Outcome.APPLIED_EXACT
                && released != M5MaterializationAdmissionV1.Outcome.EXISTING_EXACT) {
            throw new IllegalStateException("Kafka Object publication retains its unresolved Cell reservation");
        }
    }

    private static Map<PhysicalResourceIdV2, Participant> participants(
            KafkaObjectMaterializedPrefixV1 prefix, NereusKafkaObjectProvider provider,
            KafkaBookKeeperPartitionV1.NativeReadSources raw, OxiaQuotaTargetDeleteStoreV2 bookKeeper,
            List<? extends PhysicalResourceIdV2> sources, List<PhysicalResourceIdV2.ObjectVersion> versions) {
        var profile = provider.profile();
        var cut = prefix.sourceCut();
        var bkMember = new Member(PositionDomain.KAFKA_OFFSET,
                raw.activeRoot().providerScopeId().digest(), cut.identity().binding());
        var objectMember = new Member(PositionDomain.KAFKA_OFFSET, profile.scopeId().digest(), cut.identity().binding());
        var result = new TreeMap<PhysicalResourceIdV2, Participant>();
        for (var ledger : sources) {
            if (result.putIfAbsent(ledger, new Participant(bookKeeper, raw.configurationSha256(), bkMember)) != null) {
                throw new IllegalStateException("Kafka Object publication has another original BK source membership");
            }
        }
        for (var object : versions) {
            if (!object.namespace().equals(profile.physicalBinding().physicalNamespace())
                    || result.putIfAbsent(object,
                            new Participant(provider.physicalQuota(), profile.providerConfigurationSha256(), objectMember)) != null) {
                throw new IllegalStateException("Kafka Object publication has another original Object membership");
            }
        }
        if (result.size() > M5TargetDeleteMultiWriterGuardV2.MAX_TARGETS) {
            throw new IllegalStateException("Kafka Object publication physical members exceed their bounded scope");
        }
        return result;
    }

    private static Completion<Attempt> publishExact(NereusKafkaObjectMaterialization.Verified verified,
            NereusKafkaObjectProvider provider, KafkaBookKeeperPartitionV1 storage, KafkaObjectMaterializedPrefixV1 prefix,
            Map<PhysicalResourceIdV2, Participant> participants, M5MaterializationCoordinatorV1 coordinator,
            Runnable requireCurrent, Executor providerReads, Duration timeout, Sha256Digest root) {
        var task = verified.stored().task();
        var profile = provider.profile();
        boolean fenced = false;
        try {
            NereusKafkaObjectMaterialization.requireWriteAuthority(task, provider, requireCurrent, timeout);
            verifyMembers(participants, profile.protocolCellSha256(), timeout);
            var namespace = participants.keySet().stream().filter(PhysicalResourceIdV2.BookKeeperLedger.class::isInstance)
                    .findFirst().orElseThrow().namespace();
            var selection = storage.beginNativeObjectPublication(prefix, namespace, providerReads);
            fenced = true;
            var outcome = coordinator.publish(task.prepared().plan(), verified.generation(), verified.protections());
            if (!exact(outcome)) return new Completion<>(new Attempt(outcome, null), Optional.empty());
            selection.installSelected();
            return new Completion<>(new Attempt(outcome, null), Optional.of(root));
        } catch (Exception failure) {
            return new Completion<>(new Attempt(PublicationOutcome.CONFLICT, failure),
                    fenced ? Optional.empty() : Optional.of(root));
        }
    }

    private static void verifyMembers(Map<PhysicalResourceIdV2, Participant> participants,
            Sha256Digest cell, Duration timeout) throws Exception {
        for (var entry : participants.entrySet()) {
            var resource = entry.getKey();
            var owner = entry.getValue();
            var member = owner.route().members().readFact(resource).toCompletableFuture()
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS).inventory();
            if (!member.protocolCell().equals(cell) || !member.members().contains(owner.member())) {
                throw new IllegalStateException("Kafka Object publication has no exact admitted physical member");
            }
        }
    }

    private static Sha256Digest publicationRoot(KafkaObjectMaterializedPrefixV1 prefix,
            Map<PhysicalResourceIdV2, Participant> participants) {
        var bytes = ByteBuffer.allocate((participants.size() + 1) * Sha256Digest.LENGTH);
        bytes.put(prefix.manifestSha256().bytes().toByteArray());
        for (var resource : participants.keySet()) bytes.put(resource.sha256().bytes().toByteArray());
        return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.array()));
    }

    private static boolean exact(PublicationOutcome outcome) {
        return outcome == PublicationOutcome.APPLIED_EXACT || outcome == PublicationOutcome.EXISTING_EXACT;
    }

    private record ExactRoutes(Map<String, OxiaQuotaTargetDeleteStoreV2> routes)
            implements ExactMetadataTransactionStoreV1 {
        @Override
        public CompletionStage<Optional<VersionedValue>> read(String key) {
            return route(key).read(key);
        }

        @Override
        public CompletionStage<MutationOutcome> compareAndSet(Optional<VersionedValue> previous,
                String key, CanonicalBytes candidate) {
            return route(key).compareAndSet(previous, key, candidate);
        }

        @Override
        public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
            return CompletableFuture.failedFuture(
                    new UnsupportedOperationException("Kafka Object publication uses exact per-resource CAS"));
        }

        @Override
        public boolean supportsAtomicMultiKeyTransactions() {
            return false;
        }

        private OxiaQuotaTargetDeleteStoreV2 route(String key) {
            var route = routes.get(key);
            if (route == null) throw new IllegalArgumentException("Kafka Object publication has no physical route for key");
            return route;
        }
    }
}
