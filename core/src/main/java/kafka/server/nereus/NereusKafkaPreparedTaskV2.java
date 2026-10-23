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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionPublicationV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperPublicationTicketsV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperDescriptorV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperReaderV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionLayoutV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionLayoutV2.Layout;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperControlMetadataStoreV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.PartKind;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.Task;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionWriterV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionWriterV2.VerifiedPart;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperTaskTerminalV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperTaskTerminationV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.CompactionPlan;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSemanticCompactorV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionSemanticOutputV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateSpecV2;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Completion;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Context;
import com.nereusstream.storage.object.retention.M5MaterializationWorkerCoordinatorV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtection;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import com.nereusstream.storage.object.retention.M5TaskSelectionCoordinatorV2;
import com.nereusstream.storage.object.retention.M5TaskSelectionDecisionV2;

import java.util.List;
import java.util.Optional;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** Actual retained native plan and physical task, with private worker/SDK ownership until proven termination. */
public final class NereusKafkaPreparedTaskV2 {
    private final NereusKafkaNativeInputV2.Captured captured;
    private final CompactionPlan plan;
    private final KafkaCompactionSemanticOutputV2 semantic;
    private final Layout layout;
    private final KafkaBookKeeperControlMetadataStoreV2 metadata;
    private final int partition;
    private final M5TargetDeleteMultiWriterGuardV2 writerGuard;
    private boolean materializationStarted;
    private Execution execution;

    private NereusKafkaPreparedTaskV2(NereusKafkaNativeInputV2.Captured captured, CompactionPlan plan,
            KafkaCompactionSemanticOutputV2 semantic, Layout layout, KafkaBookKeeperControlMetadataStoreV2 metadata) {
        this.captured = captured;
        this.plan = plan;
        this.semantic = semantic;
        this.layout = layout;
        this.metadata = metadata;
        this.partition = captured.nativeSources().sources().activeRoot().partitionId();
        this.writerGuard = captured.writerGuard();
    }

    static NereusKafkaPreparedTaskV2 build(NereusKafkaNativeInputV2.Captured captured, CompactionPlan plan,
            KafkaBookKeeperControlMetadataStoreV2 metadata, M5BookKeeperNativeCreateClientV2 source,
            long attempt, int maximumEntryBytes, long maximumPartBytes) {
        var semantic = new KafkaSemanticCompactorV1().compileSemantic(plan);
        var layout = KafkaBookKeeperCompactionLayoutV2.plan(plan, semantic, captured.recoveryState(),
                source.spec().namespace(), source.capabilitySnapshot(), attempt, maximumEntryBytes, maximumPartBytes);
        long dataBytes = layout.task().parts().stream().filter(part -> part.kind() == PartKind.DATA)
                .mapToLong(part -> part.length()).reduce(0, Math::addExact);
        long indexBytes = layout.task().parts().stream().filter(part -> part.kind() == PartKind.INDEX)
                .mapToLong(part -> part.length()).reduce(0, Math::addExact);
        // Include all physical chunk envelopes and the independent protocol carrier, not just semantic bodies.
        if (dataBytes > plan.policy().caps().maximumOutputBytes() || indexBytes > plan.policy().caps().maximumIndexBytes())
            throw new IllegalArgumentException("native M5 physical layout exceeds retained output/index capacity");
        return new NereusKafkaPreparedTaskV2(captured, plan, semantic, layout, metadata);
    }

    void register() {
        if (!new KafkaBookKeeperInventoryV2(metadata).register(layout.task()))
            throw new IllegalStateException("native M5 task registration is unresolved");
    }

    Sha256Digest id() {
        return layout.task().taskIdSha256();
    }

    public Task task() {
        captured.requireOpen();
        return layout.task();
    }

    public CompactionPlan plan() {
        captured.requireOpen();
        return plan;
    }

    public KafkaCompactionSemanticOutputV2 semantic() {
        captured.requireOpen();
        return semantic;
    }

    public Layout layout() {
        captured.requireOpen();
        return layout;
    }

    /** Writes and independently verifies actual sealed parts; this never publishes a read selector. */
    public CompletionStage<List<VerifiedPart>> materialize() {
        return captured.materialize(this);
    }

    /** Publish this same finite Task through its actual worker, native membership and physical tickets. */
    public CompletionStage<PublicationOutcome> publish() {
        return captured.publish(this);
    }

    CompletionStage<PublicationOutcome> publishOwned(NereusKafkaOwnedProviderRuntime.Opened opened) {
        final Execution owned;
        synchronized (this) {
            owned = execution;
        }
        if (owned == null || owned.opened != opened)
            throw new IllegalStateException("native M5 publication lacks its actual materialization owner");
        return owned.publish();
    }

    synchronized boolean requiresOwnerCacheDrain() {
        return execution != null && execution.activationAttempted;
    }

    synchronized void beginMaterialization() {
        if (materializationStarted) throw new IllegalStateException("native M5 task already started materialization");
        materializationStarted = true;
    }

    CompletionStage<List<VerifiedPart>> write(M5MaterializationWorkerCoordinatorV1.Proposal proposal,
            NereusKafkaOwnedProviderRuntime.Opened opened, Executor connectionExecutor) {
        if (!proposal.identity().equals(plan.sourceCut().identity()))
            throw new IllegalStateException("native M5 materialization worker differs from the exact plan");
        register();
        if (decisions().readDecision(id()).isPresent()) throw new IllegalStateException("native M5 task selection is already terminal");
        var worker = proposal.admit(Sha256Digest.hash(M5MaterializationCodecV1.encodeSourceCut(plan.sourceCut())), semantic.compactionPlanRootSha256(), id())
                .orElseThrow(() -> new IllegalStateException("native M5 worker admission is unresolved"));
        worker.requireCurrent();
        requireNativeWorker(worker);
        var owned = new Execution(this, opened, worker);
        synchronized (this) {
            execution = owned;
        }
        return owned.write(connectionExecutor);
    }

    void cancelOwnedTask() {
        final Execution owned;
        synchronized (this) {
            owned = execution;
        }
        if (owned == null) cancelSelection();
        else owned.opened.awaitM5Task(owned.cancel());
    }

    private M5TaskSelectionCoordinatorV2 decisions() {
        return new M5TaskSelectionCoordinatorV2(metadata, partition, plan.sourceCut().identity().binding());
    }

    private void requireNativeWorker(M5MaterializationWorkerCoordinatorV1.Admission worker) {
        new M5MaterializationWorkerCoordinatorV1(metadata, partition, plan.sourceCut().identity().binding()).requireCurrent(worker.worker());
    }

    private void cancelSelection() {
        var decisions = decisions();
        var decision = decisions.readDecision(id());
        if (decision.isEmpty()) {
            var attempt = decisions.prepare(id()).orElseThrow(
                    () -> new IllegalStateException("native M5 prepared task cancellation is unresolved"));
            // Cancel this inert task against the exact current selector without changing its view or owner.
            // A rollover/takeover can invalidate the old source cut but must not make the task selectable again.
            decision = attempt.cancelSelection(attempt.selector());
        }
        if (decision.isEmpty() || decision.orElseThrow().outcome() != M5TaskSelectionDecisionV2.Outcome.SELECTION_CANCELLED)
            throw new IllegalStateException("native M5 prepared task cancellation is unknown or selected");
        if (!decisions.archiveCurrentDecision(id()))
            throw new IllegalStateException("native M5 prepared task cancellation archive is unresolved");
    }

    /** One finite Task, one bound SDK, one sorted writer invocation; no new executor or retry layer. */
    private static final class Execution {
        private final NereusKafkaPreparedTaskV2 prepared;
        private final NereusKafkaOwnedProviderRuntime.Opened opened;
        private final M5MaterializationWorkerCoordinatorV1.Admission worker;
        private final Executor control;
        private final M5TargetDeleteMultiWriterGuardV2 guard;
        private final CompletableFuture<Completion<CanonicalBytes>> terminal = new CompletableFuture<>();
        private volatile M5BookKeeperNativeCreateClientV2 client;
        private volatile BookKeeperCellSession session;
        private volatile CompletionStage<M5TargetDeleteMultiWriterGuardV2.Result<CanonicalBytes>> guarded;
        private volatile KafkaBookKeeperTaskTerminalV2 physicalTerminal;
        private volatile List<VerifiedPart> verifiedParts;
        private volatile boolean activationAttempted;
        private boolean activated;
        private KafkaSealedBookKeeperDescriptorV2 descriptor;
        private KafkaSealedBookKeeperReaderV2 publicationReader;
        private CanonicalBytes scopeTerminal;
        private CompletableFuture<PublicationOutcome> publication;
        private com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1.NativePreferredActivation activation;
        private volatile boolean clientClosed;
        private CompletableFuture<Void> cancelling;

        Execution(NereusKafkaPreparedTaskV2 prepared, NereusKafkaOwnedProviderRuntime.Opened opened,
                M5MaterializationWorkerCoordinatorV1.Admission worker) {
            this.prepared = prepared;
            this.opened = opened;
            this.worker = worker;
            control = opened.nativeM5TaskControl();
            guard = prepared.writerGuard;
        }

        private void connect() {
            if (client != null) return;
            try {
                var task = prepared.layout.task();
                var configs = java.util.stream.IntStream.range(0, task.parts().size()).mapToObj(task::configuration).toList();
                client = opened.nativeM5TaskClient(M5BookKeeperNativeCreateSpecV2.of(opened.nativeM5InstanceId(), task.taskIdSha256(), configs));
            } catch (Exception failure) {
                throw new CompletionException(failure);
            }
        }

        CompletionStage<List<VerifiedPart>> write(Executor connectionExecutor) {
            return CompletableFuture.runAsync(this::connect, connectionExecutor).thenComposeAsync(ignored -> {
                session = opened.nativeM5TaskSession(client);
                var inventory = new KafkaBookKeeperInventoryV2(prepared.metadata, control);
                CompletionStage<List<PhysicalResourceIdV2>> reserved = CompletableFuture.completedFuture(new ArrayList<>());
                for (int ordinal = 0; ordinal < prepared.layout.task().parts().size(); ordinal++) {
                    int index = ordinal;
                    reserved = reserved.thenComposeAsync(resources -> inventory.reservePart(prepared.layout.task(), index, session)
                            .thenApply(part -> {
                                resources.add(part.orElseThrow(() -> new IllegalStateException("native M5 output ID reservation is unresolved")).resource());
                                return resources;
                            }), control);
                }
                return reserved.thenComposeAsync(this::admitAndWrite, control);
            }, control).handle((value, failure) -> new WriteResult(value, failure)).thenComposeAsync(result -> closeSession()
                    .handle((ignored, closeFailure) -> {
                        if (result.failure() != null) {
                            if (closeFailure != null && closeFailure != result.failure()) result.failure().addSuppressed(closeFailure);
                            throw new CompletionException(result.failure());
                        }
                        if (closeFailure != null) throw new CompletionException(closeFailure);
                        verifiedParts = List.copyOf(result.parts());
                        return verifiedParts;
                    }), control);
        }

        private CompletionStage<List<VerifiedPart>> admitAndWrite(List<PhysicalResourceIdV2> resources) {
            var targets = M5TargetDeleteMultiWriterGuardV2.canonicalTargets(resources);
            CompletionStage<Void> admitted = CompletableFuture.completedFuture(null);
            for (var resource : targets)
                admitted = admitted.thenComposeAsync(ignored -> opened.nativeM5ResourceAuthority(resource, client.spec()), control);
            return admitted.thenComposeAsync(ignored -> {
                var context = new Context(ProofBoundWriterClassV1.OWNER_WORKER_LEASE_HANDLE_PIN_V1,
                        prepared.layout.task().capability().configurationDigest(),
                        Sha256Digest.hash(worker.worker().encode()), client.spec().sha256());
                var output = new CompletableFuture<List<VerifiedPart>>();
                guarded = guard.execute(targets, context, () -> {
                    CompletableFuture.runAsync(() -> prepared.requireNativeWorker(worker), control).thenComposeAsync(current ->
                            new KafkaBookKeeperCompactionWriterV2(new KafkaBookKeeperInventoryV2(prepared.metadata, control),
                                    session, client::captureExactTarget, control).write(prepared.layout), control)
                            .whenComplete((parts, failure) -> {
                                if (failure == null) output.complete(parts);
                                else output.completeExceptionally(failure);
                            });
                    // Native writes can complete while their tickets remain held until a checked physical terminal.
                    return terminal;
                });
                guarded.whenComplete((result, failure) -> {
                    if (failure != null) output.completeExceptionally(failure);
                    else if (!result.mutationInvoked()) output.completeExceptionally(
                            new IllegalStateException("native M5 output writer admission was rejected"));
                });
                return output;
            }, control);
        }

        private CompletionStage<Void> closeSession() {
            return session == null ? CompletableFuture.completedFuture(null) : session.closeAsync();
        }

        synchronized CompletionStage<PublicationOutcome> publish() {
            if (publication == null || publication.isCompletedExceptionally()
                    || publication.getNow(PublicationOutcome.OUTCOME_UNKNOWN) == PublicationOutcome.OUTCOME_UNKNOWN
                    && publication.isDone()
                    || publication.getNow(PublicationOutcome.OUTCOME_UNKNOWN) == PublicationOutcome.DEFINITIVELY_NOT_APPLIED) {
                publication = CompletableFuture.supplyAsync(this::publisher, control)
                        .thenCompose(value -> value)
                        .thenComposeAsync(publisher -> activated
                                ? cancel().thenApply(ignored -> {
                                    prepared.captured.adoptSelectedCache(activation);
                                    return PublicationOutcome.EXISTING_EXACT;
                                })
                                : publisher.publish(prepared.plan, prepared.semantic, descriptor, protections(),
                                        () -> prepared.captured.publicationSnapshot(prepared.plan), prepared.captured.recoveryState(), worker)
                                        .thenComposeAsync(outcome -> activate(outcome), control), control)
                        .toCompletableFuture();
            }
            return publication.thenApply(value -> value);
        }

        private CompletionStage<KafkaBookKeeperCompactionPublicationV2> publisher() {
            if (verifiedParts == null) throw new IllegalStateException("native M5 publication lacks confirmed complete sealed output");
            if (descriptor == null) descriptor = KafkaSealedBookKeeperDescriptorV2.create(prepared.plan, prepared.semantic,
                    prepared.layout, verifiedParts, prepared.captured.recoveryState());
            CompletionStage<Void> predecessorDrain = CompletableFuture.completedFuture(null);
            if (!activated) {
                prepared.captured.persistSelectedCacheBudget(descriptor);
                predecessorDrain = opened.storage().beginNativePreferredPublication(descriptor);
            }
            if (publicationReader == null) {
                session = opened.nativeM5TaskSession(client);
                int encoded = Math.toIntExact(prepared.layout.task().parts().stream().mapToLong(part -> part.length()).reduce(0, Math::addExact));
                publicationReader = new KafkaSealedBookKeeperReaderV2(session, client::captureExactTarget, encoded,
                        prepared.captured.decodingBounds(), control);
            }
            var publisher = new KafkaBookKeeperCompactionPublicationV2(prepared.metadata, prepared.partition,
                    prepared.plan.sourceCut().identity().binding(), publicationReader, control,
                    new KafkaBookKeeperPublicationTicketsV2(guard, prepared.captured, control));
            return predecessorDrain.thenApplyAsync(ignored -> publisher, control);
        }

        private List<SourceProtectionIdentity> protections() {
            var predecessor = prepared.plan.sourceCut().predecessorSelector();
            var rows = prepared.plan.sourceCut().sources().stream().map(source -> new SourceProtectionIdentity(
                    source.sourceIdentitySha256(), descriptor.sourceGeneration(), Math.addExact(predecessor.readAdmissionEpoch(), 1),
                    descriptor.sourceGeneration(), predecessor.capability()))
                    .sorted(java.util.Comparator.comparing(row -> row.sourceIdentitySha256().toHex())).toList();
            var coordinator = new M4ReadControlCoordinatorV1(prepared.metadata, prepared.partition, predecessor.binding());
            for (var row : rows) {
                var result = coordinator.createProtection(new SourceProtection(predecessor.binding(), row,
                        ProtectionState.PROTECTED, Optional.empty(), Optional.empty()));
                if (result != M4ReadControlCoordinatorV1.Outcome.APPLIED && result != M4ReadControlCoordinatorV1.Outcome.EXISTING_EXACT)
                    throw new IllegalStateException("native M5 fallback protection is unresolved: " + result);
            }
            return rows;
        }

        private CompletionStage<PublicationOutcome> activate(PublicationOutcome outcome) {
            if (outcome == PublicationOutcome.APPLIED_EXACT || outcome == PublicationOutcome.EXISTING_EXACT) {
                return publicationReader.recover(descriptor).thenComposeAsync(view -> {
                    // Keep the formal Task charge even if exact local activation fails after retaining this cache.
                    activationAttempted = true;
                    activation = opened.storage().installNativePreferred(view);
                    activated = true;
                    prepared.captured.adoptSelectedCache(activation);
                    return cancel().thenApply(ignored -> outcome);
                }, control);
            }
            if (outcome != PublicationOutcome.OUTCOME_UNKNOWN) opened.storage().resumeNativeRawAfterNonSelection(prepared.id());
            return CompletableFuture.completedFuture(outcome);
        }

        synchronized CompletionStage<Void> cancel() {
            if (cancelling == null || cancelling.isCompletedExceptionally()) {
                cancelling = CompletableFuture.runAsync(this::connect, control).thenComposeAsync(ignored -> {
                    if (scopeTerminal != null) return CompletableFuture.completedFuture(scopeTerminal);
                    return client.fenceCreates().thenCompose(fenced -> closeSession()).thenComposeAsync(closed -> {
                        var decision = prepared.decisions().readDecision(prepared.id());
                        if (decision.filter(value -> value.outcome() == M5TaskSelectionDecisionV2.Outcome.SELECTED).isPresent()) {
                            scopeTerminal = selectedScopeTerminal(decision.orElseThrow());
                            return CompletableFuture.completedFuture(scopeTerminal);
                        }
                        // Cancel against the exact current selector before the existing terminal coordinator drains
                        // the now fenced scope. Rollover must not replace the selector with this old source cut.
                        prepared.cancelSelection();
                        return new KafkaBookKeeperTaskTerminationV2(prepared.metadata, prepared.partition,
                                prepared.layout.task(), client, control).terminate().thenApply(result -> {
                                    if (result.outcome() != KafkaBookKeeperTaskTerminationV2.Outcome.TERMINATED_UNPUBLISHED)
                                        throw new IllegalStateException("native M5 output termination is unresolved: " + result.outcome());
                                    physicalTerminal = result.terminal().orElseThrow();
                                    scopeTerminal = physicalTerminal.encode();
                                    return scopeTerminal;
                                });
                    }, control);
                }, control).thenComposeAsync(value -> {
                    terminal.complete(new Completion<>(value, Optional.of(Sha256Digest.hash(value))));
                    var cleanup = guarded == null ? CompletableFuture.<Void>completedFuture(null) : guarded.thenCompose(result -> {
                        var exact = result.terminalCleanup().orElseThrow(
                                () -> new IllegalStateException("native M5 writer lacks its original invocation cleanup"));
                        return exact.reconcile().thenApply(left -> {
                            if (!left.isEmpty()) throw new IllegalStateException("native M5 output writer tickets remain unresolved");
                            return null;
                        });
                    });
                    return cleanup.thenRunAsync(() -> {
                        if (clientClosed) return;
                        try {
                            client.close();
                            clientClosed = true;
                        } catch (Exception failure) {
                            throw new CompletionException(failure);
                        }
                    }, control);
                }, control).toCompletableFuture();
            }
            return cancelling.thenApply(ignored -> null);
        }

        private CanonicalBytes selectedScopeTerminal(M5TaskSelectionDecisionV2 decision) {
            if (descriptor == null || verifiedParts == null || !decision.taskId().equals(prepared.id())
                    || !decision.binding().equals(prepared.plan.sourceCut().identity().binding())
                    || !decision.selectedOutput().equals(Optional.of(descriptor.descriptorSha256())))
                throw new IllegalStateException("native M5 selected scope differs from its verified Task/output");
            if (!prepared.decisions().archiveCurrentDecision(prepared.id()))
                throw new IllegalStateException("native M5 selected decision archive is unresolved");
            // In-memory writer-ticket terminal evidence only. A selected output never gets an unpublished
            // TaskTerminal record; its actual frozen native scope, full descriptor and decision are retained.
            var domain = "NEREUS_M5_KAFKA_SELECTED_SDK_CLOSED_V1".getBytes(StandardCharsets.US_ASCII);
            var spec = client.spec().encode().toByteArray();
            var output = descriptor.encode().toByteArray();
            var selected = decision.encode().toByteArray();
            var bytes = ByteBuffer.allocate(Math.addExact(domain.length + 3 * Integer.BYTES,
                    Math.addExact(spec.length, Math.addExact(output.length, selected.length))))
                    .put(domain).putInt(spec.length).put(spec).putInt(output.length).put(output)
                    .putInt(selected.length).put(selected);
            return CanonicalBytes.copyOf(bytes.array());
        }

        private record WriteResult(List<VerifiedPart> parts, Throwable failure) { }
    }
}
