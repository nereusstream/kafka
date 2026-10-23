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
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperRunBirthRecoveryV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerRecoveryProofV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateSpecV2;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Completion;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Context;
import com.nereusstream.storage.object.gc.BoundPhysicalDeleteAuthorityRouteV2;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/** Binds a native run birth to quota-backed writer tickets and reconciles only proven terminal births. */
final class NereusKafkaRunBirthWriterAdmissionV1 {
    record NativeRead<T>(CompletionStage<T> result, CompletionStage<Void> cleanup) { }

    record RecoveryRead(
            CompletionStage<Optional<RunLedgerRecoveryProofV1>> result,
            CompletionStage<Void> cleanup) { }

    /** Retained only after confirmed native close; retry touches the original invocation's metadata only. */
    static final class TicketCleanupException extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final M5TargetDeleteMultiWriterGuardV2 guard;
        private final List<PhysicalResourceIdV2> resources;
        private final Context context;
        private final Sha256Digest operationId;
        private final Sha256Digest terminalProof;

        private TicketCleanupException(M5TargetDeleteMultiWriterGuardV2 guard, PhysicalResourceIdV2 resource,
                                       Context context, Sha256Digest operationId, Sha256Digest terminalProof) {
            this(guard, List.of(resource), context, operationId, terminalProof);
        }

        private TicketCleanupException(M5TargetDeleteMultiWriterGuardV2 guard, List<PhysicalResourceIdV2> resources,
                                       Context context, Sha256Digest operationId, Sha256Digest terminalProof) {
            super("native invocation terminated but its exact ticket release is unresolved");
            this.guard = guard;
            this.resources = List.copyOf(resources);
            this.context = context;
            this.operationId = operationId;
            this.terminalProof = terminalProof;
        }

        Sha256Digest operationId() {
            return operationId;
        }

        CompletionStage<List<PhysicalResourceIdV2>> reconcileTickets() {
            return guard.reconcileOperation(resources, context, operationId, terminalProof)
                    .thenApply(left -> left);
        }
    }

    /** Owns one failed source CAS; only an exact terminal observation may release its original nonce. */
    static final class SourcePublicationException extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final M5TargetDeleteMultiWriterGuardV2 guard;
        private final List<PhysicalResourceIdV2> resources;
        private final Context context;
        private final Sha256Digest operationId;
        private final Supplier<Optional<CanonicalBytes>> terminal;
        private final AtomicReference<Sha256Digest> terminalProof = new AtomicReference<>();

        private SourcePublicationException(M5TargetDeleteMultiWriterGuardV2 guard, List<PhysicalResourceIdV2> resources,
                Context context, Sha256Digest operationId, Throwable failure, Supplier<Optional<CanonicalBytes>> terminal) {
            super("native source publication failed with unresolved original invocation tickets", failure);
            this.guard = guard;
            this.resources = List.copyOf(resources);
            this.context = context;
            this.operationId = operationId;
            this.terminal = terminal;
        }

        CompletionStage<List<PhysicalResourceIdV2>> reconcileTickets() {
            if (terminalProof.get() != null)
                return guard.reconcileOperation(resources, context, operationId, terminalProof.get()).thenApply(left -> left);
            final Optional<CanonicalBytes> observed;
            try {
                observed = terminal.get();
            } catch (RuntimeException unknown) {
                return CompletableFuture.completedFuture(resources);
            }
            if (observed.isEmpty()) return CompletableFuture.completedFuture(resources);
            var facts = observed.orElseThrow().toByteArray();
            var proof = Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(132 + facts.length)
                    .putInt(0x4e4b5354).put(operationId.bytes().toByteArray())
                    .put(context.capabilitySha256().bytes().toByteArray())
                    .put(context.ownerFenceSha256().bytes().toByteArray())
                    .put(context.externalFactsRootSha256().bytes().toByteArray()).put(facts).array()));
            terminalProof.compareAndSet(null, proof);
            return guard.reconcileOperation(resources, context, operationId, terminalProof.get()).thenApply(left -> left);
        }
    }

    private record SourcePublicationResult<T>(T value, Throwable failure) { }

    private final BoundPhysicalDeleteAuthorityRouteV2 route;
    private final BookKeeperCapabilitySnapshotV1 capability;
    private final M5TargetDeleteMultiWriterGuardV2 guard;
    private final OxiaPhysicalMemberInventoryV2 members;

    NereusKafkaRunBirthWriterAdmissionV1(
            BoundPhysicalDeleteAuthorityRouteV2 route, BookKeeperCapabilitySnapshotV1 capability,
            OxiaPhysicalMemberInventoryV2 members) {
        this.route = route;
        this.capability = capability;
        this.members = java.util.Objects.requireNonNull(members, "native physical members");
        guard = new M5TargetDeleteMultiWriterGuardV2(new M5TargetDeleteAuthorityCoordinatorV1(route));
    }

    M5TargetDeleteMultiWriterGuardV2 inputReadGuard() {
        return guard;
    }

    CompletionStage<Void> registerMember(PhysicalResourceIdV2 resource, Sha256Digest protocolCell, KafkaRunRootRecordV2.Scope scope,
            M5BookKeeperNativeCreateSpecV2 spec) {
        return members.register(resource, protocolCell, nativeMember(scope),
                capability.configurationDigest(), spec.sha256()).thenApply(ignored -> null);
    }

    private OxiaPhysicalMemberInventoryV2.Member nativeMember(KafkaRunRootRecordV2.Scope scope) {
        var binding = new BindingIdentity(scope.bindingId(),
                Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(scope.topic())), scope.storageEpoch().digest());
        return new OxiaPhysicalMemberInventoryV2.Member(PositionDomain.KAFKA_OFFSET,
                capability.providerScopeId().digest(), binding);
    }

    private CompletionStage<Void> requireRegisteredMembers(List<PhysicalResourceIdV2> resources,
            Sha256Digest protocolCell, KafkaRunRootRecordV2.Scope scope) {
        var expected = nativeMember(scope);
        CompletionStage<Void> confirmed = CompletableFuture.completedFuture(null);
        for (var resource : resources)
            confirmed = confirmed.thenCompose(ignored -> members.readFact(resource).thenApply(observed -> {
                if (!observed.inventory().protocolCell().equals(protocolCell)
                        || !observed.inventory().members().contains(expected))
                    throw new IllegalStateException("native source publication has another physical member identity");
                return null;
            }));
        return confirmed;
    }

    CompletionStage<Void> admitWriterAuthority(PhysicalResourceIdV2.BookKeeperLedger resource,
            M5BookKeeperNativeCreateSpecV2 spec, M5GcQuotaCoordinatorV2 quota, long deadline,
            Function<CompletionStage<Void>, CompletionStage<Void>> afterOpen) {
        var encodedAuthority = initialWriterAuthority(resource, spec);
        // OPEN authorizes writers; it does not establish M4 release or physical deletion eligibility.
        return NereusKafkaOwnedProviderRuntime.afterGrantedQuota(reserveAuthority(quota, resource, deadline, 128),
                () -> afterOpen.apply(route.compareAndSet(Optional.empty(), resource.authorityKey(), encodedAuthority)
                        .thenApply(outcome -> null))
                .handle((ignored, failure) -> failure == null ? "persisted response" : failure.toString())
                .thenCompose(response -> route.read(resource.authorityKey()).thenApply(observed -> {
                    if (observed.isEmpty() || !matchesOpenWriterAuthority(
                            encodedAuthority, observed.orElseThrow().canonicalStoredBytes()))
                        throw new IllegalStateException("native writer authority open unresolved: " + response);
                    return null;
                })));
    }

    private CompletionStage<Void> reserveAuthority(M5GcQuotaCoordinatorV2 quota,
            PhysicalResourceIdV2 resource, long deadline, int attempts) {
        return quota.reserve(resource).thenCompose(result -> {
            if (result == M5GcQuotaCoordinatorV2.Result.GRANTED) return CompletableFuture.completedFuture(null);
            if (result == M5GcQuotaCoordinatorV2.Result.RETRY && attempts > 1 && System.nanoTime() < deadline)
                return reserveAuthority(quota, resource, deadline, attempts - 1);
            return quota.describeUsage().handle((usage, failure) -> {
                var rejected = new IllegalStateException("native physical authority quota: " + result
                        + (failure == null ? "; " + usage : "; accounting observation unavailable"));
                if (failure != null) rejected.addSuppressed(failure);
                throw rejected;
            });
        });
    }

    /** Serializes one exact source-reference publication, never an ordinary local M4 read. */
    <T> CompletionStage<T> publishReadSources(KafkaOwnerIdentityV1 owner, List<PhysicalResourceIdV2> resources,
            Sha256Digest selectedView, Sha256Digest protocolCell, KafkaRunRootRecordV2.Scope scope,
            Supplier<CompletionStage<T>> publication, Supplier<Optional<CanonicalBytes>> failedTerminal) {
        var targets = M5TargetDeleteMultiWriterGuardV2.canonicalTargets(resources);
        var context = new Context(
                M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1.REFERENCE_SHARED_PHYSICAL_MEMBER_V1,
                capability.configurationDigest(), ownerFenceDigest(owner), selectedView);
        var terminal = Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(100)
                .putInt(0x4e4b5350).put(selectedView.bytes().toByteArray())
                .put(ownerFenceDigest(owner).bytes().toByteArray())
                .put(capability.configurationDigest().bytes().toByteArray()).array()));
        return requireRegisteredMembers(targets, protocolCell, scope).thenCompose(ignored -> guard.execute(targets, context, () -> {
            CompletionStage<T> actual;
            try {
                actual = java.util.Objects.requireNonNull(publication.get(), "source publication stage");
            } catch (Throwable failure) {
                actual = CompletableFuture.failedFuture(failure);
            }
            return actual.handle((value, failure) -> {
                return new Completion<>(new SourcePublicationResult<>(value, failure),
                        failure == null ? Optional.of(terminal) : Optional.empty());
            });
        })).thenApply(settled -> {
            if (!settled.mutationInvoked())
                throw new IllegalStateException("native M4 source reference admission was rejected");
            var result = settled.value().orElseThrow(() -> new IllegalStateException("native M4 source publication is unresolved"));
            if (result.failure() != null)
                throw new SourcePublicationException(guard, targets, context, settled.operationId(), result.failure(), failedTerminal);
            if (!settled.unresolvedTargets().isEmpty())
                throw new TicketCleanupException(guard, targets, context, settled.operationId(), terminal);
            return result.value();
        });
    }

    CanonicalBytes initialWriterAuthority(
            PhysicalResourceIdV2.BookKeeperLedger resource, M5BookKeeperNativeCreateSpecV2 spec) {
        var enrollment = new M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterEnrollmentV1(
                List.of(M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1.values()),
                capability.configurationDigest(),
                capability.clientArtifactSha256(),
                spec.sha256());
        return M5TargetDeleteAuthorityCodecV1.encodeAuthority(M5TargetDeleteAuthorityStateMachineV1.open(
                M5TargetDeleteAuthorityRecordsV1.PhysicalDeleteTargetV1.create(resource),
                enrollment,
                resource.sha256()));
    }

    boolean matchesOpenWriterAuthority(CanonicalBytes expectedInitial, CanonicalBytes observed) {
        var initial = M5TargetDeleteAuthorityCodecV1.decodeAuthority(expectedInitial);
        var current = M5TargetDeleteAuthorityCodecV1.decodeAuthority(observed);
        return current.state() == TargetDeleteAuthorityStateV1.OPEN_V1
                && current.closedWriterFenceEpoch() == 0
                && current.eligibilitySnapshot().isEmpty()
                && current.target().equals(initial.target())
                && current.writerEnrollment().equals(initial.writerEnrollment());
    }

    CompletionStage<ProviderMutationResultV1<RunLedgerHandleV1>> createUnderTicket(
            BookKeeperCellSession delegate,
            M5BookKeeperNativeCreateSpecV2 spec,
            KafkaOwnerIdentityV1 owner,
            RunLedgerConfigurationV1 config,
            BookKeeperLedgerIdentity reservedIdentity,
            PhysicalResourceIdV2.BookKeeperLedger resource,
            Function<CompletionStage<ProviderMutationResultV1<RunLedgerHandleV1>>,
                    CompletionStage<ProviderMutationResultV1<RunLedgerHandleV1>>> afterCreate) {
        return guard.execute(List.of(resource), ticketContext(owner, spec), () -> afterCreate
                        .apply(delegate.createReservedRunLedger(config, reservedIdentity))
                        .thenCompose(created -> {
                            if (created.outcome() != ProviderMutationOutcomeV1.APPLIED_EXACT) {
                                return CompletableFuture.completedFuture(new Completion<>(created, Optional.empty()));
                            }
                            var handle = created.exactProof().orElseThrow();
                            if (!handle.runId().equals(config.runId())
                                    || !handle.ledgerIdentity().equals(reservedIdentity)) {
                                return CompletableFuture.completedFuture(new Completion<>(
                                        ProviderMutationResultV1.<RunLedgerHandleV1>outcomeUnknown(),
                                        Optional.empty()));
                            }
                            return delegate.openRunLedger(handle).thenApply(open -> {
                                if (!open.exactHandle().equals(Optional.of(handle))) {
                                    return new Completion<>(
                                            ProviderMutationResultV1.<RunLedgerHandleV1>outcomeUnknown(),
                                            Optional.empty());
                                }
                                var terminal = Sha256Digest.hash(CanonicalBytes.copyOf(
                                        (spec.sha256().toHex() + ":"
                                                + resource.sha256().toHex() + ":"
                                                + handle.runId().value().toHex())
                                                .getBytes(StandardCharsets.US_ASCII)));
                                return new Completion<>(created, Optional.of(terminal));
                            });
                        }))
                .thenApply(result -> {
                    // A confirmed BK create is not selected while its durable writer ticket is unresolved.
                    // The closed-owner birth inventory will recover the exact ledger and settle that ticket.
                    if (!result.unresolvedTargets().isEmpty()) {
                        return ProviderMutationResultV1.<RunLedgerHandleV1>outcomeUnknown();
                    }
                    return result.mutationInvoked()
                            ? result.value().orElseGet(ProviderMutationResultV1::outcomeUnknown)
                            : ProviderMutationResultV1.fencedOrConflict();
                });
    }

    CompletionStage<Void> fenceClosedRunBirth(
            M5BookKeeperNativeCreateClientV2 source,
            KafkaRunRootRecordV2 record,
            RunLedgerHandleV1 handle,
            M5BookKeeperNativeCreateSpecV2 expected) {
        var resource = record.resource();
        var expectedOpen = initialWriterAuthority(resource, expected);
        return route.requireActiveResource(resource)
                .thenCompose(binding -> source.requireNamespaceBinding().thenApply(nativeBinding -> {
                    if (!binding.equals(nativeBinding)) {
                        throw new IllegalStateException("closed run birth has another native namespace binding");
                    }
                    return null;
                }))
                .thenCompose(ignored -> route.read(resource.authorityKey()))
                .thenCompose(observed -> {
                    if (observed.isEmpty()
                            || !matchesOpenWriterAuthority(
                                    expectedOpen, observed.orElseThrow().canonicalStoredBytes())) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("closed run birth lacks exact quota-backed OPEN"));
                    }
                    return source.fenceExistingScope(expected, handle);
                });
    }

    CompletionStage<Void> reconcileClosedRunBirth(
            KafkaOwnerAdmissionV1 closedOwner,
            KafkaRunRootRecordV2 record,
            RunLedgerHandleV1 handle,
            M5BookKeeperNativeCreateSpecV2 expected,
            Optional<RunLedgerRecoveryProofV1> recovered) {
        if (recovered.filter(proof -> !proof.handle().equals(handle)).isPresent()) {
            return CompletableFuture.failedFuture(new IllegalStateException("closed birth recovery changed its handle"));
        }
        var resource = record.resource();
        var ownerDigest = ownerFenceDigest(closedOwner.owner());
        var terminal = Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(117)
                .putInt(0x4e4b4254)
                .put(ownerDigest.bytes().toByteArray())
                .put(expected.sha256().bytes().toByteArray())
                .put(record.initialLink().initialRootSha256().bytes().toByteArray())
                .putLong(handle.ledgerIdentity().ledgerId())
                .putLong(recovered.map(RunLedgerRecoveryProofV1::lastAddConfirmed).orElse(-2L))
                .put((byte) (recovered.isPresent() ? 1 : 0))
                .array()));
        var context = ticketContext(closedOwner.owner(), expected);
        return route.requireActiveResource(resource)
                .thenCompose(ignored -> guard.reconcileTerminal(List.of(resource), context, terminal))
                .thenCompose(unresolved -> unresolved.isEmpty() ? CompletableFuture.completedFuture(unresolved)
                        // Reader cleanup may change the same authority after the TASK candidate was prepared.
                        // Reconcile the already-proven closed task once more; never repeat native recovery or reads.
                        : guard.reconcileTerminal(List.of(resource), context, terminal))
                .thenAccept(unresolved -> {
                    if (!unresolved.isEmpty()) {
                        throw new IllegalStateException("closed run birth has unresolved physical create tickets");
                    }
                });
    }

    /** The current recovery owner's invocation stays protected until its actual reader session closes. */
    RecoveryRead recoverUnderReadTicket(
            KafkaOwnerIdentityV1 recoveryOwner,
            KafkaRunRootRecordV2 record,
            RunLedgerHandleV1 handle,
            M5BookKeeperNativeCreateSpecV2 oldScope,
            Supplier<BookKeeperCellSession> reader,
            Supplier<CompletionStage<Void>> closeReader) {
        var read = readUnderReadTicket(recoveryOwner, record, oldScope,
                () -> KafkaBookKeeperRunBirthRecoveryV1.recoverNative(reader.get(), handle), closeReader);
        return new RecoveryRead(read.result(), read.cleanup());
    }

    /** The same invocation owns either birth recovery or exact root verification until native close confirms. */
    <T> NativeRead<T> readUnderReadTicket(
            KafkaOwnerIdentityV1 recoveryOwner, KafkaRunRootRecordV2 record,
            M5BookKeeperNativeCreateSpecV2 oldScope, Supplier<CompletionStage<T>> nativeReadOperation,
            Supplier<CompletionStage<Void>> closeReader) {
        return readUnderTickets(recoveryOwner, List.of(record.resource()), oldScope.sha256(),
                nativeReadOperation, closeReader);
    }

    <T> NativeRead<T> readSelectedUnderReadTickets(
            KafkaOwnerIdentityV1 recoveryOwner, List<PhysicalResourceIdV2> resources, Sha256Digest descriptor,
            Supplier<CompletionStage<T>> nativeReadOperation, Supplier<CompletionStage<Void>> closeReader) {
        return readUnderTickets(recoveryOwner, M5TargetDeleteMultiWriterGuardV2.canonicalTargets(resources), descriptor,
                nativeReadOperation, closeReader);
    }

    private <T> NativeRead<T> readUnderTickets(
            KafkaOwnerIdentityV1 recoveryOwner, List<PhysicalResourceIdV2> resources, Sha256Digest sourceFacts,
            Supplier<CompletionStage<T>> nativeReadOperation, Supplier<CompletionStage<Void>> closeReader) {
        var result = new CompletableFuture<T>();
        var cleanup = new CompletableFuture<Void>();
        var closeFailure = new AtomicReference<Throwable>();
        var readContext = new Context(
                M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1.OWNER_WORKER_LEASE_HANDLE_PIN_V1,
                capability.configurationDigest(), ownerFenceDigest(recoveryOwner), sourceFacts);
        var physical = ByteBuffer.allocate(resources.size() * Sha256Digest.LENGTH);
        resources.forEach(resource -> physical.put(resource.sha256().bytes().toByteArray()));
        var physicalDigest = resources.size() == 1 ? resources.get(0).sha256()
                : Sha256Digest.hash(CanonicalBytes.copyOf(physical.array()));
        var terminal = Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(100)
                .putInt(0x4e4b5244)
                .put(readContext.ownerFenceSha256().bytes().toByteArray())
                .put(physicalDigest.bytes().toByteArray())
                .put(sourceFacts.bytes().toByteArray())
                .array()));
        var work = guard.execute(resources, readContext, () -> {
            CompletionStage<T> nativeRead;
            try {
                nativeRead = java.util.Objects.requireNonNull(nativeReadOperation.get(), "native read stage");
            } catch (Throwable failure) {
                nativeRead = CompletableFuture.failedFuture(failure);
            }
            return nativeRead.handle((proof, failure) -> {
                // Start actual draining before publishing the native protocol result. Closing the reader is
                // independent of whether a valid native recovery or a metadata-specific absence was observed.
                var closing = closeReader(closeReader);
                if (failure == null) result.complete(proof);
                else result.completeExceptionally(failure);
                return closing.handle((ignored, failedClose) -> {
                    if (failedClose != null) {
                        closeFailure.set(failedClose);
                        return new Completion<>(false, Optional.empty());
                    }
                    // execute discharges only the nonce it acquired for this reader invocation.
                    return new Completion<>(true, Optional.of(terminal));
                });
            }).thenCompose(closing -> closing);
        });
        work.whenComplete((settled, failure) -> {
            if (failure != null || !settled.mutationInvoked()) {
                var denied = failure != null ? failure
                        : new IllegalStateException("native recovery reader pin was not acquired");
                result.completeExceptionally(denied);
                closeReader(closeReader).whenComplete((ignored, failedClose) -> {
                    if (failedClose != null) denied.addSuppressed(failedClose);
                    cleanup.completeExceptionally(denied);
                });
            } else if (closeFailure.get() != null) {
                cleanup.completeExceptionally(closeFailure.get());
            } else if (!settled.value().equals(Optional.of(true))) {
                cleanup.completeExceptionally(new IllegalStateException("native recovery reader close was not confirmed"));
            } else if (!settled.unresolvedTargets().isEmpty()) {
                cleanup.completeExceptionally(new TicketCleanupException(
                        guard, resources, readContext, settled.operationId(), terminal));
            } else {
                cleanup.complete(null);
            }
        });
        return new NativeRead<>(result.thenApply(value -> value), cleanup.thenApply(ignored -> null));
    }

    private static CompletionStage<Void> closeReader(Supplier<CompletionStage<Void>> closeReader) {
        try {
            return java.util.Objects.requireNonNull(closeReader.get(), "reader close returned no stage");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private Context ticketContext(KafkaOwnerIdentityV1 owner, M5BookKeeperNativeCreateSpecV2 spec) {
        return new Context(
                M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1.TASK_PROJECTION_MIGRATION_RECOVERY_V1,
                capability.configurationDigest(),
                ownerFenceDigest(owner),
                spec.sha256());
    }

    private Sha256Digest ownerFenceDigest(KafkaOwnerIdentityV1 owner) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(36)
                .putInt(0x4e4b4f46)
                .putLong(owner.ownerEpoch())
                .putInt(owner.kafkaLeaderEpoch())
                .putInt(owner.brokerId())
                .putLong(owner.brokerEpoch())
                .putLong(owner.metadataOffset())
                .array()));
    }
}
