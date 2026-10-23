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

import org.apache.kafka.storage.internals.log.LogConfig;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1.NativeInputSources;
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1.NativePreferredActivation;
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1.NativeSelectedInputSources;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperPublicationTicketsV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperControlMetadataStoreV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperReadCellBudgetV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperRunSourceV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperReadOwnerV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperSelectedSourceV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperMixedSourceV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecoveryStateV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperSelectedCacheBudgetV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperDescriptorV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionAdmissionV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionAdmissionV1.CellCaps;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionAdmissionV1.Reservation;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Caps;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.CompactionPlan;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Frontiers;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.InputBatch;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.OutputCompressionPolicy;
import com.nereusstream.metadata.oxia.v2.compaction.OxiaKafkaRunRootAuthorityV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaKafkaBookKeeperM4ReadRouteV2;
import com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.retention.M5MaterializationWorkerCoordinatorV1;
import com.nereusstream.domain.bytes.CanonicalBytes;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** Broker-owned sealed input membership. Worker leases and compaction semantic proofs remain separate authorities. */
public final class NereusKafkaNativeInputV2 {
    private final NereusKafkaOwnedProviderRuntime.Opened opened;
    private final NativeInputSources nativeCut;
    private final TopicPolicy topicPolicy;
    private final KafkaOwnerIdentityV1 owner;
    private final OxiaKafkaRunRootAuthorityV2 roots;
    private final OxiaKafkaBookKeeperM4ReadRouteV2 m4;
    private final BooleanSupplier current;
    private final Executor control;
    private final BindingIdentity binding;
    private final List<KafkaRunRootSnapshotV1> completeRoots;
    private final List<KafkaRunRootSnapshotV1> inputRoots;
    private final List<String> keys;
    private final Frontiers frontiers;
    private final AtomicReference<BindingReadSelector> selector = new AtomicReference<>();
    private final KafkaBookKeeperRunSourceV2 source;
    private final Optional<NativeSelectedInputSources> selectedInput;
    private final KafkaBookKeeperMixedSourceV2 mixedSource;
    private final KafkaBookKeeperRunSourceV2.Bounds bounds;
    private final M5BookKeeperNativeCreateClientV2 nativeClient;
    private final M5TargetDeleteMultiWriterGuardV2 writerGuard;

    record NativeCut(NativeInputSources inputs, TopicPolicy policy) { }

    /** Failed preparation whose actual reads, tickets and persisted capacity have all terminated. */
    public static final class CaptureReleasedException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private CaptureReleasedException(Throwable failure) {
            super(failure.getMessage(), failure);
        }
    }

    static CaptureReleasedException preparationNotDispatched(Throwable failure) {
        return new CaptureReleasedException(failure);
    }

    private static final class TerminatedCaptureException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private TerminatedCaptureException(Throwable failure) {
            super(failure.getMessage(), failure);
        }
    }

    /** Operator resource allowances, never source, owner, semantic or publication qualification. */
    public record CaptureAllowance(Caps planCaps, long backlogAgeMs, long spillBytes,
            long providerOperations, long kmsOperations, long metadataOperations, long responseUnknowns) {
        public CaptureAllowance {
            Objects.requireNonNull(planCaps, "planCaps");
            if (backlogAgeMs < 0 || spillBytes < 0 || providerOperations <= 0 || kmsOperations < 0
                    || metadataOperations <= 0 || responseUnknowns <= 0)
                throw new IllegalArgumentException("native M5 capture resource allowance is invalid");
        }

        private Reservation reservation(Sha256Digest attempt) {
            return new Reservation(attempt, planCaps.maximumDirtyBytes(), planCaps.maximumInputBatches(),
                    planCaps.maximumRecords(), planCaps.maximumDistinctKeys(), planCaps.maximumKeyBytes(),
                    planCaps.maximumOutputBytes(), planCaps.maximumIndexBytes(), planCaps.maximumTransactions(),
                    planCaps.maximumTombstones(), backlogAgeMs, spillBytes, providerOperations, kmsOperations,
                    metadataOperations, responseUnknowns);
        }
    }

    /** Actual effective Kafka configuration, stamped only after matching a published KRaft image. */
    public static final class TopicPolicy {
        private final LogConfig config;
        private final long metadataOffset;
        private final short featureLevel;

        TopicPolicy(LogConfig config, long metadataOffset, short featureLevel) {
            this.config = Objects.requireNonNull(config, "native topic config");
            if (metadataOffset < 0 || featureLevel < 0)
                throw new IllegalArgumentException("native M5 policy lacks metadata provenance");
            this.metadataOffset = metadataOffset;
            this.featureLevel = featureLevel;
        }

        public LogConfig config() {
            return config;
        }

        public long metadataOffset() {
            return metadataOffset;
        }

        public long generation() {
            return Math.addExact(metadataOffset, 1);
        }

        public short kafkaFeatureLevel() {
            return featureLevel;
        }

        public OutputCompressionPolicy compressionPolicy() {
            return switch (config.compressionType) {
                case UNCOMPRESSED -> OutputCompressionPolicy.NONE;
                case GZIP -> OutputCompressionPolicy.GZIP;
                case SNAPPY -> OutputCompressionPolicy.SNAPPY;
                case LZ4 -> OutputCompressionPolicy.LZ4;
                case ZSTD -> OutputCompressionPolicy.ZSTD;
                case PRODUCER -> OutputCompressionPolicy.PRODUCER;
            };
        }

        private boolean samePolicy(TopicPolicy other) {
            return metadataOffset == other.metadataOffset && featureLevel == other.featureLevel
                    && config.values().equals(other.config.values());
        }
    }

    NereusKafkaNativeInputV2(NereusKafkaOwnedProviderRuntime.Opened opened, NativeCut cut,
            KafkaOwnerIdentityV1 owner, OxiaKafkaRunRootAuthorityV2 roots, OxiaKafkaBookKeeperM4ReadRouteV2 m4,
            BooleanSupplier current, M5BookKeeperNativeCreateClientV2 client, M5TargetDeleteMultiWriterGuardV2 guard,
            KafkaBookKeeperReadCellBudgetV2 budget, KafkaBookKeeperRunSourceV2.Bounds bounds, Executor control) {
        this.opened = opened;
        this.nativeCut = cut.inputs();
        this.topicPolicy = cut.policy();
        var nativeCut = this.nativeCut;
        this.owner = Objects.requireNonNull(owner, "native owner");
        this.roots = Objects.requireNonNull(roots, "native roots");
        this.m4 = Objects.requireNonNull(m4, "native selector route");
        this.current = Objects.requireNonNull(current, "native assignment");
        this.control = control;
        this.bounds = bounds;
        this.nativeClient = Objects.requireNonNull(client, "native source client");
        this.writerGuard = Objects.requireNonNull(guard, "native writer guard");
        var active = nativeCut.sources().activeRoot();
        binding = new BindingIdentity(active.bindingId(),
                Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(active.topicIncarnation())),
                active.storageEpochId().digest());
        var all = new ArrayList<>(nativeCut.sources().sealedRoots());
        all.add(active);
        completeRoots = List.copyOf(all);
        selectedInput = opened.storage().nativeSelectedInputSources(nativeCut, m4.readSelector().orElseThrow());
        long selectedEnd = selectedInput.map(input -> input.descriptor().sourceCut().coverage().exclusiveEnd()).orElse(-1L);
        inputRoots = nativeCut.sources().sealedRoots().stream()
                .filter(root -> root.kafkaEndOffsetExclusive().orElseThrow() > root.kafkaStartOffset())
                .filter(root -> root.kafkaEndOffsetExclusive().orElseThrow() > selectedEnd).toList();
        if (inputRoots.isEmpty() && selectedInput.isEmpty())
            throw new IllegalStateException("native M5 cut has no sealed input data");
        var checkpoint = nativeCut.recoveryCheckpoint().orElseThrow(
                () -> new IllegalStateException("native M5 cut lacks its resolved recovery checkpoint"));
        if (!checkpoint.vector().isAlignedCompoundCheckpoint()
                || checkpoint.vector().recoveryCoveredThrough() != active.kafkaStartOffset())
            throw new IllegalStateException("native M5 checkpoint differs from the sealed input boundary");
        keys = inputRoots.stream().map(root -> roots.nativeRootKey(root.runId())).toList();
        var actual = nativeCut.protocol().root().frontiers();
        long start = selectedInput.map(input -> input.descriptor().sourceCut().coverage().inclusiveStart())
                .orElseGet(() -> inputRoots.get(0).kafkaStartOffset());
        long candidateEnd = inputRoots.isEmpty() ? selectedEnd
                : inputRoots.get(inputRoots.size() - 1).kafkaEndOffsetExclusive().orElseThrow();
        frontiers = new Frontiers(actual.trimStartOffset(), actual.durableEndOffset(), actual.allocatedEndOffset(),
                actual.highWatermark(), actual.lastStableOffset(), start, candidateEnd);
        long end = selectedInput.isPresent() ? selectedEnd : start;
        for (var root : inputRoots) {
            if (root.kafkaStartOffset() != end) throw new IllegalStateException("native M5 sealed cut has a hole");
            end = root.kafkaEndOffsetExclusive().orElseThrow();
        }
        source = new KafkaBookKeeperRunSourceV2(budget, this::readSelectedInput, client, guard, bounds, control,
                opened::openInputSession);
        if (selectedInput.isEmpty()) {
            mixedSource = null;
        } else {
            var metadata = new KafkaBookKeeperControlMetadataStoreV2(m4.compactionRecords(), m4.rawBindingControl(),
                    active.partitionId(), binding, client.spec().namespace(), client.capabilitySnapshot().providerScopeId());
            var selectedBounds = new KafkaBookKeeperSelectedSourceV2.Bounds(bounds.entries(), bounds.batches(),
                    bounds.records(), Math.toIntExact(bounds.nativeBytes()), bounds.decodedBytes());
            var selected = KafkaBookKeeperSelectedSourceV2.forNativeOwner(budget, metadata, m4.control(), client,
                    guard, selectedBounds, opened.nativeM5TaskControl(), selectedInput.orElseThrow(), opened::openSelectedInputSession);
            mixedSource = new KafkaBookKeeperMixedSourceV2(budget, selected, source);
        }
    }

    CompletionStage<Captured> capture() {
        return scan().thenApply(snapshots -> new Captured(snapshots, null));
    }

    /** Reserves before the catalog/SDK scan and retains the allowance with the returned capture. */
    CompletionStage<Captured> capture(CellCaps caps, CaptureAllowance allowance) {
        if (bounds.payloadBytes() > allowance.planCaps().maximumDirtyBytes()
                || bounds.batches() > allowance.planCaps().maximumInputBatches()
                || bounds.records() > allowance.planCaps().maximumRecords())
            throw new IllegalArgumentException("native scan exceeds its preparation allowance");
        var admission = opened.nativeM5CompactionAdmission();
        // Each accepted invocation consumes its own slot, even when owner and coherent cut are identical.
        var nonce = UUID.randomUUID();
        byte[] domain = "NEREUS_M5_KAFKA_CAPTURE_RESERVATION_V1".getBytes(StandardCharsets.US_ASCII);
        var attempt = ByteBuffer.allocate(domain.length + Long.BYTES * 3 + Sha256Digest.LENGTH * 2)
                .put(domain).putLong(owner.ownerEpoch()).put(nativeCut.protocol().root().references().sourceMap().contentDigest().bytes().toByteArray())
                .put(opened.nativeM5NamespaceBinding().bytes().toByteArray()).putLong(nonce.getMostSignificantBits()).putLong(nonce.getLeastSignificantBits());
        var reservation = new CellReservation(admission, allowance.reservation(Sha256Digest.hash(CanonicalBytes.copyOf(attempt.array()))),
                allowance, nonce.getLeastSignificantBits() & Long.MAX_VALUE, control);
        opened.retainM5Reservation(reservation);
        final CompletionStage<Void> reserved;
        try {
            reserved = CompletableFuture.runAsync(() -> {
                boolean reservationDispatched = false;
                try {
                    requireCurrentLocal();
                    requireExact(admission.install(caps), "Cell cap installation");
                    reservationDispatched = true;
                    var result = admission.reserve(reservation.value);
                    if (result == KafkaCompactionAdmissionV1.Outcome.REJECTED_CAP) opened.forgetM5Reservation(reservation);
                    requireExact(result, "capture reservation");
                } catch (Throwable failure) {
                    if (!reservationDispatched) {
                        opened.forgetM5Reservation(reservation);
                        throw preparationNotDispatched(failure);
                    }
                    throw failure;
                }
            }, control);
        } catch (Throwable failure) {
            opened.forgetM5Reservation(reservation);
            return CompletableFuture.failedFuture(failure);
        }
        return reserved.thenCompose(ignored -> reservation.own(() -> scan().thenApply(snapshots ->
                terminatedLocal(() -> new Captured(snapshots, reservation)))).handle((captured, failure) -> {
                    if (failure == null) return CompletableFuture.completedFuture(captured);
                    var cause = unwrap(failure);
                    if (!terminated(cause)) return CompletableFuture.<Captured>failedFuture(cause);
                    return reservation.discard().<Captured>handle((released, cleanupFailure) -> {
                        if (cleanupFailure != null) {
                            cause.addSuppressed(unwrap(cleanupFailure));
                            throw new java.util.concurrent.CompletionException(cause);
                        }
                        opened.forgetM5Reservation(reservation);
                        throw new java.util.concurrent.CompletionException(new CaptureReleasedException(cause));
                    });
                }).thenCompose(result -> result))
                .thenApply(value -> value);
    }

    private static Throwable unwrap(Throwable failure) {
        while (failure instanceof java.util.concurrent.CompletionException && failure.getCause() != null)
            failure = failure.getCause();
        return failure;
    }

    private static boolean terminated(Throwable failure) {
        return failure instanceof KafkaBookKeeperRunSourceV2.TerminatedReadException
                || failure instanceof KafkaBookKeeperReadOwnerV2.TerminatedReadException
                || failure instanceof TerminatedCaptureException;
    }

    /** Called only for local validation after the raw source operation has confirmed complete termination. */
    private static <T> T terminatedLocal(java.util.function.Supplier<T> validation) {
        try {
            return validation.get();
        } catch (Throwable failure) {
            throw new TerminatedCaptureException(failure);
        }
    }

    private static void requireExact(KafkaCompactionAdmissionV1.Outcome result, String operation) {
        if (result != KafkaCompactionAdmissionV1.Outcome.APPLIED_EXACT
                && result != KafkaCompactionAdmissionV1.Outcome.EXISTING_EXACT)
            throw new IllegalStateException("native M5 " + operation + " is unresolved: " + result);
    }

    /** Kept by the actual Owner until all native readers and ticket cleanup have confirmed drain. */
    static final class CellReservation {
        private final KafkaCompactionAdmissionV1 admission;
        private final Reservation value;
        private final CaptureAllowance allowance;
        private final long attempt;
        private final Executor control;
        private NereusKafkaPreparedTaskV2 prepared;
        private boolean taskPreparationStarted;
        private int pending;
        private boolean discarded;
        private boolean unresolved;
        private boolean releasing;
        private boolean ownerCache;
        private Sha256Digest selectedView;
        private CompletableFuture<Void> release;

        private CellReservation(KafkaCompactionAdmissionV1 admission, Reservation value, CaptureAllowance allowance, long attempt, Executor control) {
            this.admission = admission;
            this.value = value;
            this.allowance = allowance;
            this.attempt = attempt;
            this.control = control;
        }

        static CellReservation selectedCache(KafkaCompactionAdmissionV1 admission, Reservation value, Sha256Digest descriptor, Executor control) {
            var reservation = new CellReservation(admission, value, null, 0, control);
            reservation.ownerCache = true;
            reservation.selectedCacheInstalled(descriptor);
            return reservation;
        }

        synchronized boolean ownsSelectedCache(Sha256Digest descriptor) {
            return descriptor.equals(selectedView);
        }

        synchronized void selectedCacheInstalled(Sha256Digest descriptor) {
            if (selectedView != null && !selectedView.equals(descriptor))
                throw new IllegalStateException("native selected cache reservation changed its descriptor");
            selectedView = descriptor;
        }

        void reserveSelectedCache() {
            if (!ownerCache || admission.read().isEmpty())
                throw new IllegalStateException("selected cache lacks actual installed Cell capacity");
            requireExact(admission.reserve(value), "selected cache capacity");
        }

        synchronized void startTaskPreparation() {
            requireOpen();
            if (taskPreparationStarted) throw new IllegalStateException("native M5 capture already started task preparation");
            taskPreparationStarted = true;
        }

        synchronized void retainTask(NereusKafkaPreparedTaskV2 task) {
            requireOpen();
            if (!taskPreparationStarted || prepared != null) throw new IllegalStateException("native M5 task preparation differs");
            prepared = task;
        }

        void convertToTask() {
            // Retain the complete finite envelope while input, semantic and physical-layout bytes remain live.
            requireExact(admission.replace(value, allowance.reservation(preparedTask().id())), "task capacity conversion");
        }

        private synchronized NereusKafkaPreparedTaskV2 preparedTask() {
            return prepared;
        }

        void requireFormalTask(NereusKafkaPreparedTaskV2 task) {
            if (preparedTask() != task || !admission.read().orElseThrow(
                    () -> new IllegalStateException("native M5 task capacity is unresolved")).reservations()
                    .contains(allowance.reservation(task.id())))
                throw new IllegalStateException("native M5 task lacks its exact retained capacity");
        }

        synchronized void requireOpen() {
            if (discarded) throw new IllegalStateException("native M5 capture was discarded");
        }

        <T> CompletionStage<T> own(java.util.function.Supplier<CompletionStage<T>> operation) {
            return own(operation, false);
        }

        <T> CompletionStage<T> ownOutput(java.util.function.Supplier<CompletionStage<T>> operation) {
            return own(operation, true);
        }

        private <T> CompletionStage<T> own(java.util.function.Supplier<CompletionStage<T>> operation, boolean outputOwned) {
            synchronized (this) {
                requireOpen();
                pending++;
            }
            final CompletionStage<T> actual;
            try {
                actual = Objects.requireNonNull(operation.get(), "native M5 capture operation");
            } catch (Throwable failure) {
                finished(failure, outputOwned);
                return CompletableFuture.failedFuture(failure);
            }
            // The observer's cancelled future cannot finish this actual operation or release its capacity.
            return actual.whenComplete((ignored, failure) -> finished(failure, outputOwned)).thenApply(value -> value);
        }

        private synchronized void finished(Throwable failure, boolean outputOwned) {
            pending--;
            // Output failures retain their private Task SDK/guard owner, which must prove termination on discard.
            // Independent input/read failures still require actual Owner drain and cannot be discharged by it.
            unresolved |= failure != null && !outputOwned && !terminated(unwrap(failure));
            if (discarded && pending == 0) finishRelease();
        }

        synchronized CompletionStage<Void> discard() {
            discarded = true;
            if (release == null || release.isCompletedExceptionally()) {
                release = new CompletableFuture<>();
                releasing = false;
            }
            if (pending == 0) finishRelease();
            return release.thenApply(ignored -> null);
        }

        synchronized void releaseAfterOwnerDrain() {
            discarded = true;
            if (pending != 0) throw new IllegalStateException("native M5 capture operations have not drained");
            if (release == null || release.isCompletedExceptionally()) {
                release = new CompletableFuture<>();
                releasing = false;
            }
            // Owner close already runs on the control executor. Do not enqueue work behind other closing owners
            // that are waiting on the same executor; this generation fence also settles a queued release retry.
            releaseReservations(true);
            release.complete(null);
        }

        synchronized void releaseAfterSelectedCacheDrain() {
            if (unresolved) throw new IllegalStateException("native superseded cache retains unresolved input work");
            releaseAfterOwnerDrain();
        }

        private void releaseReservations(boolean ownerDrained) {
            var prepared = preparedTask();
            if (prepared != null) {
                // A dispatched Task fences/drains its own SDK and reconciles exact writer tickets first.
                // An inert prepared task still archives non-selection; UNKNOWN retains both possible IDs.
                prepared.cancelOwnedTask();
                releaseReservation(prepared.id(), true, "prepared task release");
            }
            // Fence the formal ID first: even an absent reread must invalidate a delayed conversion before the
            // old preparation slot is removed. Owner drain also excludes a delayed initial reservation CAS.
            releaseReservation(value.compactionTaskIdSha256(), ownerDrained || prepared != null, "capture release");
        }

        private void releaseReservation(Sha256Digest id, boolean fencePendingReservation, String operation) {
            var outcome = KafkaCompactionAdmissionV1.Outcome.CONFLICT;
            // Other Owners share this Cell table. A forward Head may mean either a lost CAS or
            // an applied cancellation whose reply/reread was overtaken. Rebase only this already
            // drained, idempotent release intent, preserving all latest sibling reservations.
            for (int remaining = KafkaCompactionAdmissionV1.MAX_RESERVATIONS; remaining > 0; remaining--) {
                outcome = fencePendingReservation ? admission.cancelPreparation(id) : admission.release(id);
                if (outcome != KafkaCompactionAdmissionV1.Outcome.CONFLICT) break;
            }
            // A true UNKNOWN is never retried here. An absent formal ID still requires the
            // successful cancelPreparation CAS which fences a delayed initial reservation.
            requireExact(outcome, operation);
        }

        private void finishRelease() {
            if (release.isDone() || releasing) return;
            // The selected cache remains owned by the Broker. A discarded observer cannot discharge its charge.
            if (ownerCache || prepared != null && prepared.requiresOwnerCacheDrain()) return;
            if (unresolved) {
                release.completeExceptionally(new IllegalStateException("native M5 capture failure retains its Cell reservation until Owner drain"));
                return;
            }
            releasing = true;
            var completion = release;
            try {
                // Exact metadata calls may block; never run them on a native/Oxia response callback.
                CompletableFuture.runAsync(() -> releaseReservations(false), control)
                        .whenComplete((ignored, failure) -> {
                            if (failure == null) completion.complete(null);
                            else completion.completeExceptionally(unwrap(failure));
                        });
            } catch (Throwable failure) {
                completion.completeExceptionally(failure);
            }
        }
    }

    private record Scan(Optional<KafkaBookKeeperSelectedSourceV2.Snapshot> selected,
            List<KafkaBookKeeperRunSourceV2.Snapshot> raw) {
        private Scan {
            raw = List.copyOf(raw);
        }

        List<SourceExtent> extents() {
            var result = new ArrayList<SourceExtent>();
            selected.ifPresent(snapshot -> result.add(snapshot.extent()));
            raw.forEach(snapshot -> result.add(snapshot.extent()));
            return List.copyOf(result);
        }

        List<InputBatch> batches() {
            var result = new ArrayList<InputBatch>();
            selected.ifPresent(snapshot -> result.addAll(snapshot.batches()));
            raw.forEach(snapshot -> result.addAll(snapshot.batches()));
            return List.copyOf(result);
        }

        void requireBounds(Caps caps) {
            var inputs = batches();
            long encoded = 0;
            for (var input : inputs) encoded = Math.addExact(encoded, input.canonicalBody().length());
            long records = extents().stream().mapToLong(SourceExtent::recordCount).sum();
            if (inputs.size() > caps.maximumInputBatches() || encoded > caps.maximumDirtyBytes()
                    || records > caps.maximumRecords())
                throw new IllegalStateException("native M5 combined capture exceeds retained input capacity");
        }

        Map<Sha256Digest, List<PhysicalResourceIdV2>> membership() {
            Map<Sha256Digest, List<PhysicalResourceIdV2>> result = new LinkedHashMap<>();
            selected.ifPresent(snapshot -> result.put(snapshot.extent().sourceIdentitySha256(), snapshot.resources()));
            for (var snapshot : raw) {
                if (result.put(snapshot.extent().sourceIdentitySha256(), List.of(snapshot.root().resource())) != null)
                    throw new IllegalStateException("native M5 source identity repeats");
            }
            return Map.copyOf(result);
        }
    }

    private CompletionStage<Scan> scan() {
        CompletionStage<Scan> scanned;
        if (mixedSource == null) {
            // The raw reader reserves before invoking its catalog adapter.
            scanned = source.captureSelectedRuns(keys, binding).thenApply(raw -> new Scan(Optional.empty(), raw));
        } else {
            scanned = CompletableFuture.runAsync(this::requireSelector, control).thenCompose(ignored -> requireOwnerAdmission())
                    .thenCompose(ignored -> mixedSource.capture(keys, binding))
                    .thenApply(snapshot -> new Scan(Optional.of(snapshot.selected()), snapshot.raw()));
        }
        return scanned.thenApplyAsync(snapshot -> terminatedLocal(() -> {
            requireCurrentLocal();
            return snapshot;
        }), control);
    }

    private void requireSelector() {
        requireCurrentLocal();
        var value = m4.readSelector().orElseThrow(() -> new IllegalStateException("native M5 selector is absent"));
        var view = selectedInput.map(input -> input.descriptor().descriptorSha256())
                .orElse(nativeCut.sources().selectedViewSha256());
        if (!value.binding().equals(binding) || value.ownerEpoch() != owner.ownerEpoch()
                || value.admissionState() != AdmissionState.ADMITTING || value.mode() != SelectorMode.PREFERRED_ONLY
                || !value.selectedViewSha256().equals(view))
            throw new IllegalStateException("native M5 selector is outside the current source generation");
        selector.compareAndSet(null, value);
        if (!selector.get().equals(value)) throw new IllegalStateException("native M5 predecessor selector changed");
    }

    private CompletionStage<KafkaOwnerAdmissionV1> requireOwnerAdmission() {
        return roots.readOwnerAdmission().thenApply(found -> {
            var admission = found.orElseThrow(() -> new IllegalStateException("native M5 owner admission is absent"));
            if (admission.closed() || !admission.owner().equals(owner))
                throw new IllegalStateException("native M5 owner is closed or replaced");
            var expectedRuns = selectedInput.map(NativeSelectedInputSources::ownerRunIds)
                    .orElseGet(() -> completeRoots.stream().filter(root -> root.creatorOwnerEpoch() == owner.ownerEpoch())
                            .map(KafkaRunRootSnapshotV1::runId).toList());
            if (!admission.runs().stream().map(KafkaRunRootRecordV2.Link::runId).toList().equals(expectedRuns))
                throw new IllegalStateException("native M5 cut omits or adds current owner runs");
            return admission;
        });
    }

    private void requireCurrentLocal() {
        if (!current.getAsBoolean()) throw new IllegalStateException("native M5 owner assignment changed");
        // This coherent log/metadata-image observation is local and dispatches no native or Oxia operation.
        var cut = terminatedLocal(opened::nativeM5InputCut);
        if (!topicPolicy.samePolicy(cut.policy())) throw new IllegalStateException("native M5 topic policy changed");
        var now = cut.inputs();
        if (!now.protocol().root().fence().equals(nativeCut.protocol().root().fence())
                || !now.sources().selectedViewSha256().equals(nativeCut.sources().selectedViewSha256())
                || !now.protocol().root().frontiers().noRegressionFrom(nativeCut.protocol().root().frontiers())
                || now.protocol().root().frontiers().trimStartOffset() > frontiers.candidateStartOffset())
            throw new IllegalStateException("native M5 input source generation or coherent cut changed");
        selectedInput.ifPresent(NativeSelectedInputSources::requireCurrent);
    }

    private CompletionStage<Optional<KafkaRunRootRecordV2>> readSelectedInput(String key) {
        int index = keys.indexOf(key);
        if (index < 0) return CompletableFuture.failedFuture(new IllegalArgumentException("foreign native M5 input key"));
        var expected = inputRoots.get(index);
        return CompletableFuture.runAsync(this::requireSelector, control)
                .thenCompose(ignored -> requireOwnerAdmission())
                .thenCompose(admission -> expected.creatorOwnerEpoch() == owner.ownerEpoch()
                        ? CompletableFuture.completedFuture(admission)
                        : roots.readClosedOwner(expected.creatorOwnerEpoch()).thenApply(old -> old.orElseThrow(
                            () -> new IllegalStateException("native M5 archived input owner is absent"))))
                .thenCompose(admission -> readAdmittedInput(admission, expected, key))
                .thenApplyAsync(record -> {
                    requireCurrentLocal();
                    return record;
                }, control);
    }

    private CompletionStage<Optional<KafkaRunRootRecordV2>> readAdmittedInput(KafkaOwnerAdmissionV1 admission,
            KafkaRunRootSnapshotV1 expected, String key) {
        if (admission.owner().ownerEpoch() != expected.creatorOwnerEpoch()
                || expected.creatorOwnerEpoch() != owner.ownerEpoch() && !admission.closed()
                || !admission.scopeSha256().equals(Sha256Digest.hash(KafkaRunRootRecordV2.Scope.of(expected).encode())))
            throw new IllegalStateException("native M5 input owner differs from the exact protocol scope");
        var link = admission.runs().stream().filter(value -> value.runId().equals(expected.runId())).findFirst()
                .orElseThrow(() -> new IllegalStateException("native M5 input is outside its owner's admitted run set"));
        return roots.readSelectedRoot(key).thenApply(found -> {
            var record = found.orElseThrow(() -> new IllegalStateException("native M5 input root is not selected"));
            if (record.retired() || !record.root().equals(expected) || !record.initialLink().equals(link))
                throw new IllegalStateException("native M5 input root differs from the exact admitted member");
            if (record.recoveryCut().filter(cut -> !admission.closed()
                    || !cut.closedOwnerSha256().equals(Sha256Digest.hash(admission.encode()))).isPresent())
                throw new IllegalStateException("native M5 recovered cut differs from its closed owner archive");
            return found;
        });
    }

    /** Constructed only from this Broker's admitted native catalog and bytes; callers cannot import a capture. */
    public final class Captured implements KafkaBookKeeperPublicationTicketsV2.InputMembership {
        private final Scan snapshot;
        private final List<SourceExtent> extents;
        private final List<InputBatch> batches;
        private final KafkaCompactionRecoveryStateV1 recoveryState;
        private final CellReservation reservation;

        private Captured(Scan snapshot, CellReservation reservation) {
            if (reservation != null) snapshot.requireBounds(reservation.allowance.planCaps());
            this.snapshot = snapshot;
            extents = snapshot.extents();
            batches = snapshot.batches();
            recoveryState = KafkaCompactionRecoveryStateV1.capture(nativeCut.protocol(), nativeCut.recoveryCheckpoint().orElseThrow());
            this.reservation = reservation;
        }

        /** Invalidates further use and waits for actual accepted scans before releasing retained-input capacity. */
        public CompletionStage<Void> discard() {
            if (reservation == null) return CompletableFuture.failedFuture(new IllegalStateException("diagnostic capture has no persisted reservation"));
            return reservation.discard().thenRun(() -> opened.forgetM5Reservation(reservation)).thenApply(ignored -> null);
        }

        void requireOpen() {
            if (reservation != null) reservation.requireOpen();
        }

        M5TargetDeleteMultiWriterGuardV2 writerGuard() {
            return writerGuard;
        }

        public NativeInputSources nativeSources() {
            requireOpen();
            return nativeCut;
        }

        public KafkaCompactionRecoveryStateV1 recoveryState() {
            requireOpen();
            return recoveryState;
        }

        public TopicPolicy topicPolicy() {
            requireOpen();
            return topicPolicy;
        }

        public BindingReadSelector predecessorSelector() {
            requireOpen();
            return selector.get();
        }

        public Frontiers frontiers() {
            requireOpen();
            return frontiers;
        }

        boolean dirtyInput(InputBatch batch) {
            return extents.stream().anyMatch(extent -> extent.kind() == SourceKind.BOOKKEEPER_LEDGER
                    && extent.sourceIdentitySha256().equals(batch.sourceIdentitySha256()));
        }

        public List<SourceExtent> extents() {
            requireOpen();
            return extents;
        }

        public List<InputBatch> batches() {
            requireOpen();
            return batches;
        }

        /** Derives every semantic input from this retained Broker cut; proposals themselves admit no worker. */
        public CompletionStage<CompactionPlan> preparePlan() {
            if (reservation == null)
                return CompletableFuture.failedFuture(new IllegalStateException("native M5 plan requires retained Cell capacity"));
            return opened.ownInputOperation(() -> reservation.own(() -> proposeWorker().thenApplyAsync(proposal ->
                terminatedLocal(() -> {
                    requireOpen();
                    requireCurrentLocal();
                    var plan = NereusKafkaCompactionPlanV2.build(this, proposal.identity(), reservation.allowance.planCaps(), bounds.decodedBytes());
                    recoveryState.requirePlan(plan);
                    return plan;
                }), control)));
        }

        /** Registers the actual semantic/recovery layout and converts retained capacity before returning a task. */
        public CompletionStage<NereusKafkaPreparedTaskV2> prepareTask(int maximumEntryBytes, long maximumPartBytes) {
            if (reservation == null)
                return CompletableFuture.failedFuture(new IllegalStateException("native M5 task requires retained Cell capacity"));
            return opened.ownInputOperation(() -> reservation.own(() -> {
                terminatedLocal(() -> {
                    reservation.startTaskPreparation();
                    return null;
                });
                return proposeWorker().thenApplyAsync(proposal -> {
                    var prepared = terminatedLocal(() -> {
                        requireOpen();
                        requireCurrentLocal();
                        var plan = NereusKafkaCompactionPlanV2.buildForTask(this, proposal.identity(), reservation.allowance.planCaps(), bounds.decodedBytes());
                        var metadata = new KafkaBookKeeperControlMetadataStoreV2(m4.compactionRecords(), m4.rawBindingControl(),
                                nativeCut.sources().activeRoot().partitionId(), binding, nativeClient.spec().namespace(),
                                nativeClient.capabilitySnapshot().providerScopeId());
                        var task = NereusKafkaPreparedTaskV2.build(this, plan, metadata, nativeClient, reservation.attempt,
                                maximumEntryBytes, maximumPartBytes);
                        reservation.retainTask(task);
                        return task;
                    });
                    // Metadata work is deliberately outside the local-only terminated classification: a lost
                    // registration/replace response may still reach Oxia and retains the possible formal task ID.
                    prepared.register();
                    reservation.convertToTask();
                    return prepared;
                }, control);
            }));
        }

        /** Rechecks the actual input and formal capacity before private worker/output dispatch. */
        CompletionStage<List<com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionWriterV2.VerifiedPart>> materialize(
                NereusKafkaPreparedTaskV2 prepared) {
            return opened.ownInputOperation(() -> reservation.ownOutput(() -> {
                terminatedLocal(() -> {
                    requireOpen();
                    reservation.requireFormalTask(prepared);
                    prepared.beginMaterialization();
                    return null;
                });
                return proposeWorker().thenComposeAsync(proposal -> {
                    requireOpen();
                    requireCurrentLocal();
                    reservation.requireFormalTask(prepared);
                    return prepared.write(proposal, opened, control);
                }, control);
            }));
        }

        CompletionStage<com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome> publish(
                NereusKafkaPreparedTaskV2 prepared) {
            return opened.ownInputOperation(() -> reservation.ownOutput(() -> {
                requireOpen();
                reservation.requireFormalTask(prepared);
                return prepared.publishOwned(opened);
            }));
        }

        /** Revalidate the retained cut's actual owner, policy, named prefix and resolved checkpoint before CAS. */
        com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionPublicationFenceV1.Snapshot publicationSnapshot(CompactionPlan plan) {
            requireOpen();
            requireCurrentLocal();
            reservation.requireFormalTask(reservation.preparedTask());
            var currentCut = opened.nativeM5InputCut().inputs();
            var checkpoint = currentCut.recoveryCheckpoint().orElseThrow(
                    () -> new IllegalStateException("native publication checkpoint is absent"));
            var state = KafkaCompactionRecoveryStateV1.capture(currentCut.protocol(), checkpoint);
            if (!state.equals(recoveryState) || !plan.inputBatches().equals(batches) || !plan.sourceCut().sources().equals(extents))
                throw new IllegalStateException("native publication retained cut/protocol state changed");
            return new com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionPublicationFenceV1().expected(plan);
        }

        com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperReaderV2.DecodingBounds decodingBounds() {
            return new com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperReaderV2.DecodingBounds(bounds.records(), bounds.decodedBytes());
        }

        void adoptSelectedCache(NativePreferredActivation activation) {
            activation.requireTask(reservation.preparedTask().id());
            reservation.selectedCacheInstalled(activation.descriptorSha256());
            activation.replacedDescriptorSha256().ifPresent(opened::releaseSupersededM5Cache);
        }

        /** Preserve the actual finite cache envelope before the selected output can become durable. */
        void persistSelectedCacheBudget(KafkaSealedBookKeeperDescriptorV2 descriptor) {
            requireOpen();
            var task = reservation.preparedTask();
            reservation.requireFormalTask(task);
            var budget = new KafkaBookKeeperSelectedCacheBudgetV1(descriptor.descriptorSha256(), decodingBounds(),
                    reservation.allowance.reservation(task.id()));
            budget.requireDescriptor(descriptor);
            var metadata = new KafkaBookKeeperControlMetadataStoreV2(m4.compactionRecords(), m4.rawBindingControl(),
                    nativeCut.sources().activeRoot().partitionId(), binding, descriptor.task().namespace(),
                    nativeCut.sources().activeRoot().providerScopeId());
            metadata.putIfAbsent(budget.key(), budget.encode());
            if (!metadata.get(budget.key()).equals(Optional.of(budget.encode())))
                throw new IllegalStateException("native selected cache budget persistence is unresolved");
        }

        /** Prepares a real owner/scope/storage fence; no output work is admitted until a complete task is bound. */
        CompletionStage<M5MaterializationWorkerCoordinatorV1.Proposal> proposeWorker() {
            return opened.ownInputOperation(() -> verifyInputs(extents, batches).thenApplyAsync(ignored -> {
                terminatedLocal(() -> {
                    requireOpen();
                    requireCurrentLocal();
                    return null;
                });
                var workers = m4.workers();
                if (!workers.migrateLegacyAuthority())
                    throw new IllegalStateException("native M5 worker authority migration is unresolved");
                byte[] domain = "NEREUS_M5_KAFKA_NATIVE_WORKER_OWNER_V1".getBytes(StandardCharsets.US_ASCII);
                var bytes = ByteBuffer.allocate(domain.length + 3 * Long.BYTES + 2 * Integer.BYTES)
                        .put(domain).putLong(owner.ownerEpoch()).putInt(owner.kafkaLeaderEpoch())
                        .putInt(owner.brokerId()).putLong(owner.brokerEpoch()).putLong(owner.metadataOffset());
                return workers.propose(selector.get(), opened.nativeM5ProtocolCell(),
                        nativeCut.sources().activeRoot().providerScopeId().digest(), opened.nativeM5StorageFence(),
                        Sha256Digest.hash(CanonicalBytes.copyOf(bytes.array())), opened.nativeM5NamespaceBinding())
                        .orElseThrow(() -> new IllegalStateException("native M5 worker predecessor changed"));
            }, control));
        }

        /** Exact native membership recheck, for use before the separately authorized publication CAS. */
        public CompletionStage<Map<Sha256Digest, List<PhysicalResourceIdV2>>> verifyInputs(
                List<SourceExtent> proposedExtents, List<InputBatch> proposedBatches) {
            if (!extents.equals(proposedExtents) || !batches.equals(proposedBatches))
                return CompletableFuture.failedFuture(new IllegalArgumentException("native M5 complete ordered inputs differ"));
            return opened.ownInputOperation(() -> reservation == null ? verifyScan() : reservation.own(this::verifyScan));
        }

        private CompletionStage<Map<Sha256Digest, List<PhysicalResourceIdV2>>> verifyScan() {
            requireOpen();
            return scan().thenApply(actual -> terminatedLocal(() -> {
                if (!actual.extents().equals(extents) || !actual.batches().equals(batches))
                    throw new IllegalStateException("native M5 inputs changed after capture");
                return snapshot.membership();
            }));
        }

        @Override
        public CompletionStage<Map<Sha256Digest, List<PhysicalResourceIdV2>>> resolve(CompactionPlan plan) {
            var cut = plan.sourceCut();
            var coverage = new ProtocolCoverage(PositionDomain.KAFKA_OFFSET,
                    frontiers.candidateStartOffset(), frontiers.candidateEndOffsetExclusive());
            if (!cut.identity().binding().equals(binding) || cut.identity().ownerEpoch() != owner.ownerEpoch()
                    || !cut.identity().providerScopeSha256().equals(nativeCut.sources().activeRoot().providerScopeId().digest()))
                return CompletableFuture.failedFuture(new IllegalArgumentException("M5 plan differs from the native owner/cut"));
            if (!cut.predecessorSelector().equals(selector.get())
                    || !cut.predecessorSelectorValueSha256().equals(Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(selector.get())))
                    || !cut.predecessorViewSha256().equals(selector.get().selectedViewSha256()))
                return CompletableFuture.failedFuture(new IllegalArgumentException("M5 plan differs from the native owner/cut"));
            if (!cut.coverage().equals(coverage) || !plan.frontiers().equals(frontiers))
                return CompletableFuture.failedFuture(new IllegalArgumentException("M5 plan differs from the native owner/cut"));
            return verifyInputs(cut.sources(), plan.inputBatches());
        }
    }
}
