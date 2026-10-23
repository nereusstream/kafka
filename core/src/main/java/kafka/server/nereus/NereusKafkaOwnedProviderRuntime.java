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

import kafka.log.nereus.NereusUnifiedLog;
import kafka.server.storage.BrokerStorageRuntimeContext;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.errors.FencedLeaderEpochException;
import org.apache.kafka.common.metadata.TopicBindingAggregateRecord;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1;
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperReadCellBudgetV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperRunSourceV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionAdmissionV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperControlMetadataStoreV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionPublicationV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryCodecV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperSelectedCacheBudgetV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperDescriptorCodecV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperReaderV2;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperClosedHistoryRecoveryV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendCapacityBudgetV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendCapacityControllerV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionPublicationObserver;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperRunBirthRecoveryV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperNativeRootVerifierV2;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.oxia.v2.compaction.OxiaKafkaRunRootAuthorityV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaKafkaBookKeeperM4ReadRouteV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.storage.api.bookkeeper.AppendQuorumProofV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerCloseProofV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerOpenResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerRecoveryProofV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateSpecV2;
import com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1;
import com.nereusstream.storage.object.gc.BoundPhysicalDeleteAuthorityRouteV2;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.retention.M5TaskSelectionCoordinatorV2;
import com.nereusstream.storage.object.retention.M5TaskSelectionDecisionV2;

import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.UUID;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;

/** Owns real BK/Oxia connections. Native assignment is checked by the lifecycle supplied current predicate. */
public final class NereusKafkaOwnedProviderRuntime implements AutoCloseable {
    /** Test observer for completed native responses and recovery; real BK/Oxia operations always run first. */
    public interface NativeRunResponseObserver {
        default AsyncOxiaClient controlClient(AsyncOxiaClient client) {
            return client;
        }
        /** Fault-injection route for one Broker; production uses the exact bound route unchanged. */
        default BoundPhysicalDeleteAuthorityRouteV2 writerAuthorityRoute(
                BoundPhysicalDeleteAuthorityRouteV2 route) {
            return route;
        }

        default CompletionStage<Void> afterOpen(CompletionStage<Void> response) {
            return response;
        }

        default CompletionStage<ProviderMutationResultV1<RunLedgerHandleV1>> afterCreate(
                CompletionStage<ProviderMutationResultV1<RunLedgerHandleV1>> response) {
            return response;
        }

        default CompletionStage<ProviderMutationResultV1<AppendQuorumProofV1>> afterAppend(
                RunLedgerAppendRequestV1 request, CompletionStage<ProviderMutationResultV1<AppendQuorumProofV1>> response) {
            return response;
        }

        default CompletionStage<M5BookKeeperNativeCreateClientV2.RecoveredScopeFacts> afterRecoveredScopeFacts(
                CompletionStage<M5BookKeeperNativeCreateClientV2.RecoveredScopeFacts> response) {
            return response;
        }

        default CompletionStage<Void> afterBirthReaderClose(CompletionStage<Void> response) {
            return response;
        }

        default BookKeeperCellSession birthReaderSession(BookKeeperCellSession session) {
            return session;
        }

        default BookKeeperCellSession compactionSession(BookKeeperCellSession session) {
            return session;
        }

        default void afterBirthReaderTicketCleanup(Throwable failure) { }

        default CompletionStage<Void> afterRootVerifierClose(KafkaRunRootRecordV2 record, CompletionStage<Void> response) {
            return response;
        }

        default void afterRootVerifierTicketCleanup(KafkaRunRootRecordV2 record, Throwable failure) { }

        default void afterRootVerifierAdmissionFailure(KafkaRunRootRecordV2 record, int maximumHandles, Throwable failure) { }

        default BookKeeperCellSession historyReaderSession(KafkaRunRootRecordV2 record,
                KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase phase, BookKeeperCellSession session) {
            return session;
        }

        default CompletionStage<Void> afterHistoryReaderClose(KafkaRunRootRecordV2 record,
                KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase phase, CompletionStage<Void> response) {
            return response;
        }

        default void afterHistoryReaderTicketCleanup(KafkaRunRootRecordV2 record,
                KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase phase, Throwable failure) { }

        default void afterHistoryReaderAdmissionFailure(KafkaRunRootRecordV2 record,
                KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase phase, Throwable failure) { }

        default void afterRecovery(org.apache.kafka.common.TopicIdPartition partition, Throwable failure) { }

        default void afterReady(org.apache.kafka.common.TopicIdPartition partition, KafkaBookKeeperPartitionV1 storage) { }

        default void afterReady(org.apache.kafka.common.TopicIdPartition partition, Opened owner) {
            afterReady(partition, owner.storage());
        }

        default CompletionStage<Optional<KafkaRunRootRecordV2>> afterHistoricalFetchRecordRead(
                com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1 root,
                CompletionStage<Optional<KafkaRunRootRecordV2>> response) {
            return response;
        }

        default void afterReadAdmissionClosed(Opened owner) { }

        default void beforeNativeSourcePublication(Opened owner, KafkaBookKeeperPartitionV1.NativeReadSources sources) { }
    }

    private final BrokerStorageRuntimeContext context;
    private final NativeRunResponseObserver runResponses;
    private final ThreadPoolExecutor openings;
    private final List<Opened> owned = new ArrayList<>();
    private final KafkaAppendCapacityControllerV1 globalCapacity;
    private BookKeeperCapabilitySnapshotV1 capability;
    private Optional<NereusKafkaM4AdmissionV2> m4Admission = Optional.empty();
    private Optional<NereusKafkaM5PolicyInputV1> m5PolicyInput = Optional.empty();
    private ExecutorService m4ReadOwnerLoop;
    private final Semaphore cellReadSlots;
    private final Semaphore m5PreparationSlots;
    private AsyncOxiaClient oxia;
    private M5BookKeeperNamespaceAuthorityV2 backend;
    private OxiaPhysicalMetadataNamespaceV2 namespace;
    private PhysicalNamespaceAuthorityBindingV2 namespaceBinding;
    private OxiaQuotaTargetDeleteStoreV2 route;
    private NereusKafkaRunBirthWriterAdmissionV1 writerAdmission;
    private boolean closed;
    private boolean backendReleased;
    private boolean oxiaReleased;

    public NereusKafkaOwnedProviderRuntime(BrokerStorageRuntimeContext context) {
        this(context, new NativeRunResponseObserver() { });
    }

    public NereusKafkaOwnedProviderRuntime(
            BrokerStorageRuntimeContext context, NativeRunResponseObserver runResponses) {
        this.context = context;
        this.runResponses = Objects.requireNonNull(runResponses, "runResponses");
        var config = context.config().nereusKafkaStorageConfig();
        cellReadSlots = new Semaphore(config.bookKeeper().orElseThrow().maxReadsInFlight());
        m5PreparationSlots = new Semaphore(config.retentionCompaction().compactionMaxConcurrentTasks());
        openings = new ThreadPoolExecutor(
                config.lifecycle().recoveryExecutorThreads(),
                config.lifecycle().recoveryExecutorThreads(),
                0,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(config.lifecycle().executorQueueCapacity()),
                task -> {
                    var thread = new Thread(
                            task, "nereus-bk-recovery-" + context.config().brokerId());
                    thread.setDaemon(true);
                    return thread;
                });
        globalCapacity = new KafkaAppendCapacityControllerV1(new KafkaAppendCapacityBudgetV1(
                config.append().executorThreads() + config.append().executorQueueCapacity(),
                config.bookKeeper().orElseThrow().maxWritesInFlight(),
                config.append().inflightBytes()));
    }

    public CompletionStage<Void> start() {
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        var config = context.config().nereusKafkaStorageConfig();
                        if (config.core().profile()
                                != org.apache.kafka.server.config.NereusKafkaStorageConfig.Profile
                                        .BOOKKEEPER_WAL_ONLY) {
                            throw new IllegalArgumentException(
                                    "NSIP-1 native activation currently admits BOOKKEEPER_WAL_ONLY; Object authority is pending");
                        }
                        capability = NereusBookKeeperCapability.loadAndValidate(context.config());
                        m4Admission = NereusKafkaM4AdmissionV2.load(context.config(), capability);
                        m5PolicyInput = NereusKafkaM5PolicyInputV1.load(context.config(), capability);
                        if (m5PolicyInput.isPresent() && m4Admission.isEmpty()) {
                            throw new IllegalArgumentException("M5 sealed-target selection requires actual M4 admission");
                        }
                        oxia = runResponses.controlClient(await(OxiaClientBuilder.create(
                                        config.core().oxiaServiceAddress().orElseThrow())
                                .namespace(config.core().oxiaNamespace())
                                .requestTimeout(config.lifecycle().recoveryTimeout())
                                .asyncClient()));
                        backend = M5BookKeeperNamespaceAuthorityV2.connect(
                                config.core()
                                        .bookKeeperMetadataServiceUri()
                                        .orElseThrow()
                                        .toString(),
                                capability);
                        var bound = await(backend.readBinding());
                        namespace = await(
                                bound.isPresent()
                                        ? OxiaPhysicalMetadataNamespaceV2.connect(
                                                oxia, bound.orElseThrow().metadataNamespace())
                                        : OxiaPhysicalMetadataNamespaceV2.provision(oxia));
                        namespaceBinding = await(namespace.bind(backend));
                        if (m5PolicyInput.isPresent()) {
                            // Native epoch capacity must precede the first permanent ledger reservation.
                            // Existing counters/limits are verified without importing or expanding old history.
                            await(backend.nativeDeleteQuota().initialize(500_000_000L));
                            int slots = config.bookKeeper().orElseThrow().ledgerGc().maxConcurrentDeletes();
                            await(backend.nativeDeleteCellBudget().initialize(
                                    new com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteCellBudgetV2.Limits(slots, slots)));
                        }
                        route = await(
                                namespace.openAuthorityRoute(backend, new Oxia09ExactMetadataTransactionStoreV1(oxia)));
                        await(route.initialize(500_000_000L));
                        writerAdmission = new NereusKafkaRunBirthWriterAdmissionV1(
                                Objects.requireNonNull(runResponses.writerAuthorityRoute(route)), capability, route.members());
                        if (m4Admission.isPresent()) {
                            m4ReadOwnerLoop = Executors.newSingleThreadExecutor(task -> {
                                var thread = new Thread(task, "nereus-m4-read-owner-" + context.config().brokerId());
                                thread.setDaemon(true);
                                return thread;
                            });
                        }
                    } catch (Exception failure) {
                        throw new CompletionException(failure);
                    }
                },
                openings);
    }

    public CompletionStage<Opened> open(
            TopicIdPartition identity,
            TopicBindingAggregateRecord aggregate,
            int leaderEpoch,
            long brokerEpoch,
            long metadataOffset,
            BooleanSupplier current,
            KafkaPartitionPublicationObserver observer) {
        return CompletableFuture.supplyAsync(
                () -> {
                    Opened opened = null;
                    try {
                        if (!current.getAsBoolean())
                            throw new FencedLeaderEpochException("native assignment changed before BK open");
                        NereusBookKeeperCapability.validateAggregate(context.config(), aggregate);
                        var scope = NereusBookKeeperCapability.nativeScope(identity, aggregate, capability);
                        await(namespace.requireCurrent());
                        var m4 = new OxiaKafkaBookKeeperM4ReadRouteV2(oxia, namespace.identity(), scope);
                        if (m4.readSelector().isPresent() && m4Admission.isEmpty()) {
                            throw new IllegalStateException(
                                    "native BK read owner cannot start with an unqualified M4 selector");
                        }
                        long ownerEpoch = Math.addExact((long) leaderEpoch, 1);
                        var owner = new KafkaOwnerIdentityV1(
                                ownerEpoch, leaderEpoch, context.config().brokerId(), brokerEpoch, metadataOffset);
                        var spec = initialRunSpec(
                                scope, ownerEpoch,
                                M5BookKeeperNativeCreateClientV2.discoverInstanceId(uri(), capability));
                        prepareSourceBootstrap(m4, spec, identity, current, scope);
                        var source =
                                M5BookKeeperNativeCreateClientV2.connect(uri(), capability, spec, namespaceBinding);
                        opened = ownSource(source, scope);
                        var session = opened.session;
                        var roots = await(namespace.openKafkaRunRoots(
                                backend,
                                new Oxia09ExactMetadataTransactionStoreV1(oxia),
                                scope,
                                opened.rootVerifier(owner)));
                        opened.nativeOwner = owner;
                        opened.nativeScope = scope;
                        opened.nativeStorageFence = Math.addExact(aggregate.epochOrdinal(), 1);
                        opened.nativeProtocolCell = NereusBookKeeperCapability.protocolCell(aggregate);
                        opened.rootsAuthority = roots;
                        opened.m4Route = m4;
                        opened.currentAssignment = current;
                        var previous = await(roots.readOwnerAdmission());
                        Optional<KafkaOwnerAdmissionV1> closedOwner = Optional.empty();
                        if (previous.isPresent()) {
                            if (!owner.succeeds(previous.orElseThrow().owner()))
                                throw new IllegalStateException("native owner does not succeed durable admission");
                            if (!current.getAsBoolean())
                                throw new FencedLeaderEpochException(
                                        "native assignment changed before admission close");
                            closedOwner = previous.orElseThrow().closed()
                                    ? previous
                                    : Optional.of(await(roots.closeOwner(
                                                    previous.orElseThrow().owner()))
                                            .exactProof()
                                            .orElseThrow(
                                                    () -> new IllegalStateException("old owner close is unresolved")));
                        }
                        var config = context.config().nereusKafkaStorageConfig();
                        var bk = config.bookKeeper().orElseThrow();
                        var fence = new KafkaPartitionFenceV1(
                                scope.bindingId(),
                                scope.topic(),
                                scope.partition(),
                                1,
                                scope.storageEpoch(),
                                ownerEpoch,
                                leaderEpoch);
                        var recoveryOwner = opened;
                        var selected = opened.recoverNativeSelectedPrefix(m4.readSelector(), closedOwner);
                        opened.storage = await(KafkaBookKeeperPartitionV1.openSelected(
                                new AdmittingSession(session, source, owner, opened),
                                roots,
                                roots,
                                owner,
                                new Nbke2RunBindingV1(
                                        scope.bindingId(),
                                        scope.topic(),
                                        scope.partition(),
                                        scope.storageEpoch(),
                                        ownerEpoch,
                                        leaderEpoch,
                                        scope.providerScope(),
                                        runId(scope, ownerEpoch)),
                                fence,
                                closedOwner,
                                new KafkaAppendCapacityControllerV1(new KafkaAppendCapacityBudgetV1(
                                        bk.maxWritesInFlight(),
                                        bk.maxWritesInFlight(),
                                        config.append().inflightBytes())),
                                globalCapacity,
                                new KafkaBookKeeperRecoveryEnvelopeV1(
                                        bk.maxEntriesPerLedger(),
                                        bk.maxBytesPerLedger(),
                                        config.lifecycle().recoveryTimeout().toNanos()),
                                Math.toIntExact(config.fetch().maxEntryBytes()),
                                observer,
                                current,
                                openings,
                                unselectedFence(recoveryOwner, owner),
                                recoveryOwner.historyReads(owner), selected));
                        opened.requireRecoveredBirthSelection();
                        await(namespace.requireCurrent());
                        if (m4Admission.isPresent()) {
                            var admission = m4Admission.orElseThrow();
                            var sources = opened.storage.nativeReadSources();
                            var admittedSources = opened.admitNativeReadSources(owner, roots, m4, sources,
                                    predecessor -> m4.provisionDurableDrainOnly(
                                    admission.adapterSha256(),
                                    admission.backendConfigurationSha256(),
                                    admission.readContractSha256(),
                                    admission.verifierSha256(),
                                    admission.inputsSha256(),
                                    admission.receiptSha256(),
                                    admission.maximumSourceAccessLifetimeMillis(),
                                    selected.map(prefix -> prefix.view().descriptor().descriptorSha256())
                                            .orElseGet(sources::selectedViewSha256),
                                    ownerEpoch, predecessor));
                            opened.storage.installM4ReadOwner(
                                    m4.control(), admittedSources.publication(), m4ReadOwnerLoop, bk.maxReadsInFlight(),
                                    cellReadSlots, opened.bindingReadSlots, admittedSources.reads(),
                                    (successor, publication) -> recoveryOwner.admitNativeReadSources(
                                            owner, roots, m4, successor, predecessor -> {
                                                publication.run();
                                                return successor.selectedViewSha256();
                                            }).reads());
                            NereusKafkaReadScopeV2.persistRecovery(m4, scope.partition(), roots, opened.storage, selected);
                        } else if (m4.readSelector().isPresent()) {
                            throw new IllegalStateException(
                                    "M4 selector changed before native BK read owner activation");
                        } else {
                            opened.storage.installHistoricalReads(opened.historicalReads(owner, roots));
                        }
                        if (!current.getAsBoolean()) {
                            opened.storage.fence();
                            throw new FencedLeaderEpochException("native assignment changed after BK recovery");
                        }
                        return opened;
                    } catch (Exception failure) {
                        if (opened != null) {
                            try {
                                opened.close();
                            } catch (Exception closeFailure) {
                                failure.addSuppressed(closeFailure);
                            }
                        }
                        if (!current.getAsBoolean())
                            throw new FencedLeaderEpochException("native recovery was superseded", failure);
                        throw new CompletionException(failure);
                    }
                },
                openings).whenComplete((opened, failure) -> {
                    if (failure == null) runResponses.afterReady(identity, opened);
                    runResponses.afterRecovery(identity, failure);
                });
    }

    private synchronized Opened ownSource(
            M5BookKeeperNativeCreateClientV2 source, KafkaRunRootRecordV2.Scope scope) throws Exception {
        var binding = digest("binding-read-capacity/" + scope.bindingId().digest().toHex() + "/"
                + Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(scope.topic())).toHex() + "/"
                + scope.storageEpoch().digest().toHex());
        var bindingSlots = owned.stream()
                .filter(item -> item.binding.equals(binding))
                .map(item -> item.bindingReadSlots)
                .findFirst()
                .orElseGet(() -> new Semaphore(context.config().nereusKafkaStorageConfig()
                        .bookKeeper().orElseThrow().maxReadsInFlight()));
        var opened = new Opened(source, source.newSession(), binding, bindingSlots);
        if (closed) {
            opened.close();
            throw new IllegalStateException("native provider is draining");
        }
        owned.add(opened);
        return opened;
    }

    private KafkaBookKeeperRunBirthRecoveryV1.UnselectedFence unselectedFence(
            Opened recoveryOwner, KafkaOwnerIdentityV1 owner) {
        var source = recoveryOwner.source;
        return new KafkaBookKeeperRunBirthRecoveryV1.UnselectedFence() {
            public CompletionStage<Void> fence(
                    KafkaOwnerAdmissionV1 oldOwner, KafkaRunRootRecordV2 record, RunLedgerHandleV1 handle) {
                return fenceClosedRunBirth(source, oldOwner, record, handle);
            }

            public CompletionStage<Optional<RunLedgerRecoveryProofV1>> recoverNative(
                    KafkaOwnerAdmissionV1 oldOwner, KafkaRunRootRecordV2 record,
                    BookKeeperCellSession borrowedSession, RunLedgerHandleV1 handle) {
                var expected = closedRunBirthSpec(source, oldOwner, record, handle);
                var reader = recoveryOwner.newNativeReader(2, null);
                var recovery = writerAdmission.recoverUnderReadTicket(
                        owner, record, handle, expected, reader::beginRead, reader::closeAsync);
                reader.observeCleanup(recovery.cleanup());
                return recovery.result();
            }

            public CompletionStage<Void> reconcileAbsent(
                    KafkaOwnerAdmissionV1 oldOwner, KafkaRunRootRecordV2 record, RunLedgerHandleV1 handle) {
                return reconcileClosedRunBirth(source, oldOwner, record, handle, Optional.empty());
            }

            public CompletionStage<Void> reconcileRecovered(
                    KafkaOwnerAdmissionV1 oldOwner, KafkaRunRootRecordV2 record,
                    RunLedgerHandleV1 handle, RunLedgerRecoveryProofV1 proof) {
                return reconcileClosedRunBirth(source, oldOwner, record, handle, Optional.of(proof));
            }
        };
    }

    private NereusKafkaM5SourceScanV1 newNativeM5SourceScan(Opened owner) {
        return new NereusKafkaM5SourceScanV1(owner, owner.m4Route, owner.rootsAuthority, owner.currentAssignment,
                owner.nativeOwner, route, owner.source, m4Admission.orElseThrow(), this,
                NereusBookKeeperCapability.lifecycleFacts(oxia, namespace.identity(), owner.nativeScope));
    }

    CompletionStage<NereusKafkaM5GcScopeV1> openNativeM5Gc(NereusKafkaM5SourceScanV1 scan, Optional<Sha256Digest> task) {
        var capture = task.isPresent() ? NereusKafkaM5GcInputsV1.capture(scan, task.orElseThrow()) : NereusKafkaM5GcInputsV1.captureReplacement(scan);
        return capture.thenCompose(inputs -> CompletableFuture.supplyAsync(() -> {
            var clients = new java.util.HashMap<PhysicalResourceIdV2, M5BookKeeperNativeCreateClientV2>();
            try {
                for (var resource : inputs.resources()) clients.put(resource, scan.opened().nativeM5TaskClient(inputs.createScope(resource)));
                return new NereusKafkaM5GcScopeV1(scan, inputs.scopeIdentity(), inputs, clients, backend.nativeDeleteCellBudget(),
                        new Oxia09ExactMetadataTransactionStoreV1(oxia), facts -> namespace.openAuthorityRoute(backend, facts),
                        context.scheduler(), openings, () -> {
                            var policy = context.config().nereusKafkaStorageConfig().bookKeeper().orElseThrow().ledgerGc();
                            if (!policy.enabled() || policy.dryRun())
                                throw new IllegalStateException("native M5 physical deletion is disabled or dry-run");
                            try {
                                m5PolicyInput.orElseThrow().requireCurrent();
                            } catch (Exception failure) {
                                throw new CompletionException(failure);
                            }
                        });
            } catch (Exception failure) {
                for (var client : clients.values()) {
                    try {
                        client.close();
                    } catch (Exception closeFailure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
                throw new CompletionException(failure);
            }
        }, openings));
    }

    public final class Opened implements AutoCloseable {
        private final M5BookKeeperNativeCreateClientV2 source;
        private final BookKeeperCellSession session;
        private final Sha256Digest binding;
        private final Semaphore bindingReadSlots;
        private final Set<NativeReader> nativeReaders = ConcurrentHashMap.newKeySet();
        private final Set<CompletableFuture<Void>> inputOperations = ConcurrentHashMap.newKeySet();
        private final Set<NereusKafkaM5SourceScanV1> sourceScans = ConcurrentHashMap.newKeySet();
        private final Set<NereusKafkaNativeInputV2.CellReservation> m5Reservations = ConcurrentHashMap.newKeySet();
        private final Set<KafkaBookKeeperRunSourceV2.TicketCleanupException> inputTicketCleanup = ConcurrentHashMap.newKeySet();
        private final Set<IllegalStateException> selectedInputTicketCleanup = ConcurrentHashMap.newKeySet();
        private final Set<NereusKafkaRunBirthWriterAdmissionV1.TicketCleanupException> sourceTicketCleanup =
                ConcurrentHashMap.newKeySet();
        private final Set<NereusKafkaRunBirthWriterAdmissionV1.SourcePublicationException> failedSourcePublications =
                ConcurrentHashMap.newKeySet();
        private KafkaBookKeeperPartitionV1 storage;
        private volatile NereusUnifiedLog nativeLog;
        private boolean nativeLogAttachmentStarted;
        private boolean policyMutationActive;
        private KafkaBookKeeperNativeRootVerifierV2 verifier;
        private KafkaOwnerIdentityV1 nativeOwner;
        private KafkaRunRootRecordV2.Scope nativeScope;
        private long nativeStorageFence;
        private Sha256Digest nativeProtocolCell;
        private OxiaKafkaRunRootAuthorityV2 rootsAuthority;
        private NereusKafkaReadScopeV2.RecoveredBirth recoveredBirth;
        private com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperNativeFallbackRetirementV2 fallbackAttempt;
        private OxiaKafkaRunRootAuthorityV2.OwnerAdmissionFact fallbackOwnerFact;
        private OxiaKafkaBookKeeperM4ReadRouteV2 m4Route;
        private BooleanSupplier currentAssignment;
        private boolean draining;
        private boolean released;
        private CompletableFuture<Void> closeAttempt;

        Opened(M5BookKeeperNativeCreateClientV2 source, BookKeeperCellSession session,
               Sha256Digest binding, Semaphore bindingReadSlots) {
            this.source = source;
            this.session = session;
            this.binding = binding;
            this.bindingReadSlots = bindingReadSlots;
        }

        /** Low-frequency M5 control capture. The caller retains its admitted Cell byte budget. */
        public CompletionStage<NereusKafkaNativeInputV2.Captured> captureM5Inputs(
                KafkaBookKeeperReadCellBudgetV2 budget, KafkaBookKeeperRunSourceV2.Bounds bounds) {
            return ownInputOperation(() -> new NereusKafkaNativeInputV2(this, nativeM5InputCut(),
                    nativeOwner, rootsAuthority, m4Route, currentAssignment, source, writerAdmission.inputReadGuard(),
                    budget, bounds, openings).capture());
        }

        /** Native task preparation retains durable Cell capacity beyond the SDK scan's local read allowance. */
        public CompletionStage<NereusKafkaNativeInputV2.Captured> captureM5Inputs(
                KafkaBookKeeperReadCellBudgetV2 budget, KafkaBookKeeperRunSourceV2.Bounds bounds,
                KafkaCompactionAdmissionV1.CellCaps caps, NereusKafkaNativeInputV2.CaptureAllowance allowance) {
            return ownInputOperation(() -> {
                if (!m5PreparationSlots.tryAcquire())
                    throw new IllegalStateException("native M5 local preparation capacity exhausted");
                boolean handedOff = false;
                try {
                    var capture = new NereusKafkaNativeInputV2(this, nativeM5InputCut(),
                            nativeOwner, rootsAuthority, m4Route, currentAssignment, source, writerAdmission.inputReadGuard(),
                            budget, bounds, openings).capture(caps, allowance);
                    handedOff = true;
                    return capture;
                } catch (Throwable failure) {
                    throw NereusKafkaNativeInputV2.preparationNotDispatched(failure);
                } finally {
                    if (!handedOff) m5PreparationSlots.release();
                }
            });
        }

        KafkaBookKeeperRunSourceV2.OwnedReadSession openSelectedInputSession() {
            var reader = new SelectedPartReaders();
            return new KafkaBookKeeperRunSourceV2.OwnedReadSession(reader, reader::observeCleanup);
        }

        KafkaCompactionAdmissionV1 nativeM5CompactionAdmission() {
            return new KafkaCompactionAdmissionV1(m4Route.compactionAdmissionMetadata(nativeProtocolCell), nativeProtocolCell);
        }

        OxiaKafkaBookKeeperM4ReadRouteV2 nativeM5BindingRoute() {
            return m4Route;
        }

        synchronized CompletionStage<Boolean> nativeM5GcAdmissionOpen() {
            if (draining || policyMutationActive || nativeLog == null || storage == null
                    || m5PolicyInput.isEmpty() || !currentAssignment.getAsBoolean())
                return CompletableFuture.completedFuture(false);
            var expected = Optional.ofNullable(fallbackOwnerFact);
            return rootsAuthority.readOwnerAdmissionFact().thenApply(actual -> {
                synchronized (this) {
                    return !draining && !policyMutationActive && currentAssignment.getAsBoolean()
                            && actual.filter(value -> !value.admission().closed() && value.admission().owner().equals(nativeOwner))
                                    .filter(value -> expected.isEmpty() || expected.equals(Optional.of(value))).isPresent();
                }
            });
        }

        CompletionStage<NereusKafkaM5SourceScanV1> openNativeM5SourceScan() {
            final NereusKafkaM5SourceScanV1 scan;
            synchronized (this) {
                if (draining || policyMutationActive || m5PolicyInput.isEmpty() || !sourceScans.isEmpty())
                    return CompletableFuture.failedFuture(new IllegalStateException("native M5 source-scan admission is closed"));
                scan = newNativeM5SourceScan(this);
                sourceScans.add(scan);
                ownInputOperation(scan::lifetime).whenComplete((ignored, failure) -> {
                    if (failure == null) sourceScans.remove(scan);
                });
            }
            var observer = scan.start().toCompletableFuture();
            observer.whenComplete((value, failure) -> {
                if (failure != null) scan.closeAsync();
            });
            return observer;
        }

        private CompletableFuture<Void> closeNativeM5SourceScans() {
            return CompletableFuture.allOf(List.copyOf(sourceScans).stream()
                    .map(NereusKafkaM5SourceScanV1::closeAsync).toArray(CompletableFuture[]::new));
        }

        /** Capture actual per-resource replacement facts on the owned control path; this grants no deletion. */
        CompletionStage<NereusKafkaReadScopeV2.ReplacementEvidence> captureNativeM5ReplacementEvidence() {
            return captureNativeM5ReplacementEvidence(null);
        }

        CompletionStage<NereusKafkaReadScopeV2.ReplacementEvidence> captureNativeM5ReplacementEvidence(
                NereusKafkaM5SourceScanV1 scan) {
            return ownInputOperation(() -> CompletableFuture.supplyAsync(() -> {
                var before = rootsAuthority.readOwnerAdmissionFact().toCompletableFuture().join().orElseThrow();
                if (!currentAssignment.getAsBoolean() || before.admission().closed() || !before.admission().owner().equals(nativeOwner))
                    throw new IllegalStateException("native replacement capture lacks its actual current Owner");
                var evidence = scan == null
                        ? NereusKafkaReadScopeV2.captureReplacement(m4Route, storage, route, nativeProtocolCell, source, rootsAuthority, before,
                                record -> nativeRunSpec(source, record))
                        : NereusKafkaReadScopeV2.captureReplacementAtSourceScan(m4Route, storage, route, nativeProtocolCell, source, rootsAuthority, before,
                                record -> nativeRunSpec(source, record), scan.exactAuthority());
                if (!currentAssignment.getAsBoolean()
                        || !rootsAuthority.readOwnerAdmissionFact().toCompletableFuture().join().equals(Optional.of(before)))
                    throw new IllegalStateException("native replacement Owner changed during fact capture");
                return evidence;
            }, nativeM5TaskControl()));
        }

        /** Read the actual cancelled output cut after original Task cleanup; this grants no GC admission. */
        CompletionStage<NereusKafkaUnpublishedScopeV2.Evidence> captureNativeM5UnpublishedEvidence(Sha256Digest taskId) {
            return captureNativeM5UnpublishedEvidence(taskId, null);
        }

        CompletionStage<NereusKafkaUnpublishedScopeV2.Evidence> captureNativeM5UnpublishedEvidence(Sha256Digest taskId,
                NereusKafkaM5SourceScanV1 scan) {
            return ownInputOperation(() -> CompletableFuture.supplyAsync(() -> {
                var before = rootsAuthority.readOwnerAdmissionFact().toCompletableFuture().join().orElseThrow();
                if (!currentAssignment.getAsBoolean() || before.admission().closed() || !before.admission().owner().equals(nativeOwner))
                    throw new IllegalStateException("native unpublished capture lacks its actual current Owner");
                var evidence = scan == null ? NereusKafkaUnpublishedScopeV2.capture(m4Route, route, nativeProtocolCell, source, taskId)
                        : NereusKafkaUnpublishedScopeV2.captureAtSourceScan(m4Route, route, nativeProtocolCell, source, taskId, scan.exactAuthority());
                if (!currentAssignment.getAsBoolean()
                        || !rootsAuthority.readOwnerAdmissionFact().toCompletableFuture().join().equals(Optional.of(before)))
                    throw new IllegalStateException("native unpublished Owner changed during fact capture");
                return evidence;
            }, nativeM5TaskControl()));
        }

        /** A native selected read actor may retire fallback only while its actual KRaft/Owner admission is current. */
        public CompletionStage<com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperNativeFallbackRetirementV2.Result>
                retireNativeM5Fallback() {
            return ownInputOperation(() -> CompletableFuture.supplyAsync(() -> {
                try {
                    if (!currentAssignment.getAsBoolean()) throw new IllegalStateException("native fallback Owner was superseded");
                    var facts = await(rootsAuthority.readOwnerAdmissionFact()).orElseThrow();
                    var admitted = facts.admission();
                    if (admitted.closed() || !admitted.owner().equals(nativeOwner))
                        throw new IllegalStateException("native fallback lacks its actual open Owner admission");
                    var retirement = storage.beginNativeFallbackRetirement();
                    if (fallbackAttempt != retirement) {
                        fallbackAttempt = retirement;
                        fallbackOwnerFact = facts;
                    } else if (!facts.equals(fallbackOwnerFact)) {
                        throw new IllegalStateException("native fallback Owner admission changed during drain");
                    }
                    return retirement;
                } catch (Exception failure) {
                    throw new CompletionException(failure);
                }
            }, nativeM5TaskControl()).thenCompose(retirement -> {
                var ownerFact = fallbackOwnerFact;
                return retirement.retire(nativeM5TaskControl()).thenApplyAsync(result -> {
                    try {
                        var current = requireFallbackOwnerFact(ownerFact);
                        storage.finishNativeFallbackRetirement(retirement, result);
                        NereusKafkaReadScopeV2.persist(m4Route, nativeScope.partition(), result, current);
                        requireFallbackOwnerFact(current);
                        storage.completeNativeFallbackRetirement(retirement, result);
                        return result;
                    } catch (Exception failure) {
                        throw new CompletionException(failure);
                    }
                }, nativeM5TaskControl());
            }));
        }

        private OxiaKafkaRunRootAuthorityV2.OwnerAdmissionFact requireFallbackOwnerFact(
                OxiaKafkaRunRootAuthorityV2.OwnerAdmissionFact expected) throws Exception {
            if (!currentAssignment.getAsBoolean()) throw new IllegalStateException("native fallback Owner changed during drain");
            var current = await(rootsAuthority.readOwnerAdmissionFact()).filter(expected::equals)
                    .orElseThrow(() -> new IllegalStateException("native fallback Owner admission changed during drain"));
            if (!currentAssignment.getAsBoolean()) throw new IllegalStateException("native fallback Owner changed during drain");
            return current;
        }

        private Optional<KafkaBookKeeperClosedHistoryRecoveryV1.SelectedPrefix> recoverNativeSelectedPrefix(Optional<BindingReadSelector> observed,
                Optional<KafkaOwnerAdmissionV1> closedOwner) throws Exception {
            if (observed.isEmpty()) return Optional.empty();
            var selector = observed.orElseThrow();
            var metadata = new KafkaBookKeeperControlMetadataStoreV2(m4Route.compactionRecords(), m4Route.rawBindingControl(),
                    nativeScope.partition(), selector.binding(), source.spec().namespace(), capability.providerScopeId());
            var bytes = metadata.get(KafkaBookKeeperCompactionPublicationV2.descriptorKey(selector.selectedViewSha256()));
            if (bytes.isEmpty()) {
                if (selector.mode() != SelectorMode.PREFERRED_ONLY)
                    throw new IllegalStateException("native selected recovery lacks its durable descriptor");
                return Optional.empty();
            }
            var descriptor = KafkaSealedBookKeeperDescriptorCodecV2.decode(bytes.orElseThrow());
            var budget = SelectedRecoveryMetadata.require(this, metadata, selector, descriptor, capability);
            if (!m5PreparationSlots.tryAcquire()) throw new IllegalStateException("native selected cache local capacity exhausted");
            var reservation = NereusKafkaNativeInputV2.CellReservation.selectedCache(nativeM5CompactionAdmission(),
                    budget.forOwnerCache(selectedCacheIdentity(nativeM5NamespaceBinding(), nativeOwner,
                            descriptor.descriptorSha256())), descriptor.descriptorSha256(), openings);
            retainM5Reservation(reservation);
            reservation.reserveSelectedCache();
            var reader = new SelectedPartReaders();
            var resources = descriptor.sealedParts().stream().map(seal -> (PhysicalResourceIdV2)
                    new PhysicalResourceIdV2.BookKeeperLedger(source.spec().namespace(), seal.handle().ledgerIdentity().ledgerId())).toList();
            int encoded = Math.toIntExact(descriptor.task().parts().stream().mapToLong(KafkaBookKeeperInventoryV2.PartPlan::length).sum());
            var recovered = await(reservation.own(() -> ownInputOperation(() -> {
                var read = writerAdmission.readSelectedUnderReadTickets(nativeOwner, resources, descriptor.descriptorSha256(),
                        () -> new KafkaSealedBookKeeperReaderV2(reader, source::captureExactTarget, encoded,
                                budget.decodingBounds(), nativeM5TaskControl()).recover(descriptor), reader::closeAsync);
                reader.observeCleanup(read.cleanup());
                return read.result().thenCompose(view -> read.cleanup().thenApply(ignored -> view));
            })));
            if (!currentAssignment.getAsBoolean() || !m4Route.readSelector().equals(observed))
                throw new IllegalStateException("native selected recovery selector or assignment changed");
            var closed = closedOwner.orElseThrow(() -> new IllegalStateException("native selected recovery has no closed predecessor"));
            var archive = await(rootsAuthority.readClosedOwnerFact(closed.owner().ownerEpoch()))
                    .filter(value -> value.admission().equals(closed)).orElseThrow(
                            () -> new IllegalStateException("native selected recovery lacks its exact closed archive"));
            recoveredBirth = NereusKafkaReadScopeV2.recoveredBirth(recovered, selector, archive,
                    nativeOwner, rootsAuthority, m4Route, currentAssignment, this::recoveringBirth);
            return Optional.of(KafkaBookKeeperClosedHistoryRecoveryV1.SelectedPrefix.verify(recovered, metadata));
        }

        private synchronized boolean recoveringBirth() {
            return storage == null && !draining;
        }

        private void requireRecoveredBirthSelection() {
            if (recoveredBirth != null) recoveredBirth.requireSelection();
        }

        /** Serial Part traversal releases each confirmed SDK handle before reserving/opening the next one. */
        private final class SelectedPartReaders implements BookKeeperCellSession {
            private final List<NativeReader> readers = new ArrayList<>();
            private NativeReader active;
            private RunLedgerHandleV1 handle;
            private CompletionStage<Void> ticketCleanup;

            public CellProviderScopeId providerScopeId() {
                return capability.providerScopeId();
            }
            public BookKeeperCapabilitySnapshotV1 capabilitySnapshot() {
                return capability;
            }

            public CompletionStage<RunLedgerOpenResultV1> openRunLedger(RunLedgerHandleV1 next) {
                final NativeReader previous;
                synchronized (this) {
                    previous = active;
                }
                var closed = previous == null ? CompletableFuture.<Void>completedFuture(null) : previous.closeAsync();
                return closed.thenCompose(ignored -> {
                    var reader = newNativeReader(1, null);
                    synchronized (this) {
                        readers.add(reader);
                        if (ticketCleanup != null) reader.observeCleanup(ticketCleanup);
                        active = reader;
                        handle = next;
                    }
                    return reader.beginRead().openRunLedger(next);
                });
            }

            public synchronized CompletionStage<RunLedgerReadResultV1> readExactEntry(RunLedgerHandleV1 expected, long entry) {
                if (active == null || !expected.equals(handle))
                    throw new IllegalStateException("selected Part read differs from its opened native handle");
                return active.beginRead().readExactEntry(expected, entry);
            }

            synchronized void observeCleanup(CompletionStage<Void> cleanup) {
                ticketCleanup = cleanup;
                List.copyOf(readers).forEach(reader -> reader.observeCleanup(cleanup));
            }

            public CompletionStage<Void> closeAsync() {
                final List<NativeReader> owned;
                synchronized (this) {
                    owned = List.copyOf(readers);
                }
                return CompletableFuture.allOf(owned.stream().map(reader -> reader.closeAsync().toCompletableFuture())
                        .toArray(CompletableFuture[]::new));
            }

            public CompletionStage<Void> drain() {
                return closeAsync();
            }
            public CompletionStage<ProviderMutationResultV1<RunLedgerHandleV1>> createRunLedger(RunLedgerConfigurationV1 config) {
                throw new UnsupportedOperationException("selected Parts are read only");
            }
            public CompletionStage<ProviderMutationResultV1<AppendQuorumProofV1>> appendExplicitEntry(RunLedgerAppendRequestV1 request) {
                throw new UnsupportedOperationException("selected Parts are read only");
            }
            public CompletionStage<ProviderMutationResultV1<RunLedgerRecoveryProofV1>> fenceAndRecoverRunLedger(RunLedgerHandleV1 value) {
                throw new UnsupportedOperationException("selected Parts are read only");
            }
            public CompletionStage<ProviderMutationResultV1<RunLedgerCloseProofV1>> closeRunLedger(RunLedgerHandleV1 value) {
                throw new UnsupportedOperationException("selected Parts are read only");
            }
        }

        ExecutorService nativeM5TaskControl() {
            if (m4ReadOwnerLoop == null) throw new IllegalStateException("native M5 task lacks qualified read control");
            return m4ReadOwnerLoop;
        }

        String nativeM5InstanceId() {
            return source.spec().nativeInstanceId();
        }

        M5BookKeeperNativeCreateClientV2 nativeM5TaskClient(M5BookKeeperNativeCreateSpecV2 spec) throws Exception {
            if (!spec.namespace().equals(source.spec().namespace()) || spec.configurations().stream().anyMatch(config ->
                    !config.equals(RunLedgerConfigurationV1.from(capability, config.runId()))))
                throw new IllegalStateException("native M5 output task changes namespace/capability");
            return M5BookKeeperNativeCreateClientV2.connect(uri(), capability, spec, namespaceBinding);
        }

        BookKeeperCellSession nativeM5TaskSession(M5BookKeeperNativeCreateClientV2 client) {
            return runResponses.compactionSession(client.newSession());
        }

        CompletionStage<Void> nativeM5ResourceAuthority(PhysicalResourceIdV2 resource, M5BookKeeperNativeCreateSpecV2 spec) {
            long deadline = System.nanoTime() + context.config().nereusKafkaStorageConfig().lifecycle().recoveryTimeout().toNanos();
            return writerAdmission.admitWriterAuthority((PhysicalResourceIdV2.BookKeeperLedger) resource,
                    spec, route.quota(), deadline, runResponses::afterOpen)
                    .thenCompose(ignored -> writerAdmission.registerMember(resource, nativeProtocolCell, nativeScope, spec));
        }

        void awaitM5Task(CompletionStage<Void> termination) {
            try {
                await(termination);
            } catch (Exception failure) {
                throw new CompletionException(failure);
            }
        }

        void retainM5Reservation(NereusKafkaNativeInputV2.CellReservation reservation) {
            m5Reservations.add(reservation);
        }

        void forgetM5Reservation(NereusKafkaNativeInputV2.CellReservation reservation) {
            if (m5Reservations.remove(reservation)) m5PreparationSlots.release();
        }

        void releaseSupersededM5Cache(Sha256Digest descriptor) {
            for (var reservation : List.copyOf(m5Reservations)) {
                if (reservation.ownsSelectedCache(descriptor)) {
                    reservation.releaseAfterSelectedCacheDrain();
                    forgetM5Reservation(reservation);
                }
            }
        }

        /** Installed after the actual Controller RECOVERED acknowledgement and before request readiness. */
        public void attachM5Log(NereusUnifiedLog log) {
            attachM5Log(log, () -> {
                if (m5PolicyInput.isPresent()) {
                    try {
                        m5PolicyInput.orElseThrow().enrollNativeWriters(m4Route, m4Admission.orElseThrow());
                    } catch (Exception failure) {
                        throw new CompletionException("native Kafka writer enrollment was not admitted", failure);
                    }
                }
                if (m5PolicyInput.isPresent()) log.installM5PolicyWriter(nativeOwner.kafkaLeaderEpoch(), storage, (proposed, offset) -> {
                    synchronized (this) {
                        if (draining || policyMutationActive) throw new IllegalStateException("native M5 policy admission is closed");
                        policyMutationActive = true;
                    }
                    try {
                        closeNativeM5SourceScans().join();
                        requireNativeM5PolicyOwner();
                        m5PolicyInput.orElseThrow().writeNativeConfiguration(context, log, m4Route,
                                currentAssignment, nativeOwner.ownerEpoch(), proposed, offset);
                        requireNativeM5PolicyOwner();
                    } finally {
                        synchronized (this) {
                            policyMutationActive = false;
                        }
                    }
                });
                return null;
            });
        }

        void attachM5Log(NereusUnifiedLog log, Supplier<Void> installation) {
            Objects.requireNonNull(log, "native M5 log");
            var drained = new CompletableFuture<Void>();
            synchronized (this) {
                if (draining) return;
                if (nativeLogAttachmentStarted) throw new IllegalStateException("native M5 log is already attached");
                nativeLogAttachmentStarted = true;
                inputOperations.add(drained);
            }
            try {
                // Enrollment and the initial policy write may need a sealed-root reader on this owner.
                installation.get();
                synchronized (this) {
                    if (!draining) nativeLog = log;
                }
            } finally {
                inputOperations.remove(drained);
                drained.complete(null);
            }
        }

        private void requireNativeM5PolicyOwner() {
            var actual = rootsAuthority.readOwnerAdmissionFact().toCompletableFuture().join().orElseThrow();
            if (!currentAssignment.getAsBoolean() || actual.admission().closed()
                    || !actual.admission().owner().equals(nativeOwner))
                throw new FencedLeaderEpochException("native M5 policy writer lost its actual Owner");
        }

        NereusKafkaNativeInputV2.NativeCut nativeM5InputCut() {
            return NereusKafkaM5PolicyInputV1.captureNativeCut(context, nativeLog, storage, nativeOwner,
                    currentAssignment, m4Route, m5PolicyInput);
        }

        long nativeM5StorageFence() {
            return nativeStorageFence;
        }

        Sha256Digest nativeM5ProtocolCell() {
            return nativeProtocolCell;
        }

        Sha256Digest nativeM5NamespaceBinding() {
            return Sha256Digest.hash(namespaceBinding.encode());
        }

        <T> CompletionStage<T> ownInputOperation(Supplier<CompletionStage<T>> operation) {
            var drained = new CompletableFuture<Void>();
            synchronized (this) {
                if (draining) return CompletableFuture.failedFuture(
                        new IllegalStateException("native M5 input owner is draining"));
                inputOperations.add(drained);
            }
            final CompletionStage<T> actual;
            try {
                actual = Objects.requireNonNull(operation.get(), "native M5 input operation");
            } catch (Throwable failure) {
                inputOperations.remove(drained);
                drained.complete(null);
                return CompletableFuture.failedFuture(failure);
            }
            return actual.whenComplete((value, failure) -> {
                var cause = failure;
                while (cause instanceof CompletionException && cause.getCause() != null) cause = cause.getCause();
                if (cause instanceof KafkaBookKeeperRunSourceV2.TicketCleanupException cleanup)
                    inputTicketCleanup.add(cleanup);
                if (NereusKafkaReadScopeV2.selectedTicketCleanup(cause))
                    selectedInputTicketCleanup.add((IllegalStateException) cause);
                inputOperations.remove(drained);
                drained.complete(null);
            }).thenApply(value -> value);
        }

        KafkaBookKeeperRunSourceV2.OwnedReadSession openInputSession(KafkaRunRootRecordV2 record) {
            var reader = newNativeReader(1, record, KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase.M5_INPUT);
            return new KafkaBookKeeperRunSourceV2.OwnedReadSession(reader.inputSession(), reader::observeCleanup);
        }

        private KafkaBookKeeperNativeRootVerifierV2 rootVerifier(KafkaOwnerIdentityV1 owner) {
            verifier = NereusKafkaReadScopeV2.rootVerifier(source,
                    (record, maximumHandles, verification) -> verifyRoot(owner, record, maximumHandles, verification),
                    () -> recoveredBirth);
            return verifier;
        }

        private CompletionStage<Void> verifyRoot(KafkaOwnerIdentityV1 owner, KafkaRunRootRecordV2 record,
                int maximumHandles, Function<RealBookKeeperCellSessionV1, CompletionStage<Void>> verification) {
            var expected = nativeRunSpec(source, record);
            final NativeReader reader;
            try {
                reader = newNativeReader(maximumHandles, record);
            } catch (RuntimeException | Error failure) {
                runResponses.afterRootVerifierAdmissionFailure(record, maximumHandles, failure);
                throw failure;
            }
            var read = writerAdmission.readUnderReadTicket(owner, record, expected,
                    () -> verification.apply(reader.beginVerification()), reader::closeAsync);
            reader.observeCleanup(read.cleanup());
            return read.result().thenCompose(ignored -> read.cleanup());
        }

        private KafkaBookKeeperClosedHistoryRecoveryV1.ReadOwner historyReads(KafkaOwnerIdentityV1 owner) {
            return new KafkaBookKeeperClosedHistoryRecoveryV1.ReadOwner() {
                @Override
                public <T> CompletionStage<T> read(KafkaRunRootRecordV2 record,
                        KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase phase,
                        Function<BookKeeperCellSession, CompletionStage<T>> operation) {
                    var expected = nativeRunSpec(source, record);
                    final NativeReader reader;
                    try {
                        reader = newNativeReader(phase.maximumHandles(), record, phase);
                    } catch (RuntimeException | Error failure) {
                        runResponses.afterHistoryReaderAdmissionFailure(record, phase, failure);
                        throw failure;
                    }
                    var read = writerAdmission.readUnderReadTicket(owner, record, expected,
                            () -> operation.apply(reader.beginRead()), reader::closeAsync);
                    reader.observeCleanup(read.cleanup());
                    return read.result().thenCompose(result -> read.cleanup().thenApply(ignored -> result));
                }
            };
        }

        private record AdmittedNativeSources<T>(T publication, KafkaBookKeeperPartitionV1.HistoricalReads reads) { }

        private <T> AdmittedNativeSources<T> admitNativeReadSources(KafkaOwnerIdentityV1 owner,
                com.nereusstream.storage.api.kafka.KafkaBookKeeperOwnerAuthorityV1 authority,
                OxiaKafkaBookKeeperM4ReadRouteV2 m4, KafkaBookKeeperPartitionV1.NativeReadSources sources,
                Function<Optional<BindingReadSelector>, T> publication) {
            var ownedPublication = ownInputOperation(() -> {
                try {
                    return CompletableFuture.completedFuture(publishNativeReadSources(owner, authority, m4, sources, publication));
                } catch (Throwable failure) {
                    return CompletableFuture.failedFuture(failure);
                }
            });
            try {
                return await(ownedPublication);
            } catch (Exception failure) {
                throw new CompletionException(failure);
            }
        }

        private <T> AdmittedNativeSources<T> publishNativeReadSources(KafkaOwnerIdentityV1 owner,
                com.nereusstream.storage.api.kafka.KafkaBookKeeperOwnerAuthorityV1 authority,
                OxiaKafkaBookKeeperM4ReadRouteV2 m4, KafkaBookKeeperPartitionV1.NativeReadSources sources,
                Function<Optional<BindingReadSelector>, T> publication) {
            runResponses.beforeNativeSourcePublication(this, sources);
            if (!sources.configurationSha256().equals(capability.configurationDigest()))
                throw new IllegalStateException("native read source configuration differs from its admitted capability");
            var roots = new ArrayList<>(sources.sealedRoots());
            roots.add(sources.activeRoot());
            var resources = roots.stream().map(root -> (PhysicalResourceIdV2)
                    new PhysicalResourceIdV2.BookKeeperLedger(source.spec().namespace(), root.ledgerIdentity().ledgerId())).toList();
            var admitted = new CompletableFuture<Void>();
            var actual = new CompletableFuture<T>();
            var predecessor = new AtomicReference<Optional<BindingReadSelector>>();
            var guarded = writerAdmission.publishReadSources(owner, resources, sources.selectedViewSha256(), nativeProtocolCell, nativeScope, () -> {
                admitted.complete(null);
                return actual;
            }, () -> sourcePublicationTerminal(m4, predecessor.get()));
            guarded.whenComplete((ignored, failure) -> {
                if (failure != null) admitted.completeExceptionally(failure);
            });
            try {
                // Stay on the existing control executor. Synchronous M4 calls cannot run on an Oxia callback.
                await(admitted);
                var records = new HashMap<StorageRunId, KafkaRunRootRecordV2>();
                for (var root : roots) {
                    var record = await(readAdmittedNativeRoot(owner, authority, root));
                    if (!record.resource().namespace().equals(source.spec().namespace()))
                        throw new IllegalStateException("native read source belongs to another physical namespace");
                    nativeRunSpec(source, record);
                    if (records.put(root.runId(), record) != null)
                        throw new IllegalStateException("native read source repeats a run");
                }
                predecessor.set(m4.readSelector());
                var value = publication.apply(predecessor.get());
                var selector = m4.readSelector().orElseThrow(() -> new IllegalStateException("native source selector is absent"));
                storage.requireNativeReadPublication(sources, selector);
                actual.complete(value);
                return new AdmittedNativeSources<>(await(guarded), localHistoricalReads(Map.copyOf(records)));
            } catch (Exception failure) {
                actual.completeExceptionally(failure);
                try {
                    await(guarded);
                } catch (Exception terminalFailure) {
                    var cause = terminalFailure;
                    while (cause.getCause() instanceof Exception nested) {
                        if (cause instanceof NereusKafkaRunBirthWriterAdmissionV1.SourcePublicationException cleanup)
                            failedSourcePublications.add(cleanup);
                        cause = nested;
                    }
                    if (cause instanceof NereusKafkaRunBirthWriterAdmissionV1.TicketCleanupException cleanup)
                        sourceTicketCleanup.add(cleanup);
                    if (terminalFailure != failure) failure.addSuppressed(terminalFailure);
                }
                throw new CompletionException(failure);
            }
        }

        /** A failed synchronous invocation cannot dispatch again; a newer durable selector excludes its late CAS. */
        private Optional<CanonicalBytes> sourcePublicationTerminal(OxiaKafkaBookKeeperM4ReadRouteV2 m4,
                Optional<BindingReadSelector> predecessor) {
            if (predecessor == null) return Optional.of(CanonicalBytes.copyOf(new byte[] {0}));
            var current = m4.readSelector();
            if (current.isEmpty()) return Optional.empty();
            var after = current.orElseThrow();
            if (predecessor.isPresent()) {
                var before = predecessor.orElseThrow();
                if (!after.binding().equals(before.binding()) || after.ownerEpoch() < before.ownerEpoch()
                        || after.readAdmissionEpoch() < before.readAdmissionEpoch() || after.sourceGeneration() < before.sourceGeneration())
                    return Optional.empty();
                if (after.ownerEpoch() == before.ownerEpoch() && after.readAdmissionEpoch() == before.readAdmissionEpoch()
                        && after.sourceGeneration() == before.sourceGeneration())
                    return Optional.empty();
            }
            var beforeBytes = predecessor.map(M4ReadControlCodecV1::encodeSelector).map(CanonicalBytes::toByteArray).orElseGet(() -> new byte[0]);
            var afterBytes = M4ReadControlCodecV1.encodeSelector(after).toByteArray();
            return Optional.of(CanonicalBytes.copyOf(ByteBuffer.allocate(9 + beforeBytes.length + afterBytes.length)
                    .put((byte) 1).putInt(beforeBytes.length).put(beforeBytes).putInt(afterBytes.length).put(afterBytes).array()));
        }

        private CompletionStage<KafkaRunRootRecordV2> readAdmittedNativeRoot(KafkaOwnerIdentityV1 owner,
                com.nereusstream.storage.api.kafka.KafkaBookKeeperOwnerAuthorityV1 authority,
                com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1 root) {
            var admission = root.creatorOwnerEpoch() == owner.ownerEpoch()
                    ? authority.readOwnerAdmission() : authority.readClosedOwner(root.creatorOwnerEpoch());
            return admission.thenCompose(found -> {
                var archived = found.orElseThrow(() -> new IllegalStateException("native read source owner is absent"));
                if (archived.owner().ownerEpoch() != root.creatorOwnerEpoch()
                        || root.creatorOwnerEpoch() == owner.ownerEpoch() && (!archived.owner().equals(owner) || archived.closed())
                        || root.creatorOwnerEpoch() != owner.ownerEpoch() && !archived.closed())
                    throw new IllegalStateException("native read source owner differs from its admitted generation");
                var link = archived.runs().stream().filter(value -> value.runId().equals(root.runId()))
                        .findFirst().orElseThrow(() -> new IllegalStateException("native read source run is not admitted"));
                return authority.readAdmittedRun(link);
            }).thenApply(found -> {
                var record = found.orElseThrow(() -> new IllegalStateException("native read source record is absent"));
                if (record.retired() || !record.root().equals(root))
                    throw new IllegalStateException("native read source differs from the selected root");
                return record;
            });
        }

        private KafkaBookKeeperPartitionV1.HistoricalReads localHistoricalReads(Map<StorageRunId, KafkaRunRootRecordV2> records) {
            return (root, handle, entryId) -> {
                var record = records.get(root.runId());
                if (record == null || !record.root().equals(root)
                        || !root.providerScopeId().equals(handle.providerScopeId())
                        || !root.runId().equals(handle.runId())
                        || !root.ledgerIdentity().equals(handle.ledgerIdentity())
                        || !capability.configurationDigest().equals(handle.configurationDigest()))
                    throw new IllegalStateException("historical Fetch differs from its admitted local source generation");
                var reader = newNativeReader(1, record, KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase.FETCH, false);
                CompletionStage<RunLedgerReadResultV1> operation;
                try {
                    operation = readHistoricalEntry(reader, handle, entryId);
                } catch (Throwable failure) {
                    operation = CompletableFuture.failedFuture(failure);
                }
                var result = operation.handle((value, failure) -> {
                    var close = reader.closeAsync();
                    reader.observeCleanup(close, false);
                    return close.handle((ignored, closeFailure) -> {
                        if (failure != null) throw new CompletionException(failure);
                        if (closeFailure != null) throw new CompletionException(closeFailure);
                        return value;
                    });
                }).thenCompose(closed -> closed);
                return CompletableFuture.completedFuture(new KafkaBookKeeperPartitionV1.HistoricalEntryRead(
                        result, reader.closedKnown.thenApply(ignored -> null), reader.closeUnknown.thenApply(ignored -> null)));
            };
        }

        private CompletionStage<RunLedgerReadResultV1> readHistoricalEntry(NativeReader reader, RunLedgerHandleV1 handle, long entryId) {
            var selected = reader.beginRead();
            return selected.openRunLedger(handle).thenCompose(open -> {
                if (!open.exactHandle().equals(Optional.of(handle)))
                    return CompletableFuture.failedFuture(new IllegalStateException("historical Fetch did not open exactly"));
                return selected.readExactEntry(handle, entryId);
            });
        }

        private KafkaBookKeeperPartitionV1.HistoricalReads historicalReads(KafkaOwnerIdentityV1 owner,
                com.nereusstream.storage.api.kafka.KafkaBookKeeperOwnerAuthorityV1 authority) {
            return (root, handle, entryId) -> {
                var admitted = root.creatorOwnerEpoch() == owner.ownerEpoch()
                        ? authority.readOwnerAdmission() : authority.readClosedOwner(root.creatorOwnerEpoch());
                return admitted.thenCompose(found -> {
                    var archived = found.orElseThrow(() -> new IllegalStateException("historical Fetch owner is absent"));
                    if (archived.owner().ownerEpoch() != root.creatorOwnerEpoch()
                            || root.creatorOwnerEpoch() == owner.ownerEpoch() && !archived.owner().equals(owner))
                        throw new IllegalStateException("historical Fetch owner identity changed");
                    var link = archived.runs().stream().filter(value -> value.runId().equals(root.runId()))
                            .findFirst().orElseThrow(() -> new IllegalStateException("historical Fetch run is not admitted"));
                    return runResponses.afterHistoricalFetchRecordRead(root, authority.readAdmittedRun(link));
                }).thenApply(found -> {
                    var record = found.orElseThrow(() -> new IllegalStateException("historical Fetch record is absent"));
                    if (record.retired() || !record.root().equals(root)
                            || !root.providerScopeId().equals(handle.providerScopeId())
                            || !root.runId().equals(handle.runId()) || !root.ledgerIdentity().equals(handle.ledgerIdentity())
                            || !capability.configurationDigest().equals(handle.configurationDigest()))
                        throw new IllegalStateException("historical Fetch differs from its captured selected root");
                    var expected = nativeRunSpec(source, record);
                    var reader = newNativeReader(1, record, KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase.FETCH);
                    CompletionStage<RunLedgerReadResultV1> result;
                    try {
                        var read = writerAdmission.readUnderReadTicket(owner, record, expected,
                                () -> readHistoricalEntry(reader, handle, entryId), reader::closeAsync);
                        reader.observeCleanup(read.cleanup());
                        result = read.result().thenCompose(value -> read.cleanup().thenApply(ignored -> value));
                    } catch (Throwable failure) {
                        var cleanup = reader.closeAsync().thenCompose(ignored -> CompletableFuture.<Void>failedFuture(failure));
                        reader.observeCleanup(cleanup);
                        result = CompletableFuture.failedFuture(failure);
                    }
                    return new KafkaBookKeeperPartitionV1.HistoricalEntryRead(
                            result, reader.closedKnown.thenApply(ignored -> null), reader.closeUnknown.thenApply(ignored -> null));
                });
            };
        }

        private synchronized NativeReader newNativeReader(int maximumHandles, KafkaRunRootRecordV2 record) {
            return newNativeReader(maximumHandles, record, null);
        }

        private synchronized NativeReader newNativeReader(int maximumHandles, KafkaRunRootRecordV2 record,
                KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase historyPhase) {
            return newNativeReader(maximumHandles, record, historyPhase, true);
        }

        private synchronized NativeReader newNativeReader(int maximumHandles, KafkaRunRootRecordV2 record,
                KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase historyPhase, boolean ownsCapacity) {
            if (draining) throw new IllegalStateException("native run reader owner is draining");
            // Header/footer reuse one handle; recovery can retain the original and its replacement together.
            String label = record == null ? "native run birth" : historyPhase == null
                    ? "native root verifier" : "native selected history";
            if (ownsCapacity) reserveReadSlots(maximumHandles, label);
            try {
                var nativeSession = source.newSession();
                var session = record == null ? runResponses.birthReaderSession(nativeSession) : historyPhase == null
                        ? nativeSession : runResponses.historyReaderSession(record, historyPhase, nativeSession);
                var reader = new NativeReader(nativeSession, session, maximumHandles, record, historyPhase, ownsCapacity);
                nativeReaders.add(reader);
                return reader;
            } catch (RuntimeException | Error failure) {
                if (ownsCapacity) {
                    cellReadSlots.release(maximumHandles);
                    bindingReadSlots.release(maximumHandles);
                }
                throw failure;
            }
        }

        private void reserveReadSlots(int maximumHandles, String label) {
            if (!cellReadSlots.tryAcquire(maximumHandles))
                throw new IllegalStateException(label + " Cell read capacity is full");
            if (!bindingReadSlots.tryAcquire(maximumHandles)) {
                cellReadSlots.release(maximumHandles);
                throw new IllegalStateException(label + " Binding read capacity is full");
            }
        }

        private final class NativeReader {
            private final RealBookKeeperCellSessionV1 nativeSession;
            private final BookKeeperCellSession reader;
            private final int maximumHandles;
            private final KafkaRunRootRecordV2 rootRecord;
            private final KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase historyPhase;
            private final boolean ownsCapacity;
            private final AtomicReference<CompletableFuture<Void>> closing = new AtomicReference<>();
            private final CompletableFuture<Void> cleanup = new CompletableFuture<>();
            private final CompletableFuture<Void> closedKnown = new CompletableFuture<>();
            private final CompletableFuture<Void> closeUnknown = new CompletableFuture<>();
            private volatile boolean nativeStarted;

            private NativeReader(RealBookKeeperCellSessionV1 nativeSession, BookKeeperCellSession reader,
                                 int maximumHandles, KafkaRunRootRecordV2 rootRecord,
                                 KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase historyPhase, boolean ownsCapacity) {
                this.nativeSession = nativeSession;
                this.reader = reader;
                this.maximumHandles = maximumHandles;
                this.rootRecord = rootRecord;
                this.historyPhase = historyPhase;
                this.ownsCapacity = ownsCapacity;
            }

            private RealBookKeeperCellSessionV1 beginVerification() {
                nativeStarted = true;
                return nativeSession;
            }

            private BookKeeperCellSession beginRead() {
                nativeStarted = true;
                return reader;
            }

            private BookKeeperCellSession inputSession() {
                return new BookKeeperCellSession() {
                    public CellProviderScopeId providerScopeId() {
                        return reader.providerScopeId();
                    }

                    public BookKeeperCapabilitySnapshotV1 capabilitySnapshot() {
                        return reader.capabilitySnapshot();
                    }

                    public CompletionStage<RunLedgerOpenResultV1> openRunLedger(RunLedgerHandleV1 handle) {
                        return beginRead().openRunLedger(handle);
                    }

                    public CompletionStage<RunLedgerReadResultV1> readExactEntry(RunLedgerHandleV1 handle, long entry) {
                        return beginRead().readExactEntry(handle, entry);
                    }

                    public CompletionStage<ProviderMutationResultV1<RunLedgerHandleV1>> createRunLedger(RunLedgerConfigurationV1 config) {
                        throw new UnsupportedOperationException("M5 input session is read only");
                    }

                    public CompletionStage<ProviderMutationResultV1<AppendQuorumProofV1>> appendExplicitEntry(RunLedgerAppendRequestV1 request) {
                        throw new UnsupportedOperationException("M5 input session is read only");
                    }

                    public CompletionStage<ProviderMutationResultV1<RunLedgerRecoveryProofV1>> fenceAndRecoverRunLedger(RunLedgerHandleV1 handle) {
                        throw new UnsupportedOperationException("M5 input session is read only");
                    }

                    public CompletionStage<ProviderMutationResultV1<RunLedgerCloseProofV1>> closeRunLedger(RunLedgerHandleV1 handle) {
                        throw new UnsupportedOperationException("M5 input session is read only");
                    }

                    public CompletionStage<Void> drain() {
                        return reader.drain();
                    }

                    public CompletionStage<Void> closeAsync() {
                        return NativeReader.this.closeAsync();
                    }
                };
            }

            private CompletionStage<Void> closeAsync() {
                var close = new CompletableFuture<Void>();
                if (!closing.compareAndSet(null, close)) return closing.get().thenApply(ignored -> null);
                try {
                    var actual = reader.closeAsync();
                    var observed = !nativeStarted ? actual : rootRecord == null
                            ? runResponses.afterBirthReaderClose(actual)
                            : historyPhase == null ? runResponses.afterRootVerifierClose(rootRecord, actual)
                                    : runResponses.afterHistoryReaderClose(rootRecord, historyPhase, actual);
                    observed.whenComplete((ignored, failure) -> {
                        if (failure != null) {
                            closeUnknown.completeExceptionally(failure);
                            close.completeExceptionally(failure);
                        } else {
                            if (ownsCapacity) {
                                cellReadSlots.release(maximumHandles);
                                bindingReadSlots.release(maximumHandles);
                            }
                            closedKnown.complete(null);
                            close.complete(null);
                        }
                    });
                } catch (Throwable failure) {
                    closeUnknown.completeExceptionally(failure);
                    close.completeExceptionally(failure);
                }
                return close.thenApply(ignored -> null);
            }

            private void observeCleanup(CompletionStage<Void> ticketCleanup) {
                observeCleanup(ticketCleanup, true);
            }

            private void observeCleanup(CompletionStage<Void> ticketCleanup, boolean ticketed) {
                ticketCleanup.whenComplete((ignored, failure) -> {
                    if (failure == null) cleanup.complete(null);
                    else cleanup.completeExceptionally(failure);
                    if (failure == null || !nativeStarted && closedKnown.isDone()) nativeReaders.remove(this);
                    if (ticketed) observeTicketCleanup(failure);
                });
            }

            private void observeTicketCleanup(Throwable failure) {
                if (rootRecord == null) runResponses.afterBirthReaderTicketCleanup(failure);
                else if (historyPhase == null) runResponses.afterRootVerifierTicketCleanup(rootRecord, failure);
                else runResponses.afterHistoryReaderTicketCleanup(rootRecord, historyPhase, failure);
            }

            private CompletionStage<Void> finishTicketCleanup() {
                return cleanup.handle((ignored, failure) -> {
                    if (failure == null) return CompletableFuture.<Void>completedFuture(null);
                    var cause = failure;
                    while (cause instanceof CompletionException && cause.getCause() != null) cause = cause.getCause();
                    var original = cause;
                    final CompletionStage<List<PhysicalResourceIdV2>> retry;
                    if (cause instanceof NereusKafkaRunBirthWriterAdmissionV1.TicketCleanupException tickets)
                        retry = tickets.reconcileTickets();
                    else if (cause instanceof KafkaBookKeeperRunSourceV2.TicketCleanupException tickets)
                        retry = tickets.reconcileTickets();
                    else if (NereusKafkaReadScopeV2.selectedTicketCleanup(cause))
                        retry = NereusKafkaReadScopeV2.reconcileSelectedTickets(cause);
                    else return CompletableFuture.<Void>failedFuture(failure);
                    return retry.thenCompose(left -> {
                        if (!left.isEmpty()) return CompletableFuture.<Void>failedFuture(failure);
                        inputTicketCleanup.remove(original);
                        selectedInputTicketCleanup.remove(original);
                        nativeReaders.remove(this);
                        observeTicketCleanup(null);
                        return CompletableFuture.<Void>completedFuture(null);
                    });
                }).thenCompose(result -> result);
            }
        }

        public KafkaBookKeeperPartitionV1 storage() {
            return storage;
        }

        public void fence() {
            if (storage != null) storage.fence();
        }

        /** Close off the metadata/coordinator callback thread. */
        public CompletableFuture<Void> closeAsync() {
            final CompletableFuture<Void> inputsDrained;
            synchronized (this) {
                draining = true;
                inputsDrained = CompletableFuture.allOf(inputOperations.toArray(CompletableFuture[]::new));
            }
            // Do not occupy the control executor while its accepted input scans still need callbacks.
            return closeNativeM5SourceScans().thenCompose(ignored -> inputsDrained).thenRunAsync(() -> {
                try {
                    close();
                } catch (Exception failure) {
                    throw new CompletionException(failure);
                }
            }, openings).thenApply(ignored -> null);
        }

        @Override
        public void close() throws Exception {
            final CompletableFuture<Void> attempt;
            boolean start = false;
            synchronized (this) {
                if (released) return;
                if (closeAttempt == null || closeAttempt.isCompletedExceptionally()) {
                    draining = true;
                    closeAttempt = new CompletableFuture<>();
                    start = true;
                }
                attempt = closeAttempt;
            }
            if (start) {
                try {
                    closeResources();
                    synchronized (this) {
                        nativeLog = null;
                        released = true;
                    }
                    attempt.complete(null);
                } catch (Throwable failure) {
                    attempt.completeExceptionally(failure);
                }
            }
            await(attempt);
        }

        private void closeResources() throws Exception {
            runResponses.afterReadAdmissionClosed(this);
            await(closeNativeM5SourceScans());
            await(CompletableFuture.allOf(inputOperations.toArray(CompletableFuture[]::new)));
            if (verifier != null) verifier.close();
            fence();
            await(source.fenceCreates());
            if (storage != null) await(storage.resign());
            await(session.closeAsync());
            for (var reader : List.copyOf(nativeReaders)) {
                await(reader.closeAsync());
                await(reader.finishTicketCleanup());
            }
            for (var cleanup : List.copyOf(sourceTicketCleanup)) {
                if (!await(cleanup.reconcileTickets()).isEmpty()) throw cleanup;
                sourceTicketCleanup.remove(cleanup);
            }
            for (var cleanup : List.copyOf(failedSourcePublications)) {
                if (!await(cleanup.reconcileTickets()).isEmpty()) throw cleanup;
                failedSourcePublications.remove(cleanup);
            }
            for (var cleanup : List.copyOf(inputTicketCleanup)) {
                if (!await(cleanup.reconcileTickets()).isEmpty()) throw cleanup;
                inputTicketCleanup.remove(cleanup);
            }
            await(NereusKafkaReadScopeV2.reconcileSelectedTickets(selectedInputTicketCleanup));
            for (var reservation : List.copyOf(m5Reservations)) {
                reservation.releaseAfterOwnerDrain();
                forgetM5Reservation(reservation);
            }
            source.close();
        }
    }

    public synchronized void fence() {
        closed = true;
        owned.forEach(Opened::fence);
    }

    @Override
    public void close() throws Exception {
        fence();
        openings.shutdown();
        if (!openings.awaitTermination(
                context.config()
                        .nereusKafkaStorageConfig()
                        .rollout()
                        .shutdownDrainTimeout()
                        .toMillis(),
                TimeUnit.MILLISECONDS)) {
            throw new IllegalStateException("native BK recovery operations have not drained");
        }
        Exception failure = null;
        for (var item : List.copyOf(owned))
            try {
                item.close();
            } catch (Exception closeFailure) {
                if (failure == null) failure = closeFailure;
                else failure.addSuppressed(closeFailure);
            }
        if (failure != null) throw failure;
        closeM4ReadOwnerLoop();
        if (backend != null && !backendReleased) {
            backend.close();
            backendReleased = true;
        }
        if (oxia != null && !oxiaReleased) {
            oxia.close();
            oxiaReleased = true;
        }
    }

    private void closeM4ReadOwnerLoop() throws InterruptedException {
        if (m4ReadOwnerLoop == null) return;
        m4ReadOwnerLoop.shutdown();
        if (!m4ReadOwnerLoop.awaitTermination(
                context.config().nereusKafkaStorageConfig().rollout().shutdownDrainTimeout().toMillis(),
                TimeUnit.MILLISECONDS)) {
            throw new IllegalStateException("native M4 read-owner loop has not drained");
        }
    }

    private String uri() {
        return context.config()
                .nereusKafkaStorageConfig()
                .core()
                .bookKeeperMetadataServiceUri()
                .orElseThrow()
                .toString();
    }

    private static final class SelectedRecoveryMetadata {
        static KafkaBookKeeperSelectedCacheBudgetV1 require(
                Opened opened, KafkaBookKeeperControlMetadataStoreV2 metadata, BindingReadSelector selector,
                com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperDescriptorV2 descriptor,
                BookKeeperCapabilitySnapshotV1 capability) {
            var decision = new M5TaskSelectionCoordinatorV2(metadata, opened.nativeScope.partition(), selector.binding())
                    .readDecision(descriptor.task().taskIdSha256()).orElseThrow(
                            () -> new IllegalStateException("native selected recovery lacks its durable Task decision"));
            if (selector.admissionState() != AdmissionState.ADMITTING
                    || selector.mode() != SelectorMode.PREFERRED_ONLY && selector.mode() != SelectorMode.PREFERRED_WITH_FALLBACK)
                throw new IllegalStateException("native selected recovery lacks an admitting selected reader");
            if (decision.outcome() != M5TaskSelectionDecisionV2.Outcome.SELECTED
                    || !decision.selectedOutput().equals(Optional.of(descriptor.descriptorSha256()))
                    || !descriptor.task().capability().equals(capability)
                    || descriptor.recoveryStateLocator().isEmpty())
                throw new IllegalStateException("native selected recovery differs from its actual selection/capability");
            var budget = KafkaBookKeeperSelectedCacheBudgetV1.decode(metadata.get(
                    KafkaBookKeeperInventoryV2.taskKey(descriptor.task().taskIdSha256()) + "/cache-budget").orElseThrow(
                            () -> new IllegalStateException("native selected recovery lacks its persisted cache budget")));
            budget.requireDescriptor(descriptor);
            for (int part = 0; part < descriptor.sealedParts().size(); part++) {
                var inventory = KafkaBookKeeperInventoryCodecV2.decodePart(metadata.get(KafkaBookKeeperInventoryV2.partKey(
                        descriptor.task().taskIdSha256(), part)).orElseThrow());
                var seal = descriptor.sealedParts().get(part);
                if (!inventory.handle().equals(seal.handle()) || !inventory.resource().namespace().equals(opened.source.spec().namespace()))
                    throw new IllegalStateException("native selected recovery differs from its actual Part inventory");
            }
            return budget;
        }

    }

    private static Sha256Digest selectedCacheIdentity(Sha256Digest namespace, KafkaOwnerIdentityV1 owner, Sha256Digest descriptor) {
        return digest("NEREUS_NATIVE_SELECTED_CACHE_V1/" + namespace.toHex()
                + "/" + owner + "/" + descriptor.toHex() + "/" + UUID.randomUUID());
    }

    private <T> T await(CompletionStage<T> value) throws Exception {
        return value.toCompletableFuture()
                .get(
                        context.config()
                                .nereusKafkaStorageConfig()
                                .lifecycle()
                                .recoveryTimeout()
                                .toMillis(),
                        TimeUnit.MILLISECONDS);
    }

    private static StorageRunId runId(KafkaRunRootRecordV2.Scope scope, long epoch) {
        return new StorageRunId(Id128.fromBytes(java.util.Arrays.copyOf(
                digest("native-owner-run/" + Sha256Digest.hash(scope.encode()).toHex() + "/" + epoch)
                        .bytes()
                        .toByteArray(),
                16)));
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private void prepareSourceBootstrap(OxiaKafkaBookKeeperM4ReadRouteV2 m4,
            M5BookKeeperNativeCreateSpecV2 spec, TopicIdPartition identity, BooleanSupplier current, KafkaRunRootRecordV2.Scope scope) throws Exception {
        if (m4Admission.isEmpty()) return;
        var admission = m4Admission.orElseThrow();
        if (m5PolicyInput.isPresent()) m5PolicyInput.orElseThrow().requireNoPriorOwner(m4, oxia, namespaceBinding, scope);
        long providerMinimum = m5PolicyInput.isPresent()
                ? await(backend.confirmedDrainProviderMinimumMillis(namespaceBinding)) : -1;
        var firstPolicy = m5PolicyInput.map(input -> input.captureFirstPolicy(context, identity, current,
                namespaceBinding, capability, providerMinimum, admission));
        m4.prepareSourceBootstrap(admission.adapterSha256(),
                admission.backendConfigurationSha256(), admission.readContractSha256(),
                admission.verifierSha256(), admission.inputsSha256(), admission.receiptSha256(),
                admission.maximumSourceAccessLifetimeMillis(), spec.sha256(), firstPolicy);
        if (!current.getAsBoolean())
            throw new FencedLeaderEpochException("native assignment changed during source bootstrap");
    }

    private M5BookKeeperNativeCreateSpecV2 initialRunSpec(
            KafkaRunRootRecordV2.Scope scope, long ownerEpoch, String instanceId) {
        var first = runId(scope, ownerEpoch);
        return M5BookKeeperNativeCreateSpecV2.of(
                instanceId,
                digest("native-bk-run-scope/" + first.value().toHex()),
                List.of(RunLedgerConfigurationV1.from(capability, first)));
    }

    private M5BookKeeperNativeCreateSpecV2 closedRunBirthSpec(
            M5BookKeeperNativeCreateClientV2 source,
            KafkaOwnerAdmissionV1 closedOwner,
            KafkaRunRootRecordV2 record,
            RunLedgerHandleV1 handle) {
        var root = record.root();
        var resource = record.resource();
        if (root.creatorOwnerEpoch() != closedOwner.owner().ownerEpoch()
                || root.kafkaLeaderEpoch() != closedOwner.owner().kafkaLeaderEpoch()
                || !resource.namespace().equals(namespaceBinding.physicalNamespace())
                || resource.ledgerId() != handle.ledgerIdentity().ledgerId()) {
            throw new IllegalStateException("closed run birth changes native ownership");
        }
        return nativeRunSpec(source, record);
    }

    private M5BookKeeperNativeCreateSpecV2 nativeRunSpec(
            M5BookKeeperNativeCreateClientV2 source, KafkaRunRootRecordV2 record) {
        var root = record.root();
        if (!record.resource().namespace().equals(namespaceBinding.physicalNamespace()))
            throw new IllegalStateException("native run reader has another physical namespace");
        var scope = KafkaRunRootRecordV2.Scope.of(root);
        var initial = initialRunSpec(scope, root.creatorOwnerEpoch(), source.spec().nativeInstanceId());
        var expected = root.runId().equals(initial.configurations().get(0).runId())
                ? initial
                : M5BookKeeperNativeCreateSpecV2.laterRun(
                        initial, RunLedgerConfigurationV1.from(capability, root.runId()));
        return expected;
    }

    private CompletionStage<Void> fenceClosedRunBirth(
            M5BookKeeperNativeCreateClientV2 source,
            KafkaOwnerAdmissionV1 closedOwner,
            KafkaRunRootRecordV2 record,
            RunLedgerHandleV1 handle) {
        var expected = closedRunBirthSpec(source, closedOwner, record, handle);
        return writerAdmission.fenceClosedRunBirth(source, record, handle, expected);
    }

    private CompletionStage<Void> reconcileClosedRunBirth(
            M5BookKeeperNativeCreateClientV2 source,
            KafkaOwnerAdmissionV1 closedOwner,
            KafkaRunRootRecordV2 record,
            RunLedgerHandleV1 handle,
            Optional<RunLedgerRecoveryProofV1> recovered) {
        var expected = closedRunBirthSpec(source, closedOwner, record, handle);
        CompletionStage<?> nativeFacts = recovered.isPresent()
                ? runResponses.afterRecoveredScopeFacts(
                        source.readRecoveredScopeFacts(expected, recovered.orElseThrow()))
                : CompletableFuture.completedFuture(null);
        return nativeFacts.thenCompose(ignored ->
                writerAdmission.reconcileClosedRunBirth(closedOwner, record, handle, expected, recovered));
    }

    private final class AdmittingSession implements BookKeeperCellSession {
        private final BookKeeperCellSession delegate;
        private final M5BookKeeperNativeCreateClientV2 source;
        private final KafkaOwnerIdentityV1 owner;
        private final Opened opening;

        AdmittingSession(BookKeeperCellSession delegate, M5BookKeeperNativeCreateClientV2 source,
                         KafkaOwnerIdentityV1 owner, Opened opening) {
            this.delegate = delegate;
            this.source = source;
            this.owner = owner;
            this.opening = opening;
        }

        public CellProviderScopeId providerScopeId() {
            return capability.providerScopeId();
        }

        public BookKeeperCapabilitySnapshotV1 capabilitySnapshot() {
            return capability;
        }

        public CompletionStage<ProviderMutationResultV1<RunLedgerHandleV1>> createRunLedger(
                RunLedgerConfigurationV1 config) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Broker run create requires a durable run birth choice"));
        }

        public CompletionStage<ProviderMutationResultV1<BookKeeperLedgerIdentity>> reserveLedgerIdentity(
                RunLedgerConfigurationV1 config) {
            if (!source.spec().configurations().contains(config)) {
                try {
                    source.admitCreateConfiguration(config);
                } catch (Exception failure) {
                    return CompletableFuture.failedFuture(failure);
                }
            }
            return delegate.reserveLedgerIdentity();
        }

        public CompletionStage<ProviderMutationResultV1<RunLedgerHandleV1>> createReservedRunLedger(
                RunLedgerConfigurationV1 config, BookKeeperLedgerIdentity reservedIdentity) {
            final M5BookKeeperNativeCreateSpecV2 spec = source.spec();
            if (!spec.configurations().contains(config)) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("reserved ledger is outside the active run create scope"));
            }
            var resource = new PhysicalResourceIdV2.BookKeeperLedger(
                    namespaceBinding.physicalNamespace(), reservedIdentity.ledgerId());
            return opening.nativeM5ResourceAuthority(resource, spec)
                    .thenCompose(ignored -> writerAdmission.createUnderTicket(
                    delegate, spec, owner, config, reservedIdentity, resource, runResponses::afterCreate));
        }

        public CompletionStage<RunLedgerOpenResultV1> openRunLedger(RunLedgerHandleV1 handle) {
            return delegate.openRunLedger(handle);
        }

        public CompletionStage<ProviderMutationResultV1<AppendQuorumProofV1>> appendExplicitEntry(
                RunLedgerAppendRequestV1 request) {
            return runResponses.afterAppend(request, delegate.appendExplicitEntry(request));
        }

        public CompletionStage<RunLedgerReadResultV1> readExactEntry(RunLedgerHandleV1 handle, long entry) {
            return delegate.readExactEntry(handle, entry);
        }

        public CompletionStage<ProviderMutationResultV1<RunLedgerRecoveryProofV1>> fenceAndRecoverRunLedger(
                RunLedgerHandleV1 handle) {
            return delegate.fenceAndRecoverRunLedger(handle);
        }

        public CompletionStage<ProviderMutationResultV1<RunLedgerCloseProofV1>> closeRunLedger(
                RunLedgerHandleV1 handle) {
            return delegate.closeRunLedger(handle);
        }

        public CompletionStage<Void> drain() {
            return delegate.drain();
        }

        public CompletionStage<Void> closeAsync() {
            return delegate.closeAsync();
        }
    }

    /** Quota failure is terminal for this create attempt, even if exact OPEN bytes already exist. */
    static <T> CompletionStage<T> afterGrantedQuota(
            CompletionStage<Void> quotaGrant, Supplier<CompletionStage<T>> grantedAction) {
        return quotaGrant.thenCompose(ignored -> grantedAction.get());
    }

    public static org.apache.kafka.metadata.nereus.NereusKafkaMetadataPolicyV1 metadataPolicy(
            kafka.server.KafkaConfig config) throws Exception {
        return NereusBookKeeperCapability.metadataPolicy(config);
    }
}
