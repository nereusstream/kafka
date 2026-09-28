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

import kafka.server.storage.BrokerStorageRuntimeContext;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.errors.FencedLeaderEpochException;
import org.apache.kafka.common.metadata.TopicBindingAggregateRecord;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1;
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendCapacityBudgetV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendCapacityControllerV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionPublicationObserver;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperNativeRootVerifierV2;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.storage.api.bookkeeper.AppendQuorumProofV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
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
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;

/** Owns real BK/Oxia connections. Native assignment is checked by the lifecycle supplied current predicate. */
public final class NereusKafkaOwnedProviderRuntime implements AutoCloseable {
    private final BrokerStorageRuntimeContext context;
    private final ThreadPoolExecutor openings;
    private final List<Opened> owned = new ArrayList<>();
    private final KafkaAppendCapacityControllerV1 globalCapacity;
    private BookKeeperCapabilitySnapshotV1 capability;
    private AsyncOxiaClient oxia;
    private M5BookKeeperNamespaceAuthorityV2 backend;
    private OxiaPhysicalMetadataNamespaceV2 namespace;
    private PhysicalNamespaceAuthorityBindingV2 namespaceBinding;
    private OxiaQuotaTargetDeleteStoreV2 route;
    private boolean closed;

    public NereusKafkaOwnedProviderRuntime(BrokerStorageRuntimeContext context) {
        this.context = context;
        var config = context.config().nereusKafkaStorageConfig();
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
                },
                new ThreadPoolExecutor.AbortPolicy());
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
                        oxia = await(OxiaClientBuilder.create(
                                        config.core().oxiaServiceAddress().orElseThrow())
                                .namespace(config.core().oxiaNamespace())
                                .requestTimeout(config.lifecycle().recoveryTimeout())
                                .asyncClient());
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
                        route = await(
                                namespace.openAuthorityRoute(backend, new Oxia09ExactMetadataTransactionStoreV1(oxia)));
                        await(route.initialize(500_000_000L));
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
                        var scope = new KafkaRunRootRecordV2.Scope(
                                new TopicBindingId(Sha256Digest.copyOf(aggregate.bindingId())),
                                new KafkaTopicIncarnationIdentity(
                                        new KafkaTopicId(new Id128(
                                                identity.topicId().getMostSignificantBits(),
                                                identity.topicId().getLeastSignificantBits())),
                                        new KafkaTopicName(identity.topic())),
                                identity.partition(),
                                new StorageEpochId(Sha256Digest.copyOf(aggregate.storageEpochId())),
                                capability.providerScopeId());
                        long ownerEpoch = Math.addExact((long) leaderEpoch, 1);
                        var owner = new KafkaOwnerIdentityV1(
                                ownerEpoch, leaderEpoch, context.config().brokerId(), brokerEpoch, metadataOffset);
                        var firstRunId = runId(scope, ownerEpoch);
                        var spec = M5BookKeeperNativeCreateSpecV2.of(
                                M5BookKeeperNativeCreateClientV2.discoverInstanceId(uri(), capability),
                                digest("native-bk-run-scope/"
                                        + firstRunId.value().toHex()),
                                List.of(RunLedgerConfigurationV1.from(capability, firstRunId)));
                        var source =
                                M5BookKeeperNativeCreateClientV2.connect(uri(), capability, spec, namespaceBinding);
                        var session = source.newSession();
                        opened = new Opened(source, session);
                        synchronized (this) {
                            if (closed) {
                                opened.close();
                                throw new IllegalStateException("native provider is draining");
                            }
                            owned.add(opened);
                        }
                        var roots = await(namespace.openKafkaRunRoots(
                                backend,
                                new Oxia09ExactMetadataTransactionStoreV1(oxia),
                                scope,
                                new KafkaBookKeeperNativeRootVerifierV2(source)));
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
                        opened.storage = await(KafkaBookKeeperPartitionV1.open(
                                new AdmittingSession(session, source),
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
                                current));
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
                openings);
    }

    public final class Opened implements AutoCloseable {
        private final M5BookKeeperNativeCreateClientV2 source;
        private final BookKeeperCellSession session;
        private KafkaBookKeeperPartitionV1 storage;
        private boolean released;

        Opened(M5BookKeeperNativeCreateClientV2 source, BookKeeperCellSession session) {
            this.source = source;
            this.session = session;
        }

        public KafkaBookKeeperPartitionV1 storage() {
            return storage;
        }

        public void fence() {
            if (storage != null) storage.fence();
        }

        @Override
        public synchronized void close() throws Exception {
            if (released) return;
            released = true;
            fence();
            try {
                await(session.closeAsync());
            } finally {
                source.close();
            }
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
        try {
            if (backend != null) backend.close();
        } catch (Exception closeFailure) {
            if (failure == null) failure = closeFailure;
            else failure.addSuppressed(closeFailure);
        }
        if (oxia != null) oxia.close();
        if (failure != null) throw failure;
    }

    private String uri() {
        return context.config()
                .nereusKafkaStorageConfig()
                .core()
                .bookKeeperMetadataServiceUri()
                .orElseThrow()
                .toString();
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

    private final class AdmittingSession implements BookKeeperCellSession {
        private final BookKeeperCellSession delegate;
        private final M5BookKeeperNativeCreateClientV2 source;

        AdmittingSession(BookKeeperCellSession delegate, M5BookKeeperNativeCreateClientV2 source) {
            this.delegate = delegate;
            this.source = source;
        }

        public CellProviderScopeId providerScopeId() {
            return capability.providerScopeId();
        }

        public BookKeeperCapabilitySnapshotV1 capabilitySnapshot() {
            return capability;
        }

        public CompletionStage<ProviderMutationResultV1<RunLedgerHandleV1>> createRunLedger(
                RunLedgerConfigurationV1 config) {
            final M5BookKeeperNativeCreateSpecV2 spec;
            try {
                spec = source.admitCreateConfiguration(config);
            } catch (Exception failure) {
                return CompletableFuture.failedFuture(failure);
            }
            return delegate.createRunLedger(config).thenCompose(result -> {
                if (result.exactProof().isEmpty()) return CompletableFuture.completedFuture(result);
                var handle = result.exactProof().orElseThrow();
                var resource = new PhysicalResourceIdV2.BookKeeperLedger(
                        namespaceBinding.physicalNamespace(),
                        handle.ledgerIdentity().ledgerId());
                var enrollment = new M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterEnrollmentV1(
                        List.of(M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1.values()),
                        capability.configurationDigest(),
                        capability.clientArtifactSha256(),
                        spec.sha256());
                var authority = M5TargetDeleteAuthorityStateMachineV1.open(
                        M5TargetDeleteAuthorityRecordsV1.PhysicalDeleteTargetV1.create(resource),
                        enrollment,
                        resource.sha256());
                // This OPEN route authorizes writers only; no M4 release or deletion eligibility is fabricated.
                long deadline = System.nanoTime()
                        + context.config()
                                .nereusKafkaStorageConfig()
                                .lifecycle()
                                .recoveryTimeout()
                                .toNanos();
                return reserveAuthority(resource, deadline, 128)
                        .thenCompose(ignored -> route.compareAndSet(
                                Optional.empty(),
                                resource.authorityKey(),
                                M5TargetDeleteAuthorityCodecV1.encodeAuthority(authority)))
                        .thenApply(outcome -> {
                            if (outcome
                                    != com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1
                                            .MutationOutcome.APPLIED_EXACT)
                                throw new IllegalStateException("native writer authority open unresolved");
                            return result;
                        });
            });
        }

        private CompletionStage<Void> reserveAuthority(PhysicalResourceIdV2 resource, long deadline, int attempts) {
            return route.quota().reserve(resource).thenCompose(result -> {
                if (result == M5GcQuotaCoordinatorV2.Result.GRANTED) return CompletableFuture.completedFuture(null);
                if (result == M5GcQuotaCoordinatorV2.Result.RETRY && attempts > 1 && System.nanoTime() < deadline)
                    return reserveAuthority(resource, deadline, attempts - 1);
                return CompletableFuture.failedFuture(
                        new IllegalStateException("native physical authority quota: " + result));
            });
        }

        public CompletionStage<RunLedgerOpenResultV1> openRunLedger(RunLedgerHandleV1 handle) {
            return delegate.openRunLedger(handle);
        }

        public CompletionStage<ProviderMutationResultV1<AppendQuorumProofV1>> appendExplicitEntry(
                RunLedgerAppendRequestV1 request) {
            return delegate.appendExplicitEntry(request);
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

    public static org.apache.kafka.metadata.nereus.NereusKafkaMetadataPolicyV1 metadataPolicy(
            kafka.server.KafkaConfig config) throws Exception {
        return NereusBookKeeperCapability.metadataPolicy(config);
    }
}
