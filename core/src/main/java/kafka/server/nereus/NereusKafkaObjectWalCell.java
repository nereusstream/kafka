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

import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.kafka.bookkeeper.broker.KafkaObjectNativeAppendV1;
import com.nereusstream.kafka.bookkeeper.broker.KafkaObjectNativeVerificationV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentCommitCoordinatorV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaObjectAuthorizedOwnerOpenV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaObjectRecoveredProtocolClosureV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaObjectRecoveredTailV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.ObjectKafkaProtocolCheckpointStoreV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectAuthorizationV1;
import com.nereusstream.kafka.bookkeeper.object.recovery.KafkaNativePartitionOwnerAuthorityV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionPublicationObserver;
import com.nereusstream.metadata.oxia.v2.objectwal.OxiaCanonicalControlMetadataStore;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaKafkaBookKeeperM4ReadRouteV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.control.TerminalProtocolCheckpointVerifierV1;
import com.nereusstream.storage.object.control.WalCheckpointHeadV1;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunLifecycleManager;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.recovery.OwnerOpenRecoveryCoordinator;
import com.nereusstream.storage.object.recovery.WalRunLineageRecovery;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import io.oxia.client.api.AsyncOxiaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.metadata.nereus.KafkaTopicBindingAggregateMapperV1.fromRecord;

/** Native Object WAL opens one exact KRaft partition assignment and its retained source set. */
final class NereusKafkaObjectWalCell implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(NereusKafkaObjectWalCell.class);
    private final BrokerStorageRuntimeContext context;
    private final OxiaPhysicalMetadataNamespaceV2 namespace;
    private final AsyncOxiaClient control;
    private final NereusKafkaObjectProvider provider;
    private final NereusKafkaObjectWalKms kms;
    private final NereusKafkaObjectWalRoot roots;
    private final Executor recoveryExecutor;
    private final Duration timeout;
    private final Optional<NereusKafkaObjectReadAdmissionV1> readAdmission;
    private final Set<NereusKafkaObjectWalPartition> owned = new LinkedHashSet<>();
    private final ScheduledFuture<?> compactionSchedule;
    private CompletableFuture<Void> activeCompaction = CompletableFuture.completedFuture(null);
    private boolean closed;

    NereusKafkaObjectWalCell(BrokerStorageRuntimeContext context, OxiaPhysicalMetadataNamespaceV2 namespace,
            AsyncOxiaClient control, NereusKafkaObjectProvider provider, NereusKafkaObjectWalKms kms,
            Executor recoveryExecutor, Optional<NereusKafkaObjectReadAdmissionV1> readAdmission) {
        this.context = context;
        this.namespace = namespace;
        this.control = control;
        this.provider = provider;
        this.kms = kms;
        this.recoveryExecutor = recoveryExecutor;
        this.readAdmission = readAdmission;
        this.roots = new NereusKafkaObjectWalRoot(context.config(), provider.profile());
        timeout = context.config().nereusKafkaStorageConfig().lifecycle().recoveryTimeout();
        long period = context.config().nereusKafkaStorageConfig().retentionCompaction().retentionCheckInterval().toMillis();
        compactionSchedule = readAdmission.isEmpty() ? null
                : context.scheduler().schedule("nereus-kafka-object-wal-compaction", this::compact, period, period);
    }

    static NereusKafkaObjectWalCell start(BrokerStorageRuntimeContext context,
            OxiaPhysicalMetadataNamespaceV2 namespace, AsyncOxiaClient control, Executor recoveryExecutor) throws Exception {
        var cell = NereusBookKeeperCapability.metadataPolicy(context.config()).cellIdentity();
        var provider = NereusKafkaObjectProvider.open(context.config(), cell, namespace);
        NereusKafkaObjectWalKms kms = null;
        try {
            kms = NereusKafkaObjectWalKms.open(context.config(), provider.profile());
            return new NereusKafkaObjectWalCell(context, namespace, control, provider, kms, recoveryExecutor,
                    NereusKafkaObjectReadAdmissionV1.load(context.config(), provider));
        } catch (Exception | Error failure) {
            if (kms != null) {
                try {
                    kms.close();
                } catch (RuntimeException cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            try {
                provider.close();
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    static boolean selectsObjectWal(TopicBindingAggregateRecord aggregate) {
        return fromRecord(aggregate, (short) 0).storageProfile() == StorageProfileV1.OBJECT_WAL;
    }

    NereusKafkaObjectWalPartition open(TopicIdPartition identity, TopicBindingAggregateRecord aggregate,
            KafkaOwnerIdentityV1 owner, NereusKafkaObjectWalPartition.NativeAssignment assignment,
            KafkaPartitionPublicationObserver observer) throws Exception {
        return assignment.whileCurrent(() -> {
            synchronized (this) {
                if (closed) throw new FencedLeaderEpochException("Object WAL Cell is draining");
            }
            NereusBookKeeperCapability.validateAggregate(context.config(), aggregate);
            var fence = fence(identity, aggregate, owner);
            namespace.requireCurrent().toCompletableFuture().get(timeout.toNanos(), TimeUnit.NANOSECONDS);
            provider.requireCurrent().toCompletableFuture().get(timeout.toNanos(), TimeUnit.NANOSECONDS);
            var exact = new Oxia09ExactMetadataTransactionStoreV1(control);
            var shard = NereusKafkaObjectWalShard.reserve(exact, provider.profile().protocolCellSha256(), fence, timeout);
            var metadata = new OxiaCanonicalControlMetadataStore(control, shard.cellRoot(), shard.id());
            var runs = new NereusKafkaObjectWalRuns(shard, metadata, exact, provider, kms, roots, timeout);
            var authority = new KafkaObjectAuthorizationV1(metadata, shard.id(),
                    new KafkaObjectAuthorizationV1.Bounds(2048, 1024, 1024L * 64 * 1024 * 1024, 1024));
            var attempt = new Open(runs, metadata, authority, fence, owner, assignment, observer,
                    identity, Math.addExact(aggregate.epochOrdinal(), 1));
            var opened = attempt.open();
            synchronized (this) {
                if (closed) {
                    opened.closeAsync().join();
                    throw new FencedLeaderEpochException("Object WAL Cell drained during native recovery");
                }
                owned.add(opened);
            }
            return opened;
        });
    }

    synchronized void forget(NereusKafkaObjectWalPartition partition) {
        owned.remove(partition);
    }

    synchronized void fence() {
        closed = true;
        List.copyOf(owned).forEach(NereusKafkaObjectWalPartition::fence);
    }

    private synchronized void compact() {
        if (!activeCompaction.isDone()) return;
        var candidate = closed ? Optional.<NereusKafkaObjectWalPartition>empty()
                : owned.stream().filter(NereusKafkaObjectWalPartition::compactionReady).findFirst();
        candidate.ifPresent(partition -> {
            owned.remove(partition);
            owned.add(partition);
        });
        activeCompaction = CompletableFuture.completedFuture(null).thenComposeAsync(ignored -> {
            provider.reconcileOriginalReads();
            return candidate.map(partition -> partition.advanceCompaction(recoveryExecutor))
                    .orElseGet(() -> CompletableFuture.completedFuture(null));
        }, recoveryExecutor).exceptionally(failure -> {
            LOG.warn("Native Kafka Object WAL maintenance retains its current operation", failure);
            return null;
        });
    }

    @Override
    public void close() throws Exception {
        fence();
        List<NereusKafkaObjectWalPartition> closing;
        synchronized (this) {
            closing = List.copyOf(owned);
        }
        try {
            CompletableFuture.allOf(closing.stream().map(NereusKafkaObjectWalPartition::closeAsync)
                    .toArray(CompletableFuture[]::new)).get(context.config().nereusKafkaStorageConfig().rollout()
                            .shutdownDrainTimeout().toNanos(), TimeUnit.NANOSECONDS);
        } finally {
            if (compactionSchedule != null) compactionSchedule.cancel(false);
        }
        provider.drainFetches();
        kms.close();
        provider.close();
    }

    private static KafkaPartitionFenceV1 fence(TopicIdPartition identity, TopicBindingAggregateRecord aggregate,
            KafkaOwnerIdentityV1 owner) {
        if (!selectsObjectWal(aggregate) || !identity.topicId().equals(aggregate.topicId())
                || !identity.topic().equals(aggregate.topicName())) {
            throw new IllegalArgumentException("native Object WAL assignment differs from the persisted Binding");
        }
        var topic = new KafkaTopicIncarnationIdentity(new KafkaTopicId(new Id128(
                identity.topicId().getMostSignificantBits(), identity.topicId().getLeastSignificantBits())),
                new KafkaTopicName(identity.topic()));
        return new KafkaPartitionFenceV1(new TopicBindingId(Sha256Digest.copyOf(aggregate.bindingId())),
                topic, identity.partition(), 1, new StorageEpochId(Sha256Digest.copyOf(aggregate.storageEpochId())),
                owner.ownerEpoch(), owner.kafkaLeaderEpoch());
    }

    private final class Open {
        private final NereusKafkaObjectWalRuns runs;
        private final OxiaCanonicalControlMetadataStore metadata;
        private final KafkaObjectAuthorizationV1 authority;
        private final KafkaPartitionFenceV1 fence;
        private final KafkaOwnerIdentityV1 owner;
        private final NereusKafkaObjectWalPartition.NativeAssignment assignment;
        private final KafkaPartitionPublicationObserver observer;
        private final TopicIdPartition identity;
        private final long storageFence;
        private final Map<Sha256Digest, NereusKafkaObjectWalPartition.Source> sources = new HashMap<>();

        private Open(NereusKafkaObjectWalRuns runs, OxiaCanonicalControlMetadataStore metadata,
                KafkaObjectAuthorizationV1 authority, KafkaPartitionFenceV1 fence, KafkaOwnerIdentityV1 owner,
                NereusKafkaObjectWalPartition.NativeAssignment assignment, KafkaPartitionPublicationObserver observer,
                TopicIdPartition identity, long storageFence) {
            this.runs = runs;
            this.metadata = metadata;
            this.authority = authority;
            this.fence = fence;
            this.owner = owner;
            this.assignment = assignment;
            this.observer = observer;
            this.identity = identity;
            this.storageFence = storageFence;
        }

        private NereusKafkaObjectWalPartition open() throws Exception {
            try {
                runs.admitNativeOwner(owner);
                var before = authority.read(fence);
                var selected = runs.selectOrCreateGenesis(context.time().milliseconds(), before.isPresent(), owner);
                if (selected.freshSession().isPresent()) {
                    var fresh = selected.freshSession().orElseThrow();
                    ownLive(fresh, fence);
                    if (before.isPresent()) throw new IllegalStateException("native Object WAL fresh genesis has history");
                    authority.open(fence, 0, Optional.empty());
                    return arm(fresh, KafkaCoherentCommitCoordinatorV1.bootstrapObject(fence, 0, observer),
                            Optional.empty());
                }
                return takeOver(selected);
            } catch (Exception | Error failure) {
                for (var source : sources.values()) {
                    try {
                        source.close().run();
                    } catch (RuntimeException closeFailure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
                throw failure;
            }
        }

        private NereusKafkaObjectWalPartition takeOver(NereusKafkaObjectWalRuns.Selected selected) throws Exception {
            var root = selected.root();
            var origin = origin(root);
            if (origin.ownerEpoch() >= fence.ownerEpoch()) {
                throw new FencedLeaderEpochException("Object WAL recover-only Root requires a newer native election");
            }
            var old = authority.read(fence);
            if (old.filter(head -> head.fence().ownerEpoch() >= fence.ownerEpoch()).isPresent()) {
                throw new FencedLeaderEpochException("Object WAL authorization is already at or beyond this election");
            }
            var closed = old.isPresent() ? authority.close(old.orElseThrow().fence()) : null;
            runs.requireDrained(root);
            if (closed == null) {
                closed = authority.closeAbsentGenesis(origin);
            }
            ensurePhysicalHead(root, selected.reference().rootSha256(), origin.ownerEpoch());
            var predecessor = root.predecessor().map(link -> link.root());
            var predecessorCheckpoint =
                    new AtomicReference<ObjectKafkaProtocolCheckpointStoreV1.VerifiedTerminalCheckpoint>();
            var lineage = new WalRunLineageRecovery(metadata, terminalVerifier(checkpoint -> {
                if (predecessor.equals(Optional.of(checkpoint.root()))) {
                    predecessorCheckpoint.set(checkpoint);
                }
            }), runs::terminalReader)
                    .recover(WalRunControlKeys.pointerKey(root.shardId()), root.protocolCellIdentity(),
                            root.providerScopeId(), Optional.empty(), System::nanoTime);
            var protocol = new KafkaObjectAuthorizedOwnerOpenV1(root, authority, closed, origin,
                    Optional.ofNullable(predecessorCheckpoint.get()));
            var verification = KafkaObjectNativeVerificationV1.context(root, origin);
            var execution = KafkaNativePartitionOwnerAuthorityV1.protocolOwnerFenceExecutor(
                    (actual, callback) -> {
                        if (!actual.equals(fence)) throw new FencedLeaderEpochException("wrong Object WAL election");
                        return whileCurrentIo(() -> {
                            runs.requireNativeOwner(owner);
                            return callback.execute();
                        });
                    }, fence, KafkaObjectNativeAppendV1.runBinding(root, origin), root.protocolCellIdentity(),
                    selected.reference(), verification);
            var restored = OwnerOpenRecoveryCoordinator.recoverUnderDurableFence(execution, root.protocolCellIdentity(),
                    selected.reference(), root, metadata, runs.openProvider(root), kms.session(), lineage, protocol);
            ownLive(restored, origin);
            runs.requireNativeOwner(owner);
            var head = restored.checkpointRecoveredPhysicalRows(metadata, owner.ownerEpoch());
            runs.requireNativeOwner(owner);
            var sealed = KafkaObjectRecoveredProtocolClosureV1.seal(
                    restored, metadata, head, protocol.result(), owner.ownerEpoch());
            var recovered = protocol.result();
            openRetainedSources(lineage, closed);
            var lifecycle = new WalRunLifecycleManager(metadata, terminalVerifier(), runs::terminalReader);
            var successor = runs.advance(sealed, lifecycle, context.time().milliseconds(), owner);
            var fresh = successor.freshSession().orElseThrow(
                    () -> new FencedLeaderEpochException("Object WAL successor must reenter through owner recovery"));
            ownLive(fresh, fence);
            long end = closed.head().endOffset();
            var coordinator = KafkaCoherentCommitCoordinatorV1.bootstrapObjectRecovered(fence, closed.head().startOffset(),
                    end, end, recovered.state(), recovered.tail(), observer);
            authority.open(fence, end, Optional.of(closed));
            authority.checkpoint(fresh.rootSha256(), KafkaObjectNativeAppendV1.runBinding(fresh.rootRecord(), fence),
                    coordinator);
            return arm(fresh, coordinator, Optional.of(recovered.tail()));
        }

        private void ensurePhysicalHead(WalRunRootRecord root, Sha256Digest digest, long epoch) {
            String key = WalRunControlKeys.checkpointHeadKey(root.shardId(), root.shardRunEpoch());
            if (metadata.get(key).isPresent()) return;
            var candidate = WalRunControlCodec.encodeCheckpointHead(WalCheckpointHeadV1.empty(
                    digest, root.shardRunEpoch(), epoch));
            if (metadata.putIfAbsent(key, candidate) != ControlMutationOutcome.APPLIED
                    && !metadata.get(key).equals(Optional.of(candidate))) {
                throw new IllegalStateException("native Object WAL original empty physical Head did not converge");
            }
        }

        private KafkaPartitionFenceV1 origin(WalRunRootRecord root) throws Exception {
            var creator = runs.creator(root);
            return new KafkaPartitionFenceV1(fence.bindingId(), fence.topicIncarnation(), fence.partitionId(),
                    fence.bindingGeneration(), fence.storageEpochId(), creator.ownerEpoch(), creator.kafkaLeaderEpoch());
        }

        private TerminalProtocolCheckpointVerifierV1 terminalVerifier() {
            return terminalVerifier(ignored -> { });
        }

        private TerminalProtocolCheckpointVerifierV1 terminalVerifier(
                Consumer<ObjectKafkaProtocolCheckpointStoreV1.VerifiedTerminalCheckpoint> verified) {
            return (root, seal, binding, value, budget) -> {
                try {
                    ObjectKafkaProtocolCheckpointStoreV1.terminalVerifier(
                            root, KafkaObjectNativeAppendV1.checkpointPrefix(root),
                            KafkaObjectNativeAppendV1.runBinding(root, origin(root)), verified)
                            .verifyTerminal(root, seal, binding, value, budget);
                } catch (RuntimeException failure) {
                    throw failure;
                } catch (Exception failure) {
                    throw new IllegalStateException("terminal Object WAL Root creator is unavailable", failure);
                }
            };
        }

        private void openRetainedSources(WalRunLineageRecovery.RecoveredLineage lineage,
                KafkaObjectAuthorizationV1.ClosedHistory closed) throws Exception {
            for (var grant : closed.head().grants()) {
                var digest = grant.locator().extent().walRunRootSha();
                if (sources.containsKey(digest)) continue;
                var retained = lineage.runs().stream().filter(run -> run.reference().rootSha256().equals(digest))
                        .findFirst().orElseThrow(() -> new IllegalStateException("authorized Object WAL Root is absent"));
                var origin = origin(retained.root());
                if (!origin.equals(grant.fence())) throw new IllegalStateException("retained Object WAL origin differs");
                var verification = KafkaObjectNativeVerificationV1.context(retained.root(), origin);
                sources.put(digest, NereusKafkaObjectWalPartition.Source.historical(
                        runs.openHistorical(lineage, retained.reference()), verification));
            }
        }

        private void ownLive(WalRunObjectSession session, KafkaPartitionFenceV1 origin) {
            try {
                sources.put(session.rootSha256(), NereusKafkaObjectWalPartition.Source.live(
                        session, KafkaObjectNativeVerificationV1.context(session.rootRecord(), origin)));
            } catch (RuntimeException | Error failure) {
                try {
                    session.close();
                } catch (RuntimeException cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        }

        private NereusKafkaObjectWalPartition arm(WalRunObjectSession fresh, KafkaCoherentCommitCoordinatorV1 coordinator,
                Optional<KafkaObjectRecoveredTailV1> tail) throws Exception {
            var writer = KafkaObjectNativeAppendV1.open(fresh, metadata, authority, coordinator, tail, assignment::current);
            if (!assignment.current()) throw new FencedLeaderEpochException("Object WAL election changed before ready");
            var partition = new NereusKafkaObjectWalPartition(coordinator, writer, sources, provider.fetchExecutor(),
                    recoveryExecutor, assignment, () -> context.time().milliseconds(), this::rollover);
            installCompaction(partition);
            return partition;
        }

        private void installCompaction(NereusKafkaObjectWalPartition partition) throws Exception {
            var binding = new BindingIdentity(fence.bindingId(),
                    Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(fence.topicIncarnation())),
                    fence.storageEpochId().digest());
            var route = OxiaKafkaBookKeeperM4ReadRouteV2.forObjectWal(control, namespace.identity(), identity.partition(), binding);
            var previous = route.readSelector();
            if (previous.isEmpty() && (readAdmission.isEmpty()
                    || !NereusKafkaM5PolicyInputV1.captureObjectWalPolicy(context, identity).config().compact)) return;
            var admission = readAdmission.orElseThrow(() ->
                    new IllegalStateException("native Object WAL selection requires its exact read qualification"));
            var rawView = NereusKafkaM4AdmissionV2.digest("NEREUS_KAFKA_OBJECT_WAL_RAW_BINDING_VIEW_V1",
                    binding.bindingId().digest().toHex(), binding.incarnationSha256().toHex(),
                    binding.storageEpochSha256().toHex(), Integer.toString(identity.partition()));
            requireCompactionOwner();
            var selector = admission.provision(route, previous, previous.map(value -> value.selectedViewSha256()).orElse(rawView),
                    owner.ownerEpoch());
            var worker = new NereusKafkaObjectWalCompaction(context, identity, partition, authority, route, provider,
                    this::requireCompactionOwner, owner.ownerEpoch(), storageFence, NereusKafkaNativeInputV2.nativeWorkerFence(owner), rawView);
            worker.recover(selector);
            partition.attachCompaction(worker);
        }

        private void requireCompactionOwner() {
            if (!assignment.current()) throw new FencedLeaderEpochException("native Object WAL compaction owner changed");
            try {
                runs.requireNativeOwner(owner);
            } catch (RuntimeException failure) {
                throw failure;
            } catch (Exception failure) {
                throw new CompletionException(failure);
            }
        }

        private NereusKafkaObjectWalPartition.RolledRun rollover(KafkaObjectNativeAppendV1 writer) throws Exception {
            var predecessor = writer.sealForRollover();
            var lifecycle = new WalRunLifecycleManager(metadata, terminalVerifier(), runs::terminalReader);
            var selected = runs.advance(predecessor, lifecycle, context.time().milliseconds(), owner);
            var fresh = selected.freshSession().orElseThrow(
                    () -> new FencedLeaderEpochException("Object WAL rollover successor requires native recovery"));
            try {
                var source = NereusKafkaObjectWalPartition.Source.live(
                        fresh, KafkaObjectNativeVerificationV1.context(fresh.rootRecord(), fence));
                var successor = writer.openSuccessor(fresh);
                return new NereusKafkaObjectWalPartition.RolledRun(successor, fresh.rootSha256(), source);
            } catch (RuntimeException | Error failure) {
                try {
                    fresh.close();
                } catch (RuntimeException cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        }

        private <T> T whileCurrentIo(Callable<T> callback) throws IOException {
            try {
                return assignment.whileCurrent(callback);
            } catch (IOException | FencedLeaderEpochException failure) {
                throw failure;
            } catch (Exception failure) {
                throw new IOException("native Object WAL election changed during recovery", failure);
            }
        }
    }
}
