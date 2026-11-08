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
import com.nereusstream.storage.object.recovery.OwnerOpenRecoveryCoordinator;
import com.nereusstream.storage.object.recovery.WalRunLineageRecovery;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.oxia.client.api.AsyncOxiaClient;

import static org.apache.kafka.metadata.nereus.KafkaTopicBindingAggregateMapperV1.fromRecord;

/** Native Object WAL opens one exact KRaft partition assignment and its retained source set. */
final class NereusKafkaObjectWalCell implements AutoCloseable {
    private final BrokerStorageRuntimeContext context;
    private final OxiaPhysicalMetadataNamespaceV2 namespace;
    private final AsyncOxiaClient control;
    private final NereusKafkaObjectProvider provider;
    private final NereusKafkaObjectWalKms kms;
    private final NereusKafkaObjectWalRoot roots;
    private final Duration timeout;
    private final Set<NereusKafkaObjectWalPartition> owned = new HashSet<>();
    private boolean closed;

    NereusKafkaObjectWalCell(BrokerStorageRuntimeContext context, OxiaPhysicalMetadataNamespaceV2 namespace,
            AsyncOxiaClient control, NereusKafkaObjectProvider provider, NereusKafkaObjectWalKms kms) {
        this.context = context;
        this.namespace = namespace;
        this.control = control;
        this.provider = provider;
        this.kms = kms;
        this.roots = new NereusKafkaObjectWalRoot(context.config(), provider.profile());
        timeout = context.config().nereusKafkaStorageConfig().lifecycle().recoveryTimeout();
    }

    static NereusKafkaObjectWalCell start(BrokerStorageRuntimeContext context,
            OxiaPhysicalMetadataNamespaceV2 namespace, AsyncOxiaClient control) throws Exception {
        var cell = NereusBookKeeperCapability.metadataPolicy(context.config()).cellIdentity();
        var provider = NereusKafkaObjectProvider.open(context.config(), cell, namespace);
        NereusKafkaObjectWalKms kms = null;
        try {
            kms = NereusKafkaObjectWalKms.open(context.config(), provider.profile());
            return new NereusKafkaObjectWalCell(context, namespace, control, provider, kms);
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
            var attempt = new Open(runs, metadata, authority, fence, owner, assignment, observer);
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

    @Override
    public void close() throws Exception {
        fence();
        List<NereusKafkaObjectWalPartition> closing;
        synchronized (this) {
            closing = List.copyOf(owned);
        }
        CompletableFuture.allOf(closing.stream().map(NereusKafkaObjectWalPartition::closeAsync)
                .toArray(CompletableFuture[]::new)).get(context.config().nereusKafkaStorageConfig().rollout()
                        .shutdownDrainTimeout().toNanos(), TimeUnit.NANOSECONDS);
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
        private final Map<Sha256Digest, NereusKafkaObjectWalPartition.Source> sources = new HashMap<>();

        private Open(NereusKafkaObjectWalRuns runs, OxiaCanonicalControlMetadataStore metadata,
                KafkaObjectAuthorizationV1 authority, KafkaPartitionFenceV1 fence, KafkaOwnerIdentityV1 owner,
                NereusKafkaObjectWalPartition.NativeAssignment assignment, KafkaPartitionPublicationObserver observer) {
            this.runs = runs;
            this.metadata = metadata;
            this.authority = authority;
            this.fence = fence;
            this.owner = owner;
            this.assignment = assignment;
            this.observer = observer;
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
            var lineage = new WalRunLineageRecovery(metadata, terminalVerifier(), runs::terminalReader)
                    .recover(WalRunControlKeys.pointerKey(root.shardId()), root.protocolCellIdentity(),
                            root.providerScopeId(), Optional.empty(), System::nanoTime);
            var protocol = new KafkaObjectAuthorizedOwnerOpenV1(root, authority, closed, origin);
            var verification = KafkaObjectNativeVerificationV1.context(root, origin);
            var execution = KafkaNativePartitionOwnerAuthorityV1.protocolOwnerFenceExecutor(
                    (actual, callback) -> {
                        if (!actual.equals(fence)) throw new FencedLeaderEpochException("wrong Object WAL election");
                        return whileCurrentIo(callback::execute);
                    }, fence, KafkaObjectNativeAppendV1.runBinding(root, origin), root.protocolCellIdentity(),
                    selected.reference(), verification);
            var restored = OwnerOpenRecoveryCoordinator.recoverUnderDurableFence(execution, root.protocolCellIdentity(),
                    selected.reference(), root, metadata, runs.openProvider(root), kms.session(), lineage, protocol);
            ownLive(restored, origin);
            var head = restored.checkpointRecoveredPhysicalRows(metadata, owner.ownerEpoch());
            var predecessor = KafkaObjectRecoveredProtocolClosureV1.seal(
                    restored, metadata, head, protocol.result(), owner.ownerEpoch());
            var recovered = protocol.result();
            openRetainedSources(lineage, closed);
            var lifecycle = new WalRunLifecycleManager(metadata, terminalVerifier(), runs::terminalReader);
            var successor = runs.advance(predecessor, lifecycle, context.time().milliseconds(), owner);
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
            return (root, seal, binding, value, budget) -> {
                try {
                    ObjectKafkaProtocolCheckpointStoreV1.terminalVerifier(
                            root, KafkaObjectNativeAppendV1.checkpointPrefix(root),
                            KafkaObjectNativeAppendV1.runBinding(root, origin(root)))
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
                Optional<KafkaObjectRecoveredTailV1> tail) {
            var writer = KafkaObjectNativeAppendV1.open(fresh, metadata, authority, coordinator, tail, assignment::current);
            if (!assignment.current()) throw new FencedLeaderEpochException("Object WAL election changed before ready");
            return new NereusKafkaObjectWalPartition(coordinator, writer, sources, provider.fetchExecutor(),
                    assignment::current, () -> context.time().milliseconds());
        }

        private <T> T whileCurrentIo(Callable<T> callback) throws IOException {
            try {
                return assignment.whileCurrent(callback);
            } catch (IOException failure) {
                throw failure;
            } catch (Exception failure) {
                throw new IOException("native Object WAL election changed during recovery", failure);
            }
        }
    }
}
