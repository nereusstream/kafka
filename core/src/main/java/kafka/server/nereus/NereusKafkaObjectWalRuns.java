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
import com.nereusstream.metadata.oxia.v2.objectwal.OxiaCanonicalControlMetadataStore;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1;
import com.nereusstream.storage.object.control.CurrentWalRunPointer;
import com.nereusstream.storage.object.control.TerminalProtocolCheckpointVerifierV1;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunHistoricalObjectReaderV1;
import com.nereusstream.storage.object.control.WalRunLifecycleManager;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunPredecessor;
import com.nereusstream.storage.object.control.WalRunReference;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.kms.KmsCellSession;
import com.nereusstream.storage.object.kms.RunKeyCacheIdentity;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.recovery.WalRunLineageRecovery;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

/** Selects the exact physical run for a single native partition. A discovered run is always recover-only. */
final class NereusKafkaObjectWalRuns {
    private static final long GENESIS_EPOCH = 1;
    private static final int MAX_CONTROL_BYTES = 1024 * 1024;

    private final int shard;
    private final NereusKafkaObjectWalShard.Assignment assignment;
    private final ExactMetadataTransactionStoreV1 exactMetadata;
    private final Duration operationTimeout;
    private final NereusKafkaObjectWalOwners owners;
    private final String pointerKey;
    private final OxiaCanonicalControlMetadataStore metadata;
    private final NereusKafkaObjectProvider provider;
    private final NereusKafkaObjectWalKms kms;
    private final NereusKafkaObjectWalRoot roots;
    private final WalRunLifecycleManager lifecycle;

    NereusKafkaObjectWalRuns(NereusKafkaObjectWalShard.Assignment assignment,
            OxiaCanonicalControlMetadataStore metadata, ExactMetadataTransactionStoreV1 exactMetadata,
            NereusKafkaObjectProvider provider, NereusKafkaObjectWalKms kms, NereusKafkaObjectWalRoot roots,
            Duration operationTimeout) {
        this.assignment = assignment;
        this.shard = assignment.id();
        this.exactMetadata = exactMetadata;
        this.operationTimeout = operationTimeout;
        this.owners = new NereusKafkaObjectWalOwners(assignment, exactMetadata, operationTimeout);
        this.pointerKey = WalRunControlKeys.pointerKey(shard);
        this.metadata = metadata;
        this.provider = provider;
        this.kms = kms;
        this.roots = roots;
        this.lifecycle = new WalRunLifecycleManager(metadata);
    }

    record Selected(WalRunReference reference, WalRunRootRecord root, Optional<WalRunObjectSession> freshSession) { }

    void admitNativeOwner(KafkaOwnerIdentityV1 owner) throws Exception {
        owners.admit(owner);
    }

    void requireNativeOwner(KafkaOwnerIdentityV1 owner) throws Exception {
        owners.requireCurrent(owner);
    }

    KafkaOwnerIdentityV1 creator(WalRunRootRecord root) throws Exception {
        return owners.creator(root);
    }

    Optional<Selected> current() {
        return metadata.get(pointerKey).map(bytes -> read(WalRunControlCodec.decodePointer(bytes).current()));
    }

    /** Call only inside the native owner exclusion, after checking the partition's authorization Head. */
    Selected selectOrCreateGenesis(long nowMillis, boolean hasLegalHistory, KafkaOwnerIdentityV1 owner) throws Exception {
        owners.requireCurrent(owner);
        var current = current();
        if (current.isPresent()) return current.orElseThrow();
        if (hasLegalHistory) {
            throw new IllegalStateException("native Object WAL authorization exists without its current pointer");
        }
        String rootKey = WalRunControlKeys.rootKey(shard, GENESIS_EPOCH);
        var orphan = metadata.get(rootKey);
        if (orphan.isPresent()) {
            var bytes = orphan.orElseThrow();
            var root = decodeRoot(bytes);
            var selected = requireRoot(new WalRunReference(rootKey, Sha256Digest.hash(bytes), shard, GENESIS_EPOCH),
                    root);
            if (root.predecessor().isPresent()) {
                throw new IllegalStateException("Object WAL genesis Root has a predecessor");
            }
            lifecycle.initializePointer(pointerKey, new CurrentWalRunPointer(selected.reference()));
            return selected;
        }
        return createGenesis(rootKey, nowMillis, owner);
    }

    private Selected createGenesis(String rootKey, long nowMillis, KafkaOwnerIdentityV1 owner) throws Exception {
        try (var key = kms.session().beginNewRunKey(new RunKeyCacheIdentity(shard, GENESIS_EPOCH))) {
            var root = roots.create(shard, GENESIS_EPOCH, nowMillis, key, Optional.empty());
            owners.recordCreator(root, owner);
            var created = lifecycle.createRootAndInitializePointer(rootKey, root, key);
            if (created.ownerAuthority().isEmpty()) {
                // A lost reply/adoption never grants fresh ownership. Reconcile only this exact Root.
                var observed = metadata.get(pointerKey);
                if (observed.isEmpty()) {
                    lifecycle.initializePointer(pointerKey, new CurrentWalRunPointer(created.reference()));
                }
                var pointer = lifecycle.readPointer(pointerKey);
                if (!pointer.current().equals(created.reference())) {
                    throw new IllegalStateException("Object WAL genesis pointer selected a different exact Root");
                }
                return read(pointer.current());
            }
            return openFresh(created.reference(), root, created.ownerAuthority().orElseThrow(), key, owner);
        }
    }

    /** Call under exact native owner exclusion with a verifier for the complete retained lineage. */
    Selected advance(WalRunPredecessor predecessor, WalRunLifecycleManager verifier, long nowMillis,
            KafkaOwnerIdentityV1 owner) throws Exception {
        owners.requireCurrent(owner);
        if (predecessor.root().shardId() != shard) {
            throw new IllegalArgumentException("native Object WAL predecessor names another shard");
        }
        requireDrained(read(predecessor.root()).root());
        long epoch = Math.incrementExact(predecessor.root().shardRunEpoch());
        String rootKey = WalRunControlKeys.rootKey(shard, epoch);
        var already = metadata.get(rootKey);
        if (already.isPresent()) {
            var bytes = already.orElseThrow();
            var candidate = decodeRoot(bytes);
            requireRoot(new WalRunReference(rootKey, Sha256Digest.hash(bytes), shard, epoch), candidate);
            if (!candidate.predecessor().equals(Optional.of(predecessor))) {
                throw new IllegalStateException("persisted native Object WAL successor names a different predecessor");
            }
            var selected = verifier.completeSealedPointer(pointerKey, predecessor.root(),
                    Optional.of(new WalRunLifecycleManager.SuccessorCandidate(rootKey, candidate)));
            return read(selected.current());
        }
        var sealBytes = metadata.get(predecessor.sealKey())
                .orElseThrow(() -> new IllegalStateException("native Object WAL predecessor Seal is absent"));
        if (sealBytes.length() > MAX_CONTROL_BYTES || !Sha256Digest.hash(sealBytes).equals(predecessor.sealSha256())) {
            throw new IllegalStateException("native Object WAL predecessor Seal has invalid bytes or digest");
        }
        var seal = WalRunControlCodec.decodeSeal(sealBytes);
        try (var key = kms.session().beginNewRunKey(new RunKeyCacheIdentity(shard, epoch))) {
            var root = roots.create(shard, epoch, nowMillis, key, Optional.of(predecessor));
            owners.recordCreator(root, owner);
            key.markRootPublicationAttempt(root);
            var created = verifier.publishSuccessorAndAdvanceWithOwnerAuthority(
                    pointerKey, new CurrentWalRunPointer(predecessor.root()), predecessor.sealKey(), seal, rootKey, root);
            if (created.ownerAuthority().isEmpty()) return read(created.reference());
            return openFresh(created.reference(), root, created.ownerAuthority().orElseThrow(), key, owner);
        }
    }

    private Selected openFresh(WalRunReference reference, WalRunRootRecord root,
            WalRunLifecycleManager.NewWalRunOwnerAuthority authority, KmsCellSession.NewRunKeyCreation key,
            KafkaOwnerIdentityV1 owner) throws Exception {
        owners.requireCurrent(owner);
        var dispatch = dispatch(root);
        dispatch.initializeFresh();
        var runProvider = openProvider(root, dispatch);
        try {
            return new Selected(reference, root, Optional.of(
                    WalRunObjectSession.openNew(authority, runProvider, key, System::nanoTime)));
        } catch (RuntimeException | Error failure) {
            try {
                runProvider.close();
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    Selected read(WalRunReference reference) {
        if (reference.shardId() != shard) throw new IllegalArgumentException("Object WAL pointer names another shard");
        var bytes = metadata.get(reference.rootKey())
                .orElseThrow(() -> new IllegalStateException("selected native Object WAL Root is absent"));
        if (bytes.length() > MAX_CONTROL_BYTES || !Sha256Digest.hash(bytes).equals(reference.rootSha256())) {
            throw new IllegalStateException("selected native Object WAL Root has invalid bytes or digest");
        }
        return requireRoot(reference, decodeRoot(bytes));
    }

    C1ObjectProviderSession openProvider(WalRunRootRecord root) {
        return openProvider(root, null);
    }

    private C1ObjectProviderSession openProvider(
            WalRunRootRecord root, NereusKafkaObjectWalDispatchFence dispatch) {
        roots.requireAdmitted(root, shard);
        var configuration = root.providerConfiguration();
        return provider.openWalRunProvider(configuration.exclusiveNamespacePrefix(), configuration.maxObjectBodyBytes(),
                root.nwg1AdmissionCaps().maxDirectoryPrefixBytes(), dispatch);
    }

    void requireDrained(WalRunRootRecord root) throws IOException {
        try (var readProvider = openProvider(root)) {
            dispatch(root).closeAndRequireDrained(readProvider);
        }
    }

    private NereusKafkaObjectWalDispatchFence dispatch(WalRunRootRecord root) {
        return new NereusKafkaObjectWalDispatchFence(exactMetadata, assignment, root, operationTimeout);
    }

    TerminalProtocolCheckpointVerifierV1.ProtocolObjectRecoveryReaderV1 terminalReader(WalRunRootRecord root) {
        return identity -> {
            try (var readOnlyProvider = openProvider(root)) {
                return readOnlyProvider.readVerifiedObject(identity);
            }
        };
    }

    WalRunHistoricalObjectReaderV1 openHistorical(
            WalRunLineageRecovery.RecoveredLineage lineage, WalRunReference reference) {
        var selected = lineage.requireSealedPredecessor(reference);
        var readProvider = openProvider(selected.root());
        try {
            return new WalRunHistoricalObjectReaderV1(lineage, reference, readProvider, kms.session());
        } catch (RuntimeException | Error failure) {
            try {
                readProvider.close();
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private Selected requireRoot(WalRunReference reference, WalRunRootRecord root) {
        roots.requireAdmitted(root, shard);
        if (root.shardRunEpoch() != reference.shardRunEpoch()) {
            throw new IllegalStateException("selected native Object WAL Root differs from its reference");
        }
        return new Selected(reference, root, Optional.empty());
    }

    private static WalRunRootRecord decodeRoot(CanonicalBytes bytes) {
        if (bytes.length() > MAX_CONTROL_BYTES) {
            throw new IllegalStateException("native Object WAL Root exceeds the control cap");
        }
        return WalRunControlCodec.decodeRoot(bytes);
    }
}
