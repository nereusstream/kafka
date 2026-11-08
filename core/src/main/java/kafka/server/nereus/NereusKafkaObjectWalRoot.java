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

import kafka.server.KafkaConfig;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import com.nereusstream.storage.object.control.Nwg1RootAdmissionCaps;
import com.nereusstream.storage.object.control.ObjectProviderAccessProfile;
import com.nereusstream.storage.object.control.ObjectProviderRootConfiguration;
import com.nereusstream.storage.object.control.ProviderProofMode;
import com.nereusstream.storage.object.control.WalCheckpointPolicy;
import com.nereusstream.storage.object.control.WalRunBounds;
import com.nereusstream.storage.object.control.WalRunFormatContractV1;
import com.nereusstream.storage.object.control.WalRunPredecessor;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.kms.KmsCellSession;
import com.nereusstream.storage.object.materialization.M5CellProviderProfileV2;
import com.nereusstream.storage.object.nwg1.Nwg1ConstantsV1;

import java.util.Optional;
import java.util.UUID;

/** Constructs and validates a partition run against the actually admitted provider Cell. */
final class NereusKafkaObjectWalRoot {
    private static final int MAX_EXTENTS = 1024;
    private static final int MAX_PREDECESSORS = 4;
    private static final int MAX_FRAMES_PER_EXTENT = 64;
    private static final int CHECKPOINT_ROWS = 64;
    private static final int MAX_UNCHECKPOINTED_EXTENTS = 16;
    private static final int MAX_RECOVERY_RETRIES = 16;
    private static final int MAX_DIRECTORY_PREFIX_BYTES = 16 * 1024;
    private static final long MAX_RUN_AGE_MILLIS = 30 * 60 * 1000L;

    private final M5CellProviderProfileV2 profile;
    private final Sha256Digest capabilityDigest;
    private final String wrappingKeyId;
    private final Nwg1RootAdmissionCaps caps;
    private final WalRunBounds bounds;
    private final WalCheckpointPolicy checkpoint;
    private final long recoveryTimeoutNanos;

    NereusKafkaObjectWalRoot(KafkaConfig config, M5CellProviderProfileV2 profile) {
        this.profile = profile;
        capabilityDigest = Sha256Digest.hash(profile.encode());
        wrappingKeyId = NereusKafkaObjectWalKms.keyIdentity(config);
        var storage = config.nereusKafkaStorageConfig();
        int tagBytes = Nwg1ConstantsV1.GCM_TAG_BYTES;
        long body = Math.min(profile.maximumObjectBytes(),
                Math.min(WalRunRootRecord.IMPLEMENTATION_MAX_CANONICAL_BODY_BYTES, storage.append().inflightBytes()));
        int prefix = Math.toIntExact(Math.min(MAX_DIRECTORY_PREFIX_BYTES,
                Math.min(profile.maximumRangeBytes(), body / 2)));
        int frame = Math.toIntExact(Math.min(storage.fetch().maxEntryBytes(),
                Math.min(profile.maximumRangeBytes() - tagBytes, body - prefix - tagBytes)));
        caps = new Nwg1RootAdmissionCaps(body, prefix, prefix - Nwg1ConstantsV1.HEADER_BYTES - tagBytes,
                1, 1, MAX_FRAMES_PER_EXTENT, frame, Math.addExact(frame, tagBytes), body, body);
        bounds = new WalRunBounds(MAX_EXTENTS, Math.multiplyExact(MAX_EXTENTS, body),
                MAX_RUN_AGE_MILLIS, MAX_PREDECESSORS);
        long interval = storage.lifecycle().checkpointInterval().toMillis();
        checkpoint = new WalCheckpointPolicy(interval, MAX_UNCHECKPOINTED_EXTENTS,
                Math.multiplyExact(MAX_UNCHECKPOINTED_EXTENTS, body), Math.multiplyExact(2L, Math.max(1L, interval)),
                CHECKPOINT_ROWS, WalCheckpointPolicy.SUCCESSOR_PAGE_FIXED_BYTES
                        + CHECKPOINT_ROWS * WalCheckpointPolicy.PROOF_NONE_ROW_BYTES);
        recoveryTimeoutNanos = storage.lifecycle().recoveryTimeout().toNanos();
    }

    WalRunRootRecord create(int shard, long runEpoch, long nowMillis, KmsCellSession.NewRunKeyCreation key,
            Optional<WalRunPredecessor> predecessor) {
        var sessionId = UUID.randomUUID();
        var provider = provider(shard, runEpoch);
        return new WalRunRootRecord(shard, runEpoch,
                new Id128(sessionId.getMostSignificantBits(), sessionId.getLeastSignificantBits()), nowMillis,
                profile.protocolCell(), profile.scopeId(), WalRunFormatContractV1.frozen(), caps, bounds, checkpoint,
                provider, WalRunRootRecord.boundedRecoveryEnvelope(
                        ProtocolKindV1.KAFKA, bounds, caps, checkpoint, provider, 1, MAX_RECOVERY_RETRIES,
                        recoveryTimeoutNanos),
                key.wrappedRunKey(), predecessor);
    }

    void requireAdmitted(WalRunRootRecord root, int shard) {
        root.requireM3ProductionProviderProofMode();
        var provider = root.providerConfiguration();
        if (root.shardId() != shard
                || !root.protocolCellIdentity().equals(profile.protocolCell())
                || !root.providerScopeId().equals(profile.scopeId())
                || !root.wrappedRunKey().wrappingKeyId().equals(wrappingKeyId)
                || !provider.exclusiveNamespacePrefix().equals(prefix(shard, root.shardRunEpoch()))) {
            throw new IllegalStateException("persisted native Object WAL Root differs from its admitted Cell");
        }
        if (!provider.capabilityReceiptSha256().equals(capabilityDigest)
                || !provider.adapterVersion().equals(profile.adapterVersion())
                || provider.accessProfile() != ObjectProviderAccessProfile.C1_SINGLE_PUT_SINGLE_RANGE_STRONG_LIST
                || provider.maxObjectBodyBytes() > profile.maximumObjectBytes()
                || provider.maxSinglePutBytes() > profile.maximumObjectBytes()
                || provider.maxSingleRangeReadBytes() > profile.maximumRangeBytes()) {
            throw new IllegalStateException("persisted native Object WAL Root exceeds its admitted Provider");
        }
        if (!"canonical-key-v1".equals(provider.canonicalizerVersion())
                || provider.maxListPageKeys() > profile.maximumListPageKeys()) {
            throw new IllegalStateException("persisted native Object WAL Root has another Provider key contract");
        }
    }

    private ObjectProviderRootConfiguration provider(int shard, long runEpoch) {
        return new ObjectProviderRootConfiguration(
                ObjectProviderAccessProfile.C1_SINGLE_PUT_SINGLE_RANGE_STRONG_LIST, profile.adapterVersion(),
                "canonical-key-v1", prefix(shard, runEpoch), ProviderProofMode.NONE, 0,
                caps.maxCanonicalBodyBytes(), caps.maxCanonicalBodyBytes(), profile.maximumRangeBytes(), 1,
                profile.maximumListPageKeys(), capabilityDigest);
    }

    private String prefix(int shard, long runEpoch) {
        return profile.exclusiveNamespacePrefix() + "/object-wal/" + shard + "/" + runEpoch;
    }
}
