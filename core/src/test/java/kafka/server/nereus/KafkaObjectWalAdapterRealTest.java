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
import kafka.server.nsip.KafkaBookKeeperOwnerRelocationRealTest;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.server.config.NereusKafkaConfigs;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.DeterministicTopicIdsV1;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaRawAssignedRecordBatchFactsV1;
import com.nereusstream.kafka.bookkeeper.broker.KafkaObjectNativeAppendV1;
import com.nereusstream.kafka.bookkeeper.broker.KafkaObjectNativeExtentV1;
import com.nereusstream.kafka.bookkeeper.broker.KafkaObjectNativeVerificationV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentCommitCoordinatorV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaObjectAuthorizedRecoveryV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaObjectAuthorizedOwnerOpenV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaObjectRecoveredProtocolClosureV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaProtocolCheckpointHeadCodecV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaProtocolCheckpointHeadStateV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.ObjectKafkaProtocolCheckpointStoreV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectAuthorizationV1;
import com.nereusstream.kafka.bookkeeper.object.recovery.KafkaNativePartitionOwnerAuthorityV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.metadata.oxia.v2.objectwal.OxiaCanonicalControlMetadataStore;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunLifecycleManager;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunReference;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.control.WalRunRuntime;
import com.nereusstream.storage.object.kms.RunKeyCacheIdentity;
import com.nereusstream.storage.object.materialization.M5CellProviderProfileV2;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ProviderObjectOutcome;
import com.nereusstream.storage.object.provider.RepeatableObjectBody;
import com.nereusstream.storage.object.recovery.OwnerOpenRecoveryCoordinator;
import com.nereusstream.storage.object.recovery.WalRunLineageRecovery;
import com.nereusstream.storage.object.s3.S3C1ObjectProviderTransport;
import com.nereusstream.storage.object.s3.S3M5PhysicalNamespaceAuthorityV2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.AsyncOxiaClient;
import software.amazon.awssdk.services.s3.S3Client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises real native frame bytes, Vault, Oxia and MinIO at the broker adapter boundary. */
public class KafkaObjectWalAdapterRealTest {
    private static final String WRAPPING_KEY = "vault-transit://transit/keys/nereus-kafka-wal";

    @Test
    @Timeout(120)
    public void publishesOnlyAfterTheExactGrantAndReopensTheExactPhysicalRoot(@TempDir Path work) throws Exception {
        var endpoint = URI.create(System.getProperty("nereus.nsip.s3Endpoint"));
        var oxia = System.getProperty("nereus.nsip.oxia");
        try (var client = KafkaObjectProviderAdmissionRealTest.minio(endpoint);
                 var control = OxiaClientBuilder.create(oxia).asyncClient().get(30, TimeUnit.SECONDS)) {
            var fixture = provision(work, client, control, endpoint, "zk://127.0.0.1:2181/ledgers", oxia);
            var props = new HashMap<>(fixture.settings());
            var namespace = fixture.namespace();
            var config = fixture.config();
            var profile = fixture.profile();
            var cell = NereusBookKeeperCapability.metadataPolicy(config).cellIdentity();
            props.put(NereusKafkaConfigs.OBJECT_WAL_VAULT_KEY_CONFIG, WRAPPING_KEY + "-foreign");
            assertThrows(IllegalArgumentException.class,
                    () -> NereusKafkaObjectProvider.profile(new KafkaConfig(props, false), cell));
            props.put(NereusKafkaConfigs.OBJECT_WAL_VAULT_KEY_CONFIG, WRAPPING_KEY);
            props.put(NereusKafkaConfigs.OBJECT_WAL_VAULT_ENDPOINT_CONFIG, "http://127.0.0.1:1");
            assertThrows(IllegalArgumentException.class,
                    () -> NereusKafkaObjectProvider.profile(new KafkaConfig(props, false), cell));
            var topic = new KafkaTopicIncarnationIdentity(new KafkaTopicId(new Id128(1, 2)),
                    new KafkaTopicName("object-wal-native"));
            var binding = DeterministicTopicIdsV1.deriveBindingId(cell, topic);
            var epoch = DeterministicTopicIdsV1.deriveStorageEpochId(binding, 0);
            var fence = new KafkaPartitionFenceV1(binding, topic, 0, 1, epoch, 1, 0);
            var exact = new Oxia09ExactMetadataTransactionStoreV1(control);
            var shard = NereusKafkaObjectWalShard.reserve(exact, profile.protocolCellSha256(), fence,
                    Duration.ofSeconds(10));
            assertEquals(shard, NereusKafkaObjectWalShard.reserve(exact, profile.protocolCellSha256(), fence,
                    Duration.ofSeconds(10)));
            var metadata = new OxiaCanonicalControlMetadataStore(control, shard.cellRoot(), shard.id());
            var roots = new NereusKafkaObjectWalRoot(config, profile);
            var authority = new KafkaObjectAuthorizationV1(metadata, shard.id(),
                    new KafkaObjectAuthorizationV1.Bounds(64, 16, 16L << 20, 32));
            CanonicalBytes sourceVersion;
            try (var provider = NereusKafkaObjectProvider.open(config, cell, namespace);
                 var kms = NereusKafkaObjectWalKms.open(config, profile)) {
                var runs = new NereusKafkaObjectWalRuns(shard, metadata, exact, provider, kms, roots,
                        Duration.ofSeconds(10));
                sourceVersion = verifyWrite(runs, metadata, authority, fence, provider);
                var previous = runs.current().orElseThrow().root().predecessor().orElseThrow().root();
                var oldKey = new RunKeyCacheIdentity(previous.shardId(), previous.shardRunEpoch());
                assertThrows(IllegalStateException.class, () -> kms.session().beginNewRunKey(oldKey));
                verifyColdRead(runs, metadata, authority, fence, sourceVersion, provider);
                assertThrows(IllegalStateException.class, () -> kms.session().beginNewRunKey(oldKey));
                var otherFence = new KafkaPartitionFenceV1(binding, topic, 1, 1, epoch, 1, 0);
                KafkaObjectRevokedAppendRealScenario.verify(otherFence, fixture, control, exact, provider, kms);
                provider.beginDrain();
            }
            try (var provider = NereusKafkaObjectProvider.open(config, cell, namespace);
                 var kms = NereusKafkaObjectWalKms.open(config, profile)) {
                var runs = new NereusKafkaObjectWalRuns(shard, metadata, exact, provider, kms, roots,
                        Duration.ofSeconds(10));
                verifyColdRead(runs, metadata, authority, fence, sourceVersion, provider);
                assertEquals(1, kms.session().unwrapCalls());
                verifyArmedLatePut(runs, exact, shard);
                verifyRecoveredPhysicalSeal(runs, metadata, authority, fence, kms);
                provider.beginDrain();
            }
        }
    }

    record Provisioned(Map<String, Object> settings, KafkaConfig config, M5CellProviderProfileV2 profile,
                       OxiaPhysicalMetadataNamespaceV2 namespace) { }

    record NativeTerminal(OxiaCanonicalControlMetadataStore metadata, WalRunReference ref, WalRunRootRecord root,
                          long writerEpoch, int kafkaLeaderEpoch) {
        static NativeTerminal capture(Provisioned fixture, AsyncOxiaClient control, Uuid topicId,
                String name, int brokerId) throws Exception {
            var topic = new KafkaTopicIncarnationIdentity(new KafkaTopicId(
                    new Id128(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits())), new KafkaTopicName(name));
            var binding = DeterministicTopicIdsV1.deriveBindingId(fixture.profile().protocolCell(), topic);
            var epoch = DeterministicTopicIdsV1.deriveStorageEpochId(binding, 0);
            var scope = new KafkaPartitionFenceV1(binding, topic, 0, 1, epoch, 1, 0);
            var exact = new Oxia09ExactMetadataTransactionStoreV1(control);
            var timeout = Duration.ofSeconds(10);
            var shard = NereusKafkaObjectWalShard.reserve(exact, fixture.profile().protocolCellSha256(), scope, timeout);
            var metadata = new OxiaCanonicalControlMetadataStore(control, shard.cellRoot(), shard.id());
            var pointer = WalRunControlCodec.decodePointer(metadata.get(WalRunControlKeys.pointerKey(shard.id())).orElseThrow());
            var ref = pointer.current();
            var root = WalRunControlCodec.decodeRoot(metadata.get(ref.rootKey()).orElseThrow());
            var owner = new NereusKafkaObjectWalOwners(shard, exact, timeout).creator(root);
            assertEquals(brokerId, owner.brokerId());
            return new NativeTerminal(metadata, ref, root, owner.ownerEpoch(), owner.kafkaLeaderEpoch());
        }

        String checkpointPrefix() {
            return KafkaObjectNativeAppendV1.checkpointPrefix(root);
        }

        String checkpointHeadKey() {
            return WalRunControlKeys.kafkaProtocolCheckpointHeadKey(ref.shardId(), ref.shardRunEpoch());
        }

        Optional<String> physicalPageKey() {
            return metadata.get(physicalCheckpointHeadKey())
                    .flatMap(value -> WalRunControlCodec.decodeCheckpointHead(value).pageKey());
        }

        String physicalCheckpointHeadKey() {
            return WalRunControlKeys.checkpointHeadKey(ref.shardId(), ref.shardRunEpoch());
        }

        String requireSealedByOriginalWriter(S3Client minio, String bucket, long end) {
            var key = checkpointHeadKey();
            var head = KafkaProtocolCheckpointHeadCodecV1.decode(checkpointPrefix(),
                    ref.rootSha256(), metadata.get(key).orElseThrow());
            assertEquals(KafkaProtocolCheckpointHeadStateV1.TERMINAL, head.state());
            assertEquals(writerEpoch, head.publisherEpoch());
            assertEquals(List.of(end), head.coveredThroughVector().stream().map(row -> row.rangeIndexCoveredThrough()).toList());
            assertEquals(head.checkpointObjectLength(),
                    minio.headObject(request -> request.bucket(bucket).key(head.checkpointObjectKey())).contentLength());
            return head.checkpointObjectKey();
        }

        RemovedProtocolObject removeSelectedVersion(S3Client minio, String bucket, long end) {
            String key = requireSealedByOriginalWriter(minio, bucket, end);
            var selected = minio.getObjectAsBytes(request -> request.bucket(bucket).key(key));
            String version = selected.response().versionId();
            assertTrue(version != null && !version.isBlank());
            minio.deleteObject(request -> request.bucket(bucket).key(key).versionId(version));
            var remaining = minio.listObjectVersions(request -> request.bucket(bucket).prefix(key));
            assertFalse(remaining.isTruncated());
            assertTrue(remaining.versions().stream().noneMatch(candidate -> candidate.key().equals(key)));
            return new RemovedProtocolObject(key, version, CanonicalBytes.copyOf(selected.asByteArray()));
        }
    }

    record RemovedProtocolObject(String key, String version, CanonicalBytes body) {
        void requireExactRecreation(S3Client minio, String bucket) {
            var current = minio.getObjectAsBytes(request -> request.bucket(bucket).key(key));
            assertTrue(current.response().versionId() != null && !version.equals(current.response().versionId()));
            assertEquals(body, CanonicalBytes.copyOf(current.asByteArray()));
        }
    }

    static Provisioned provision(Path work, S3Client client, AsyncOxiaClient control, URI endpoint,
            String bkUri, String oxia) throws Exception {
        var namespace = OxiaPhysicalMetadataNamespaceV2.provision(control).toCompletableFuture().get(30, TimeUnit.SECONDS);
        String bucket = KafkaObjectProviderAdmissionRealTest.newVersionedBucket(client);
        var physical = S3M5PhysicalNamespaceAuthorityV2.provision(client, bucket, namespace.identity());
        var props = new HashMap<>(KafkaBookKeeperOwnerRelocationRealTest.nativeConfig(work, bkUri, oxia));
        props.put("node.id", "0");
        props.put("process.roles", "broker");
        props.put("controller.quorum.voters", "1@127.0.0.1:9193");
        props.put("controller.listener.names", "CONTROLLER");
        props.put("log.dirs", work.resolve("logs").toString());
        props.put("message.max.bytes", "4096");
        KafkaBookKeeperOwnerRelocationRealTest.writeCapability(work, Optional.empty());
        KafkaObjectProviderAdmissionRealTest.configureObject(props, work, endpoint, bucket);
        props.put(NereusKafkaConfigs.PROFILE_CONFIG, "OBJECT_WAL_SYNC_OBJECT");
        props.put(NereusKafkaConfigs.OBJECT_WAL_VAULT_KEY_CONFIG, WRAPPING_KEY);
        props.put(NereusKafkaConfigs.OBJECT_WAL_VAULT_ENDPOINT_CONFIG, System.getProperty("nereus.nsip.vaultEndpoint"));
        props.put(NereusKafkaConfigs.OBJECT_WAL_VAULT_TOKEN_FILE_CONFIG, System.getProperty("nereus.nsip.vaultTokenFile"));
        var config = new KafkaConfig(props, false);
        var cell = NereusBookKeeperCapability.metadataPolicy(config).cellIdentity();
        var profile = new M5CellProviderProfileV2(
                namespace.bind(physical).toCompletableFuture().get(30, TimeUnit.SECONDS), cell,
                KafkaObjectProviderAdmissionRealTest.PREFIX,
                new M5CellProviderProfileV2.References("local-s3",
                        KafkaObjectProviderAdmissionRealTest.CREDENTIAL_SCOPE, "NONE",
                        "kafka-cell-quota", "kafka-owner", "version-match-delete"),
                S3C1ObjectProviderTransport.ADAPTER_VERSION, NereusKafkaObjectProvider.configurationSha256(config),
                1 << 20, 1 << 20, 100, 1024);
        Files.write(work.resolve("object-capability"), profile.encode().toByteArray());
        namespace.openCellProviderScopes(physical).thenCompose(registry -> registry.prepare(profile).admit())
                .toCompletableFuture().get(30, TimeUnit.SECONDS);
        namespace.openAuthorityRoute(physical).thenCompose(quota -> quota.initialize(8L << 20))
                .toCompletableFuture().get(30, TimeUnit.SECONDS);
        return new Provisioned(Map.copyOf(props), config, profile, namespace);
    }

    private static CanonicalBytes verifyWrite(NereusKafkaObjectWalRuns runs, OxiaCanonicalControlMetadataStore metadata,
            KafkaObjectAuthorizationV1 authority, KafkaPartitionFenceV1 fence, NereusKafkaObjectProvider provider)
            throws Exception {
        var nativeOwner = election(fence);
        runs.admitNativeOwner(nativeOwner);
        var selected = runs.selectOrCreateGenesis(System.currentTimeMillis(), false, nativeOwner);
        assertEquals(nativeOwner, runs.creator(selected.root()));
        NereusKafkaObjectWalRuns.Selected successor;
        CanonicalBytes sourceVersion;
        try (var session = selected.freshSession().orElseThrow()) {
            var coordinator = KafkaCoherentCommitCoordinatorV1.bootstrapObject(fence, 0, ignored -> { });
            authority.open(fence, 0, Optional.empty());
            var writer = KafkaObjectNativeAppendV1.open(session, metadata, authority, coordinator,
                    Optional.empty(), () -> true);
            var canonical = nativeBytes();
            var assigned = KafkaNativeAssignedRecordBatchV1.validate(KafkaRawAssignedRecordBatchFactsV1.parse(canonical));
            try (var reservation = writer.admit(List.of(canonical.length()))) {
                var published = writer.append(reservation, List.of(assigned), System.currentTimeMillis(), publish -> {
                    assertEquals(1, authority.read(fence).orElseThrow().endOffset());
                    assertEquals(0, coordinator.captureObject().root().frontiers().durableEndOffset());
                    return publish.get();
                }, state -> {
                    assertEquals(1, state.root().frontiers().durableEndOffset());
                    var head = authority.read(fence).orElseThrow();
                    assertFalse(head.closed());
                    assertEquals(1, head.endOffset());
                    assertEquals(state.activeTail().locators(), head.grants().stream().map(grant -> grant.locator()).toList());
                });
                assertEquals(1, published.root().frontiers().highWatermark());
            }
            var grant = authority.read(fence).orElseThrow().grants().get(0);
            KafkaObjectNativeExtentV1.read(session,
                    KafkaObjectNativeVerificationV1.context(selected.root(), fence), grant.locator(),
                    (frame, payload) -> {
                        assertEquals(1, frame.coverage1());
                        byte[] actual = new byte[payload.remaining()];
                        payload.get(actual);
                        assertEquals(canonical, CanonicalBytes.copyOf(actual));
                    });
            var verification = KafkaObjectNativeVerificationV1.context(selected.root(), fence);
            var original = provider.readOriginal(grant.object(), Optional.empty());
            assertThrows(IllegalStateException.class, () ->
                    KafkaObjectNativeExtentV1.capture(session, verification, grant, original.value(), (frame, payload) -> {
                        throw new AssertionError("open Object WAL source exposed a materialization frame");
                    }));
            var sealed = writer.sealDrained();
            assertEquals(selected.reference(), sealed.predecessor().root());
            assertTrue(sealed.predecessor().terminalProtocolCheckpoint().isPresent());
            assertTrue(sealed.history().head().closed());
            assertEquals(1, authority.checkpoint(sealed.history()).orElseThrow()
                    .vector().recoveryCoveredThrough());
            byte[] altered = original.value().toByteArray();
            altered[altered.length - 1] ^= 1;
            assertThrows(IOException.class, () -> KafkaObjectNativeExtentV1.capture(session, verification, grant,
                    CanonicalBytes.copyOf(altered), (frame, payload) -> {
                        throw new AssertionError("altered Object WAL source exposed a materialization frame");
                    }));
            KafkaObjectNativeExtentV1.capture(session, verification, grant, original.value(),
                    (frame, payload) -> assertEquals(canonical, copyFrame(payload)));
            sourceVersion = original.immutableVersionToken().orElseThrow();
            successor = verifySuccessor(runs, metadata, authority, fence, session, sealed);
        }
        var discovered = runs.current().orElseThrow();
        assertEquals(successor.reference(), discovered.reference());
        assertEquals(successor.root(), discovered.root());
        assertTrue(discovered.freshSession().isEmpty());
        return sourceVersion;
    }

    private static void verifyColdRead(NereusKafkaObjectWalRuns runs, OxiaCanonicalControlMetadataStore metadata,
            KafkaObjectAuthorizationV1 authority, KafkaPartitionFenceV1 fence, CanonicalBytes sourceVersion,
            NereusKafkaObjectProvider provider) throws Exception {
        var successor = runs.current().orElseThrow();
        var previous = successor.root().predecessor().orElseThrow().root();
        var root = runs.read(previous).root();
        var verifier = ObjectKafkaProtocolCheckpointStoreV1.terminalVerifier(
                root, KafkaObjectNativeAppendV1.checkpointPrefix(root), KafkaObjectNativeAppendV1.runBinding(root, fence));
        var lineage = new WalRunLineageRecovery(metadata, verifier, runs::terminalReader)
                .recover(WalRunControlKeys.pointerKey(root.shardId()), root.protocolCellIdentity(),
                        root.providerScopeId(), Optional.empty(), System::nanoTime);
        assertEquals(successor.reference(), lineage.runs().get(0).reference());
        var grant = authority.readClosed(fence).head().grants().get(0);
        try (var historical = runs.openHistorical(lineage, previous)) {
            KafkaObjectNativeExtentV1.read(historical,
                    KafkaObjectNativeVerificationV1.context(root, fence), grant.locator(), (frame, payload) -> {
                        assertEquals(1, frame.coverage1());
                        byte[] actual = new byte[payload.remaining()];
                        payload.get(actual);
                        assertEquals(nativeBytes(), CanonicalBytes.copyOf(actual));
                    });
            var original = provider.readVersion(grant.object(), sourceVersion);
            KafkaObjectNativeExtentV1.capture(historical,
                    KafkaObjectNativeVerificationV1.context(root, fence), grant, original.value(),
                    (frame, payload) -> assertEquals(nativeBytes(), copyFrame(payload)));
            assertEquals(Optional.of(sourceVersion), original.immutableVersionToken());
        }
    }

    private static CanonicalBytes copyFrame(ByteBuffer payload) {
        byte[] actual = new byte[payload.remaining()];
        payload.get(actual);
        return CanonicalBytes.copyOf(actual);
    }

    static CanonicalBytes nativeBytes() {
        return nativeBytes(0, 0);
    }

    private static CanonicalBytes nativeBytes(long offset, int epoch) {
        var records = MemoryRecords.withRecords(offset, Compression.NONE, epoch,
                new SimpleRecord(10L, new byte[] {1, 3, 5}));
        var batch = records.batches().iterator().next();
        var bytes = ByteBuffer.allocate(batch.sizeInBytes());
        batch.writeTo(bytes);
        return CanonicalBytes.copyOf(bytes.array());
    }

    private static void verifyRecoveredPhysicalSeal(NereusKafkaObjectWalRuns runs,
            OxiaCanonicalControlMetadataStore metadata, KafkaObjectAuthorizationV1 authority,
            KafkaPartitionFenceV1 genesisFence, NereusKafkaObjectWalKms kms) throws Exception {
        var selected = runs.current().orElseThrow();
        var root = selected.root();
        String headKey = WalRunControlKeys.checkpointHeadKey(root.shardId(), root.shardRunEpoch());
        assertEquals(-1, WalRunControlCodec.decodeCheckpointHead(metadata.get(headKey).orElseThrow()).pageOrdinal());
        var predecessor = runs.read(root.predecessor().orElseThrow().root()).root();
        var oldRun = KafkaObjectNativeAppendV1.runBinding(predecessor, genesisFence);
        var verifier = ObjectKafkaProtocolCheckpointStoreV1.terminalVerifier(
                predecessor, KafkaObjectNativeAppendV1.checkpointPrefix(predecessor), oldRun);
        var lineage = new WalRunLineageRecovery(metadata, verifier, runs::terminalReader)
                .recover(WalRunControlKeys.pointerKey(root.shardId()), root.protocolCellIdentity(),
                        root.providerScopeId(), Optional.empty(), System::nanoTime);
        var origin = authority.read(genesisFence).orElseThrow().fence();
        var closed = authority.close(origin);
        assertEquals(2, closed.head().endOffset());
        var current = new KafkaPartitionFenceV1(origin.bindingId(), origin.topicIncarnation(), origin.partitionId(),
                origin.bindingGeneration(), origin.storageEpochId(), 3, 2);
        runs.admitNativeOwner(election(current));
        assertThrows(IllegalStateException.class, () -> runs.admitNativeOwner(election(origin)));
        var verification = KafkaObjectNativeVerificationV1.context(root, origin);
        var ownerFence = KafkaNativePartitionOwnerAuthorityV1.protocolOwnerFenceExecutor(
                (actual, callback) -> {
                    assertEquals(current, actual);
                    return callback.execute();
                }, current, KafkaObjectNativeAppendV1.runBinding(root, origin), root.protocolCellIdentity(),
                selected.reference(), verification);
        var protocol = new KafkaObjectAuthorizedOwnerOpenV1(root, authority, closed);
        try (var restored = OwnerOpenRecoveryCoordinator.recoverUnderDurableFence(
                ownerFence, root.protocolCellIdentity(), selected.reference(), root, metadata, runs.openProvider(root),
                kms.session(), lineage, protocol)) {
            assertEquals(WalRunRuntime.State.STOPPING, restored.runtimeState());
            assertEquals(2, restored.runtimeRecoveryState().resolvedExtentCount());
            assertEquals(2, protocol.result().state().vector().recoveryCoveredThrough());
            assertEquals(closed.head().grants().stream().map(grant -> grant.locator()).toList(),
                    protocol.result().tail().activeTail().locators());
            var head = restored.checkpointRecoveredPhysicalRows(metadata, current.ownerEpoch());
            assertEquals(0, head.pageOrdinal());
            var proof = KafkaObjectRecoveredProtocolClosureV1.seal(
                    restored, metadata, head, protocol.result(), current.ownerEpoch());
            assertEquals(selected.reference(), proof.root());
            assertTrue(proof.terminalProtocolCheckpoint().isPresent());
        }
    }

    private static void verifyArmedLatePut(NereusKafkaObjectWalRuns runs, Oxia09ExactMetadataTransactionStoreV1 metadata,
            NereusKafkaObjectWalShard.Assignment shard) throws Exception {
        var root = runs.current().orElseThrow().root();
        var original = new NereusKafkaObjectWalDispatchFence(metadata, shard, root, Duration.ofSeconds(10));
        var replacement = new NereusKafkaObjectWalDispatchFence(metadata, shard, root, Duration.ofSeconds(10));
        var body = CanonicalBytes.copyOf(new byte[] {2, 4, 6});
        var identity = new ObjectIdentity(root.providerConfiguration().exclusiveNamespacePrefix() + "/held-create",
                body.length(), Sha256Digest.hash(body));
        original.arm(identity);
        assertEquals(Optional.of(identity), replacement.closeAdmission());
        assertThrows(IOException.class, () -> runs.requireDrained(root));
        assertThrows(IOException.class, () -> original.arm(identity));

        // Release the original armed operation only after the new owner actually observed a missing body.
        try (var provider = runs.openProvider(root)) {
            var persisted = provider.conditionalCreate(new RepeatableObjectBody() {
                public ObjectIdentity identity() {
                    return identity;
                }

                public InputStream openStream() {
                    return new ByteArrayInputStream(body.toByteArray());
                }
            });
            assertEquals(ProviderObjectOutcome.APPLIED_EXACT, persisted.outcome());
        }
        runs.requireDrained(root);
        assertEquals(Optional.empty(), replacement.closeAdmission());
        assertThrows(IOException.class, original::initializeFresh);
    }

    private static NereusKafkaObjectWalRuns.Selected verifySuccessor(
            NereusKafkaObjectWalRuns runs, OxiaCanonicalControlMetadataStore metadata,
            KafkaObjectAuthorizationV1 authority, KafkaPartitionFenceV1 oldFence, WalRunObjectSession predecessor,
            KafkaObjectNativeAppendV1.ClosedRun sealed) throws Exception {
        var oldRoot = predecessor.rootRecord();
        var oldRun = KafkaObjectNativeAppendV1.runBinding(oldRoot, oldFence);
        var terminalVerifier = ObjectKafkaProtocolCheckpointStoreV1.terminalVerifier(
                oldRoot, KafkaObjectNativeAppendV1.checkpointPrefix(oldRoot), oldRun);
        var lifecycle = new WalRunLifecycleManager(metadata, terminalVerifier, runs::terminalReader);
        var next = new KafkaPartitionFenceV1(oldFence.bindingId(), oldFence.topicIncarnation(),
                oldFence.partitionId(), oldFence.bindingGeneration(), oldFence.storageEpochId(), 2, 1);
        var nativeOwner = election(next);
        runs.admitNativeOwner(nativeOwner);
        var successor = runs.advance(sealed.predecessor(), lifecycle, System.currentTimeMillis(), nativeOwner);
        try (var newRun = successor.freshSession().orElseThrow()) {
            assertEquals(2, newRun.rootRecord().shardRunEpoch());
            assertEquals(nativeOwner, runs.creator(newRun.rootRecord()));
            var recovered = KafkaObjectAuthorizedRecoveryV1.recover(authority, sealed.history(), oldRun,
                    predecessor.rootSha256(), ignored -> {
                        throw new AssertionError("complete terminal checkpoint requires no authorized tail replay");
                    });
            var coordinator = KafkaCoherentCommitCoordinatorV1.bootstrapObjectRecovered(next, 0, 1, 1,
                    recovered.state(), recovered.tail(), ignored -> { });
            authority.open(next, 1, Optional.of(sealed.history()));
            var withhold = new AtomicBoolean();
            var uncertain = new KafkaObjectAuthorizationV1(withholdGrant(metadata, authority.headKey(next), withhold),
                    newRun.rootRecord().shardId(), new KafkaObjectAuthorizationV1.Bounds(64, 16, 16L << 20, 32));
            var writer = KafkaObjectNativeAppendV1.open(newRun, metadata, uncertain, coordinator,
                    Optional.of(recovered.tail()), () -> true);
            var bytes = nativeBytes(1, 1);
            var assigned = KafkaNativeAssignedRecordBatchV1.validate(KafkaRawAssignedRecordBatchFactsV1.parse(bytes));
            try (var reserved = writer.admit(List.of(bytes.length()))) {
                writer.append(reserved, List.of(assigned), System.currentTimeMillis(),
                        snapshot -> assertEquals(2, snapshot.root().frontiers().durableEndOffset()));
            }
            var orphan = nativeBytes(2, 1);
            var ungranted = KafkaNativeAssignedRecordBatchV1.validate(KafkaRawAssignedRecordBatchFactsV1.parse(orphan));
            withhold.set(true);
            try (var reserved = writer.admit(List.of(orphan.length()))) {
                assertThrows(KafkaObjectAuthorizationV1.Unknown.class, () ->
                        writer.append(reserved, List.of(ungranted), System.currentTimeMillis(), snapshot -> {
                            throw new AssertionError("an unselected grant reached the native callback");
                        }));
            }
            assertFalse(withhold.get());
            assertEquals(2, coordinator.captureObject().root().frontiers().durableEndOffset());
            assertEquals(2, authority.read(next).orElseThrow().endOffset());
            var adopted = runs.advance(sealed.predecessor(), lifecycle, System.currentTimeMillis(), nativeOwner);
            assertEquals(successor.reference(), adopted.reference());
            assertTrue(adopted.freshSession().isEmpty());
        }
        return successor;
    }

    private static CanonicalControlMetadataStore withholdGrant(CanonicalControlMetadataStore metadata, String headKey,
            AtomicBoolean withhold) {
        return new CanonicalControlMetadataStore() {
            public Optional<CanonicalBytes> get(String key) {
                return metadata.get(key);
            }

            public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes value) {
                return metadata.putIfAbsent(key, value);
            }

            public ControlMutationOutcome compareAndSet(String key, Optional<CanonicalBytes> expected,
                    CanonicalBytes candidate) {
                if (key.equals(headKey) && withhold.compareAndSet(true, false)) {
                    return ControlMutationOutcome.RESPONSE_UNKNOWN;
                }
                return metadata.compareAndSet(key, expected, candidate);
            }
        };
    }

    static KafkaOwnerIdentityV1 election(KafkaPartitionFenceV1 fence) {
        return new KafkaOwnerIdentityV1(fence.ownerEpoch(), fence.kafkaLeaderEpoch(),
                fence.kafkaLeaderEpoch() % 2, fence.ownerEpoch(), fence.ownerEpoch());
    }
}
