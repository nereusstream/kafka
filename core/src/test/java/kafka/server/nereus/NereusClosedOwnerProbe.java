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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.TopicBindingAggregateRecord;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperRunBirthRecoveryV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperNativeRootVerifierV2;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerOpenOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.api.kafka.KafkaBookKeeperOwnerAuthorityV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateSpecV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;

/** Independent native reread of the exact old owner archive used by the real Kafka handoff test. */
public final class NereusClosedOwnerProbe implements AutoCloseable {
    private final AsyncOxiaClient oxia;
    private final M5BookKeeperNamespaceAuthorityV2 backend;
    private final M5BookKeeperNativeCreateClientV2 source;
    private final KafkaBookKeeperOwnerAuthorityV1 roots;
    private final PhysicalNamespaceAuthorityBindingV2 binding;
    private final OxiaQuotaTargetDeleteStoreV2 route;

    public NereusClosedOwnerProbe(
            KafkaConfig config,
            Uuid topicId,
            TopicPartition partition,
            TopicBindingAggregateRecord aggregate,
            long ownerEpoch) throws Exception {
        var capability = NereusBookKeeperCapability.loadAndValidate(config);
        var core = config.nereusKafkaStorageConfig().core();
        String uri = core.bookKeeperMetadataServiceUri().orElseThrow().toString();
        oxia = await(OxiaClientBuilder.create(core.oxiaServiceAddress().orElseThrow())
                .namespace(core.oxiaNamespace())
                .requestTimeout(config.nereusKafkaStorageConfig().lifecycle().recoveryTimeout())
                .asyncClient());
        backend = M5BookKeeperNamespaceAuthorityV2.connect(uri, capability);
        var namespace = await(OxiaPhysicalMetadataNamespaceV2.connect(
                oxia, await(backend.readBinding()).orElseThrow().metadataNamespace()));
        binding = await(namespace.bind(backend));
        route = await(namespace.openAuthorityRoute(backend, new Oxia09ExactMetadataTransactionStoreV1(oxia)));
        var scope = new KafkaRunRootRecordV2.Scope(
                new TopicBindingId(Sha256Digest.copyOf(aggregate.bindingId())),
                new KafkaTopicIncarnationIdentity(
                        new KafkaTopicId(new Id128(
                                topicId.getMostSignificantBits(),
                                topicId.getLeastSignificantBits())),
                        new KafkaTopicName(partition.topic())),
                partition.partition(),
                new StorageEpochId(Sha256Digest.copyOf(aggregate.storageEpochId())),
                capability.providerScopeId());
        var runId = new StorageRunId(Id128.fromBytes(Arrays.copyOf(
                digest("native-owner-run/" + Sha256Digest.hash(scope.encode()).toHex() + "/" + ownerEpoch)
                        .bytes().toByteArray(), 16)));
        var spec = M5BookKeeperNativeCreateSpecV2.of(
                M5BookKeeperNativeCreateClientV2.discoverInstanceId(uri, capability),
                digest("native-bk-run-scope/" + runId.value().toHex()),
                List.of(RunLedgerConfigurationV1.from(capability, runId)));
        source = M5BookKeeperNativeCreateClientV2.connect(uri, capability, spec, binding);
        roots = await(namespace.openKafkaRunRoots(
                backend, new Oxia09ExactMetadataTransactionStoreV1(oxia), scope,
                new KafkaBookKeeperNativeRootVerifierV2(source)));
    }

    public boolean archived(long ownerEpoch) throws Exception {
        return await(roots.readClosedOwner(ownerEpoch)).isPresent();
    }

    public KafkaOwnerAdmissionV1 owner() throws Exception {
        return await(roots.readOwnerAdmission()).orElseThrow();
    }

    public long activeCreateTickets(RunLedgerHandleV1 handle) throws Exception {
        return activeTicketOperations(handle, ProofBoundWriterClassV1.TASK_PROJECTION_MIGRATION_RECOVERY_V1).size();
    }

    public long activeReaderTickets(RunLedgerHandleV1 handle) throws Exception {
        return activeReaderOperations(handle).size();
    }

    public List<Sha256Digest> activeReaderOperations(RunLedgerHandleV1 handle) throws Exception {
        return activeTicketOperations(handle, ProofBoundWriterClassV1.OWNER_WORKER_LEASE_HANDLE_PIN_V1);
    }

    public boolean admittedSource(RunLedgerHandleV1 handle, long creatorOwnerEpoch) throws Exception {
        var archived = await(roots.readClosedOwner(creatorOwnerEpoch)).orElseThrow();
        var link = archived.runs().stream().filter(value -> value.runId().equals(handle.runId())).findFirst().orElseThrow();
        var record = await(roots.readAdmittedRun(link)).orElseThrow();
        return !record.retired() && record.resource().namespace().equals(binding.physicalNamespace())
                && record.root().runId().equals(handle.runId())
                && record.root().ledgerIdentity().equals(handle.ledgerIdentity());
    }

    private List<Sha256Digest> activeTicketOperations(RunLedgerHandleV1 handle, ProofBoundWriterClassV1 role) throws Exception {
        var resource = new PhysicalResourceIdV2.BookKeeperLedger(
                binding.physicalNamespace(), handle.ledgerIdentity().ledgerId());
        var exact = await(route.read(resource.authorityKey())).orElseThrow();
        var authority = M5TargetDeleteAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
        if (!authority.target().resourceId().equals(resource)) {
            throw new IllegalStateException("writer authority has another physical resource");
        }
        return authority.activeWriterTickets().stream()
                .filter(ticket -> ticket.writerClass() == role)
                .map(ticket -> ticket.operationIdSha256())
                .toList();
    }

    public boolean retainedOpenQuota(RunLedgerHandleV1 handle) throws Exception {
        var resource = new PhysicalResourceIdV2.BookKeeperLedger(
                binding.physicalNamespace(), handle.ledgerIdentity().ledgerId());
        var exact = await(route.read(resource.authorityKey())).orElseThrow();
        var authority = M5TargetDeleteAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
        var reservation = await(route.quota().requireExistingReservation(resource));
        return authority.target().resourceId().equals(resource)
                && authority.state() == TargetDeleteAuthorityStateV1.OPEN_V1
                && !reservation.settled();
    }

    public record Birth(KafkaBookKeeperRunBirthRecoveryV1.Kind kind, RunLedgerHandleV1 handle) { }

    public List<Birth> closedBirths(long ownerEpoch) throws Exception {
        var closed = await(roots.readClosedOwner(ownerEpoch)).orElseThrow();
        var session = source.newSession();
        try {
            var result = new java.util.ArrayList<Birth>();
            for (var record : await(roots.readRunBirths(closed)).orElseThrow()) {
                var root = record.root();
                var handle = new RunLedgerHandleV1(
                        root.providerScopeId(), root.runId(), root.ledgerIdentity(),
                        session.capabilitySnapshot().configurationDigest());
                if (closed.runs().contains(record.initialLink())) {
                    result.add(new Birth(KafkaBookKeeperRunBirthRecoveryV1.Kind.SELECTED, handle));
                    continue;
                }
                var open = await(session.openRunLedger(handle));
                if (open.outcome() == RunLedgerOpenOutcomeV1.ABSENT) {
                    result.add(new Birth(KafkaBookKeeperRunBirthRecoveryV1.Kind.NEVER_CREATED, handle));
                } else if (open.outcome() == RunLedgerOpenOutcomeV1.OPENED_EXACT
                        && open.exactHandle().equals(java.util.Optional.of(handle))) {
                    result.add(new Birth(KafkaBookKeeperRunBirthRecoveryV1.Kind.PHYSICALLY_CREATED_UNSELECTED, handle));
                } else {
                    throw new IllegalStateException("closed birth has unresolved physical identity: " + open.outcome());
                }
            }
            return List.copyOf(result);
        } finally {
            await(session.closeAsync());
        }
    }

    @Override
    public void close() throws Exception {
        try {
            source.close();
        } finally {
            try {
                backend.close();
            } finally {
                oxia.close();
            }
        }
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static <T> T await(CompletionStage<T> result) throws Exception {
        return result.toCompletableFuture().get(30, TimeUnit.SECONDS);
    }
}
