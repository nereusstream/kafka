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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionAdmissionV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Caps;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectAuthorizationV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaObjectMaterializedPrefixV1.UnresolvedRead;
import com.nereusstream.metadata.oxia.v2.retention.OxiaKafkaBookKeeperM4ReadRouteV2;
import com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCoordinatorV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.retention.M5MaterializationWorkerCoordinatorV1.Proposal;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/** Stages and publishes the first sealed native NWG1 key domain through the Kafka M5 worker. */
final class NereusKafkaObjectWalCompaction {
    private final BrokerStorageRuntimeContext context;
    private final TopicIdPartition identity;
    private final NereusKafkaObjectWalPartition partition;
    private final KafkaObjectAuthorizationV1 authorization;
    private final OxiaKafkaBookKeeperM4ReadRouteV2 route;
    private final NereusKafkaObjectProvider provider;
    private final Runnable requireOwner;
    private final long ownerEpoch;
    private final long storageFence;
    private final Sha256Digest workerFence;
    private final Sha256Digest namespace;
    private final Sha256Digest cell;
    private final Sha256Digest rawView;
    private final KafkaCompactionAdmissionV1 inputAdmission;
    private final M5MaterializationAdmissionV1 outputAdmission;
    private final Duration timeout;
    private Job job;
    private volatile boolean settled;

    NereusKafkaObjectWalCompaction(BrokerStorageRuntimeContext context, TopicIdPartition identity,
            NereusKafkaObjectWalPartition partition, KafkaObjectAuthorizationV1 authorization,
            OxiaKafkaBookKeeperM4ReadRouteV2 route, NereusKafkaObjectProvider provider, Runnable requireOwner,
            long ownerEpoch, long storageFence, Sha256Digest workerFence, Sha256Digest rawView) {
        this.context = context;
        this.identity = identity;
        this.partition = partition;
        this.authorization = authorization;
        this.route = route;
        this.provider = provider;
        this.requireOwner = requireOwner;
        this.ownerEpoch = ownerEpoch;
        this.storageFence = storageFence;
        this.workerFence = workerFence;
        this.rawView = rawView;
        var profile = provider.profile();
        namespace = Sha256Digest.hash(profile.physicalBinding().encode());
        cell = profile.protocolCellSha256();
        inputAdmission = new KafkaCompactionAdmissionV1(route.compactionAdmissionMetadata(cell), cell);
        outputAdmission = new M5MaterializationAdmissionV1(route.materializationMetadata(cell), cell);
        timeout = context.config().nereusKafkaStorageConfig().lifecycle().recoveryTimeout();
    }

    void recover(BindingReadSelector selector) throws Exception {
        if (selector.selectedViewSha256().equals(rawView)) {
            if (selector.mode() != SelectorMode.PREFERRED_ONLY) {
                throw new IllegalStateException("native Object WAL raw selection has an unexpected protection set");
            }
            return;
        }
        var selected = NereusKafkaObjectRecovery.recover(route, identity.partition(), selector, provider, timeout)
                .orElseThrow(() -> new IllegalStateException("native Object WAL exact selected manifest is missing"));
        if (selected.plan().sourceCut().sources().stream().anyMatch(source -> source.kind() != SourceKind.OBJECT_WAL_NWG1)) {
            throw new IllegalStateException("native Object WAL selection belongs to another source profile");
        }
        selected.prefix().verifyComplete();
        requireOwner.run();
        partition.installSelectedPrefix(selected.prefix());
        NereusKafkaObjectPublication.reconcileNativeWal(selected, selector, provider, route, identity.partition(), timeout);
        releaseInputs(selected.plan().taskIdSha256());
        settled = true;
    }

    boolean settled() {
        return settled;
    }

    CompletionStage<Void> advance(Executor executor) throws Exception {
        requireOwner.run();
        var selector = route.readSelector().orElseThrow();
        if (selector.ownerEpoch() != ownerEpoch) throw new FencedLeaderEpochException("native Object WAL read owner changed");
        if (!selector.selectedViewSha256().equals(rawView)) {
            recover(selector);
            return CompletableFuture.completedFuture(null);
        }
        if (job == null) job = prepare(selector).orElse(null);
        if (job == null) return CompletableFuture.completedFuture(null);
        return job.advance(executor);
    }

    private Optional<Job> prepare(BindingReadSelector selector) throws Exception {
        var policy = NereusKafkaM5PolicyInputV1.captureObjectWalPolicy(context, identity);
        var inputs = inputAdmission.read();
        var outputs = outputAdmission.read();
        if (!policy.config().compact || inputs.isEmpty() || outputs.isEmpty()) return Optional.empty();
        var metadata = route.materializationMetadata(cell);
        var tasks = new M5MaterializationCoordinatorV1(metadata, identity.partition(), selector.binding());
        if (outputs.orElseThrow().reservations().stream()
                .anyMatch(row -> tasks.readPlan(row.taskIdSha256()).isPresent())) return Optional.empty();
        var caps = inputs.orElseThrow().caps();
        var limits = context.config().nereusKafkaStorageConfig().retentionCompaction();
        if (!limits.compactionEnabled()) return Optional.empty();
        int records = (int) Math.min(Math.min(limits.compactionTaskMaxRecords(), caps.maximumRecords()), KafkaCompactionRecordsV1.MAX_RECORDS);
        long decoded = limits.compactionDecodeMaxUncompressedBytes();
        var allowance = new NereusKafkaNativeInputV2.CaptureAllowance(new Caps(
                Math.min(Math.min(limits.compactionTaskMaxSourceBytes(), caps.maximumDirtyBytes()), outputs.orElseThrow().caps().maximumSourceBytes()),
                (int) Math.min(caps.maximumInputBatches(), KafkaCompactionRecordsV1.MAX_BATCHES), records,
                (int) Math.min(records, caps.maximumDistinctKeys()), Math.min(decoded, caps.maximumKeyBytes()),
                caps.maximumOutputBytes(), caps.maximumIndexBytes(), (int) Math.min(records, caps.maximumTransactions()),
                (int) Math.min(records, caps.maximumTombstones())), 0, 0, caps.maximumProviderOperations(),
                caps.maximumKmsOperations(), caps.maximumMetadataOperations(), caps.maximumResponseUnknowns());
        var preparation = NereusKafkaM4AdmissionV2.digest("M6-NATIVE-OBJECT-WAL-COMPACTION-PREPARATION-V1",
                selector.binding().bindingId().digest().toHex(), selector.binding().storageEpochSha256().toHex(),
                workerFence.toHex(), Long.toString(selector.sourceGeneration()));
        var reservation = allowance.reservation(preparation);
        var admitted = inputAdmission.reserve(reservation);
        if (admitted == KafkaCompactionAdmissionV1.Outcome.REJECTED_CAP) return Optional.empty();
        requireExact(admitted);
        boolean handedOff = false;
        try {
            var captured = partition.captureForCompaction(authorization, policy, selector, rawView, provider,
                    allowance.planCaps(), decoded);
            if (captured.isEmpty()) return Optional.empty();
            if (!route.workers().migrateLegacyAuthority()) {
                throw new IllegalStateException("native Object WAL worker authority is unresolved");
            }
            var proposal = proposal(selector);
            var prepared = NereusKafkaObjectMaterialization.prepare(captured.orElseThrow(), proposal, provider, allowance.planCaps(), decoded);
            requireExact(inputAdmission.replace(reservation, allowance.reservation(prepared.plan().taskIdSha256())));
            handedOff = true;
            return Optional.of(new Job(prepared, policy, selector));
        } catch (UnresolvedRead failure) {
            handedOff = true;
            failure.originalDrain().thenRun(() -> releaseInputs(preparation));
            throw failure;
        } finally {
            if (!handedOff) requireExact(inputAdmission.cancelPreparation(preparation));
        }
    }

    private Proposal proposal(BindingReadSelector selector) {
        return route.workers().propose(selector, cell, provider.profile().scopeId().digest(), storageFence,
                workerFence, namespace).orElseThrow(() -> new IllegalStateException("native Object WAL worker predecessor changed"));
    }

    private void releaseInputs(Sha256Digest id) {
        requireExact(inputAdmission.release(id));
    }

    private static void requireExact(KafkaCompactionAdmissionV1.Outcome outcome) {
        if (outcome != KafkaCompactionAdmissionV1.Outcome.APPLIED_EXACT
                && outcome != KafkaCompactionAdmissionV1.Outcome.EXISTING_EXACT) {
            throw new IllegalStateException("native Object WAL compaction input capacity is unresolved: " + outcome);
        }
    }

    private final class Job {
        private final NereusKafkaObjectMaterialization.Prepared prepared;
        private final NereusKafkaNativeInputV2.TopicPolicy policy;
        private final BindingReadSelector predecessor;
        private NereusKafkaObjectMaterialization.Registered registered;
        private NereusKafkaObjectMaterialization.Stored stored;
        private NereusKafkaObjectMaterialization.Verified verified;

        private Job(NereusKafkaObjectMaterialization.Prepared prepared, NereusKafkaNativeInputV2.TopicPolicy policy,
                BindingReadSelector predecessor) {
            this.prepared = prepared;
            this.policy = policy;
            this.predecessor = predecessor;
        }

        private void requireCurrent() {
            requireOwner.run();
            if (!NereusKafkaM5PolicyInputV1.samePolicyInputs(context, identity, policy.config(), policy.kafkaFeatureLevel())) {
                throw new FencedLeaderEpochException("native Object WAL compaction policy changed");
            }
        }

        private CompletionStage<Void> advance(Executor executor) throws Exception {
            requireCurrent();
            if (registered == null) {
                var plan = prepared.plan();
                registered = NereusKafkaObjectMaterialization.register(prepared, provider.profile(), identity.partition(),
                        route.materializationMetadata(cell), this::requireCurrent,
                        () -> route.workers().resumePlan(plan, workerFence, namespace)
                                .or(() -> proposal(predecessor).admitPlan(plan)).orElseThrow());
            }
            if (stored == null) stored = NereusKafkaObjectMaterialization.store(registered, provider, this::requireCurrent, timeout);
            if (verified == null) verified = NereusKafkaObjectMaterialization.protectAndVerify(stored, provider, this::requireCurrent, timeout);
            return NereusKafkaObjectPublication.publishNativeWal(verified, provider, partition, this::requireCurrent, executor, timeout)
                    .thenRun(() -> {
                        releaseInputs(prepared.plan().taskIdSha256());
                        settled = true;
                    });
        }
    }
}
