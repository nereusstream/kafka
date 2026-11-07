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
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperObjectReadScopeV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaObjectMaterializedPrefixV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperClosedHistoryRecoveryV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaKafkaBookKeeperM4ReadRouteV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Member;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationKeysV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.GenerationObject;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializedGeneration;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.TaskState;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.ValidatedGeneration;
import com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Reopens the exact selected Object manifest, without choosing a replacement task or redispatching a PUT. */
final class NereusKafkaObjectRecovery {
    private NereusKafkaObjectRecovery() { }

    record Selected(KafkaObjectMaterializedPrefixV1 prefix, MaterializationPlan plan, ValidatedGeneration validated,
            M5PendingOutputInventoryV2 inventory, List<PhysicalResourceIdV2.ObjectVersion> versions) { }

    static Optional<Selected> recover(OxiaKafkaBookKeeperM4ReadRouteV2 binding,
            int partition, BindingReadSelector selector, NereusKafkaObjectProvider provider, Duration timeout)
            throws Exception {
        var profile = provider.profile();
        var metadata = binding.materializationMetadata(profile.protocolCellSha256());
        var keys = new M5MaterializationKeysV1(partition, selector.binding());
        var selected = metadata.get(keys.manifest(selector.selectedViewSha256()));
        if (selected.isEmpty()) return Optional.empty();
        var manifest = M5MaterializationCodecV1.decodeManifest(selected.orElseThrow());
        var generation = M5MaterializationCodecV1.decodeGeneration(
                read(metadata, keys.generation(manifest.preferredGenerationSha256())));
        var validation = M5MaterializationCodecV1.decodeValidationRoot(
                read(metadata, keys.validation(generation.validationRootSha256())));
        var plan = M5MaterializationCodecV1.decodePlan(read(metadata, keys.plan(generation.taskIdSha256())));
        var cut = M5MaterializationCodecV1.encodeSourceCut(plan.sourceCut());
        var inventory = M5PendingOutputInventoryV2.decode(read(metadata, keys.pendingOutputs(generation.taskIdSha256())));
        var validated = new ValidatedGeneration(validation, generation, manifest);
        var prefix = new KafkaObjectMaterializedPrefixV1(plan, validated, provider::readForFetch);
        inventory.requirePlan(plan);
        if (!prefix.manifestSha256().equals(selector.selectedViewSha256())
                || prefix.sourceGeneration() > selector.sourceGeneration()
                || !selector.capability().equals(plan.sourceCut().identity().capability())
                || !plan.sourceCut().identity().binding().equals(selector.binding())) {
            throw new IllegalStateException("selected Kafka Object manifest differs from its exact selector");
        }
        if (!read(metadata, keys.sourceCut(plan.taskIdSha256())).equals(cut)
                || !inventory.profile().equals(profile)
                || inventory.state() != M5PendingOutputInventoryV2.State.PREPARED) {
            throw new IllegalStateException("Kafka Object recovery lacks its exact retained cut and profile");
        }
        requireOriginalTask(metadata, keys, generation, prefix.manifestSha256());
        requireProtectedFallback(binding, selector, prefix, timeout);
        var versions = requireOriginalObjects(provider, inventory, generation, timeout);
        await(provider.requireCurrent(), timeout);
        if (!binding.readSelector().equals(Optional.of(selector))) {
            throw new IllegalStateException("Kafka Object selector changed during exact recovery");
        }
        return Optional.of(new Selected(prefix, plan, validated, inventory, versions));
    }

    static Optional<KafkaBookKeeperClosedHistoryRecoveryV1.ObjectPrefix> nativeScope(Optional<Selected> selected,
            OxiaKafkaBookKeeperM4ReadRouteV2 binding, CanonicalControlMetadataStore metadata,
            BindingReadSelector selector, Duration timeout) throws Exception {
        var bytes = metadata.get(KafkaBookKeeperObjectReadScopeV1.key(selector.selectedViewSha256()));
        if (bytes.isEmpty()) return Optional.empty();
        var scope = KafkaBookKeeperObjectReadScopeV1.decode(bytes.orElseThrow());
        var manifest = selected.orElseThrow(
                () -> new IllegalStateException("Kafka Object native scope has no original selected manifest"));
        var prefix = new KafkaBookKeeperClosedHistoryRecoveryV1.ObjectPrefix(selector, manifest.prefix(), scope);
        if (!releasedHead(binding, manifest.prefix(), scope.retirementBatch(), timeout).equals(scope.releaseProofHead())) {
            throw new IllegalStateException("Kafka Object recovery has another BK fallback release proof");
        }
        if (!binding.readSelector().equals(Optional.of(selector))) {
            throw new IllegalStateException("Kafka Object selector changed during native scope recovery");
        }
        return Optional.of(prefix);
    }

    static void persistNativeScope(Selected selected, OxiaKafkaBookKeeperM4ReadRouteV2 binding,
            CanonicalControlMetadataStore metadata, BindingReadSelector selector,
            KafkaBookKeeperPartitionV1 storage, KafkaBookKeeperPartitionV1.ObjectBookKeeperSourceTransfer transfer,
            Runnable requireCurrent, Duration timeout) throws Exception {
        var prefix = selected.prefix();
        var release = transfer.release();
        var checkpoint = transfer.durableCheckpoint().orElseThrow(
                () -> new IllegalStateException("Kafka Object reference transfer has no retained durable BK checkpoint"));
        var carrier = checkpoint.sealedCarrier();
        if (selector.ownerEpoch() != release.selector().ownerEpoch()
                || selector.readAdmissionEpoch() != release.selector().readAdmissionEpoch()
                || !selector.selectedViewSha256().equals(release.selector().selectedViewSha256())
                || !prefix.originalFallbackSources().equals(release.batch().sources())) {
            throw new IllegalStateException("Kafka Object native scope does not name the actual drained fallback");
        }
        var batch = release.batch().batchIdSha256();
        var scope = new KafkaBookKeeperObjectReadScopeV1(selector.binding(), prefix.manifestSha256(),
                M5MaterializationCodecV1.sourceCutSha256(prefix.sourceCut()), selector.ownerEpoch(),
                release.batch().sharedLastFallbackCapableReadAdmissionEpoch(), batch,
                releasedHead(binding, prefix, batch, timeout), prefix.coverage().exclusiveEnd(), carrier.runId(),
                carrier.ledgerIdentity().ledgerId(), checkpoint.checkpointEntry(), checkpoint.checkpointSha256(),
                checkpoint.footerSha256());
        scope.requireSelection(prefix, selector);
        if (!scope.namesRetainedCarrier(carrier)) {
            throw new IllegalStateException("Kafka Object checkpoint does not belong to its retained BK suffix");
        }
        requireCurrent.run();
        storage.nativeObjectRetainedInputSources(selector, scope);
        var bytes = scope.encode();
        metadata.putIfAbsent(scope.key(), bytes);
        if (!metadata.get(scope.key()).equals(Optional.of(bytes))) {
            throw new IllegalStateException("Kafka Object native scope publication lacks its exact stored bytes");
        }
        storage.nativeObjectRetainedInputSources(selector, scope);
        requireCurrent.run();
    }

    private static Sha256Digest releasedHead(OxiaKafkaBookKeeperM4ReadRouteV2 binding, KafkaObjectMaterializedPrefixV1 prefix,
            Sha256Digest batch, Duration timeout) throws Exception {
        var proof = Optional.<Sha256Digest>empty();
        for (var source : prefix.originalFallbackSources()) {
            var fact = await(binding.readProtectionFact(source.sourceIdentitySha256(), source.protectionGeneration()),
                    timeout).orElseThrow();
            var release = M4ReadControlCodecV1.decodeProtection(fact.canonicalStoredBytes());
            if (!release.identity().equals(source) || release.state() != ProtectionState.RELEASED
                    || !release.releasedByBatchSha256().equals(Optional.of(batch))
                    || proof.isPresent() && !proof.equals(release.releaseProofHeadSha256())) {
                throw new IllegalStateException("Kafka Object native scope lacks its exact released BK fallback");
            }
            proof = release.releaseProofHeadSha256();
        }
        return proof.orElseThrow();
    }

    private static void requireOriginalTask(CanonicalControlMetadataStore metadata, M5MaterializationKeysV1 keys,
            MaterializedGeneration generation, Sha256Digest manifest) {
        var task = M5MaterializationCodecV1.decodeTask(read(metadata, keys.task(generation.taskIdSha256())));
        if (task.state() != TaskState.PUBLISHED && task.state() != TaskState.OUTPUT_VERIFIED) {
            throw new IllegalStateException("selected Kafka Object task lacks verified immutable outputs");
        }
        if (!task.taskIdSha256().equals(generation.taskIdSha256())
                || !task.outputIdentitySha256().equals(generation.outputIdentitySha256())
                || !task.validationRootSha256().equals(Optional.of(generation.validationRootSha256()))
                || !task.generationSha256().equals(Optional.of(M5MaterializationCodecV1.generationSha256(generation)))
                || !task.manifestViewSha256().equals(Optional.of(manifest))) {
            throw new IllegalStateException("selected Kafka Object generation differs from its original retained Task");
        }
    }

    private static void requireProtectedFallback(OxiaKafkaBookKeeperM4ReadRouteV2 binding,
            BindingReadSelector selector, KafkaObjectMaterializedPrefixV1 prefix, Duration timeout) throws Exception {
        if (selector.admissionState() != AdmissionState.ADMITTING) {
            throw new IllegalStateException("Kafka Object recovery selector is not admitting");
        }
        if (selector.mode() == SelectorMode.PREFERRED_ONLY) return;
        var sources = prefix.originalFallbackSources();
        if (selector.mode() != SelectorMode.PREFERRED_WITH_FALLBACK || !selector.fallbackSetSha256()
                .equals(Optional.of(M4ReadControlCodecV1.calculateFallbackSetSha256(sources)))) {
            throw new IllegalStateException("Kafka Object recovery has another fallback set");
        }
        for (var source : sources) {
            var found = await(binding.readProtectionFact(source.sourceIdentitySha256(), source.protectionGeneration()),
                    timeout).orElseThrow();
            var protection = M4ReadControlCodecV1.decodeProtection(found.canonicalStoredBytes());
            if (!protection.identity().equals(source) || protection.state() != ProtectionState.PROTECTED) {
                throw new IllegalStateException("Kafka Object recovery lacks the still-protected original BK source");
            }
        }
    }

    private static List<PhysicalResourceIdV2.ObjectVersion> requireOriginalObjects(NereusKafkaObjectProvider provider,
            M5PendingOutputInventoryV2 inventory, MaterializedGeneration generation, Duration timeout) throws Exception {
        var route = provider.physicalQuota();
        var scope = await(route.quota().snapshot(), timeout).head().scope();
        var profile = provider.profile();
        var objects = Stream.concat(generation.payloadObjects().stream(), generation.indexObjects().stream())
                .collect(Collectors.toUnmodifiableMap(object -> object.identity().key(), object -> object));
        if (objects.size() != inventory.outputs().size()) {
            throw new IllegalStateException("Kafka Object recovery does not name all original outputs");
        }
        var member = new Member(PositionDomain.KAFKA_OFFSET, profile.scopeId().digest(), generation.identity().binding());
        var versions = new ArrayList<PhysicalResourceIdV2.ObjectVersion>();
        for (var slot : inventory.outputs()) {
            var object = objects.get(slot.objectKey());
            if (object == null) throw new IllegalStateException("Kafka Object recovery has another output key");
            var physical = versionOf(object, profile.physicalBinding().physicalNamespace());
            var credit = await(route.quota().readBirth(M5ObjectBirthCreditV3.key(scope, inventory.worker(), slot)), timeout)
                    .orElseThrow();
            if (credit.state() != M5ObjectBirthCreditV3.State.ASSIGNED
                    || !credit.output().equals(Optional.of(object.identity()))
                    || !credit.resource().equals(Optional.of(physical))) {
                throw new IllegalStateException("Kafka Object recovery has another original birth or immutable version");
            }
            var membership = await(route.members().readFact(physical), timeout).inventory();
            if (!membership.protocolCell().equals(profile.protocolCellSha256()) || !membership.members().contains(member)) {
                throw new IllegalStateException("Kafka Object recovery lacks its exact physical member");
            }
            versions.add(physical);
        }
        return List.copyOf(versions);
    }

    private static PhysicalResourceIdV2.ObjectVersion versionOf(GenerationObject object,
            PhysicalResourceIdV2.Namespace namespace) {
        return new PhysicalResourceIdV2.ObjectVersion(namespace, CanonicalUtf8.fromString(object.identity().key()),
                PhysicalResourceIdV2.ObjectIdentityKind.IMMUTABLE_VERSION,
                CanonicalUtf8.fromBytes(object.immutableProviderVersionToken().orElseThrow().toByteArray()));
    }

    private static CanonicalBytes read(CanonicalControlMetadataStore metadata, String key) {
        return metadata.get(key).orElseThrow(() -> new IllegalStateException("missing selected Kafka Object value: " + key));
    }

    private static <T> T await(CompletionStage<T> operation, Duration timeout) throws Exception {
        return operation.toCompletableFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }
}
