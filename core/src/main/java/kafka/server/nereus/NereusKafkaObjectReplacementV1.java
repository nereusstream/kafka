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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperObjectReadScopeV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperReplacementSemanticsV2;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperClosedHistoryRecoveryV1;
import com.nereusstream.metadata.oxia.v2.compaction.OxiaKafkaRunRootAuthorityV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaKafkaBookKeeperM4ReadRouteV2;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.SemanticAspect;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.SemanticTransfer;
import com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationKeysV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationSourceCut;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.TaskState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/** The selected Object generation and the independent retained BK protocol checkpoint. */
final class NereusKafkaObjectReplacementV1 {
    private NereusKafkaObjectReplacementV1() { }

    static Selection capture(NereusKafkaObjectRecovery.Selected selected, KafkaBookKeeperClosedHistoryRecoveryV1.ObjectPrefix prefix,
            OxiaKafkaBookKeeperM4ReadRouteV2 binding, PhysicalResourceIdV2.Namespace bookKeeper, int partition) {
        var plan = selected.plan();
        var cell = plan.sourceCut().identity().protocolCellSha256();
        var keys = new M5MaterializationKeysV1(partition, prefix.selector().binding());
        var validation = selected.validated().validationRoot();
        var generation = selected.validated().generation();
        var manifest = selected.validated().manifestView();
        var facts = new ArrayList<VersionedValue>();
        var validationFact = read(binding, cell, keys.validation(generation.validationRootSha256()), facts);
        if (!validationFact.canonicalStoredBytes().equals(M5MaterializationCodecV1.encodeValidationRoot(validation))
                || !read(binding, cell, keys.generation(manifest.preferredGenerationSha256()), facts)
                        .canonicalStoredBytes().equals(M5MaterializationCodecV1.encodeGeneration(generation))
                || !read(binding, cell, keys.manifest(prefix.scope().manifest()), facts)
                        .canonicalStoredBytes().equals(M5MaterializationCodecV1.encodeManifest(manifest))
                || !read(binding, cell, keys.sourceCut(plan.taskIdSha256()), facts)
                        .canonicalStoredBytes().equals(M5MaterializationCodecV1.encodeSourceCut(plan.sourceCut()))
                || !read(binding, cell, keys.pendingOutputs(plan.taskIdSha256()), facts)
                        .canonicalStoredBytes().equals(selected.inventory().encode())) {
            throw new IllegalStateException("Kafka Object scan changed its actual materialization records");
        }
        var task = M5MaterializationCodecV1.decodeTask(read(binding, cell, keys.task(plan.taskIdSha256()), facts).canonicalStoredBytes());
        var admission = M5MaterializationAdmissionV1.decode(read(binding, cell,
                M5MaterializationAdmissionV1.stateKey(cell), facts).canonicalStoredBytes());
        if (task.state() != TaskState.PUBLISHED || !admission.reservations().isEmpty()
                || generation.indexObjects().stream().noneMatch(index -> index.indexKind() == IndexKind.TIMESTAMP)) {
            throw new IllegalStateException("Kafka Object scan requires a settled Task and complete side indexes");
        }
        return new Selection(selected, prefix, bookKeeper, facts, validationFact);
    }

    private static VersionedValue read(OxiaKafkaBookKeeperM4ReadRouteV2 binding, Sha256Digest cell, String key,
            List<VersionedValue> retained) {
        var fact = binding.readNativeMaterializationFact(cell, key).toCompletableFuture().join().orElseThrow();
        retained.add(fact);
        return fact;
    }

    /** The Object payload/index bytes and independent checkpoint have been reread before this exact capture. */
    static List<KafkaBookKeeperReplacementSemanticsV2.SourceEvidence> sources(Selection selection,
            OxiaKafkaRunRootAuthorityV2 roots, VersionedValue nativeScope, KafkaBookKeeperObjectReadScopeV1.CarrierProof carrier) {
        var validation = selection.selected().validated().validationRoot();
        var scope = selection.nativePrefix().scope();
        if (!nativeScope.canonicalStoredBytes().equals(scope.encode())
                || !carrier.checkpointFrame().equals(scope.checkpointFrame()) || !carrier.footerFrame().equals(scope.footerFrame())) {
            throw new IllegalStateException("Kafka Object replacement has another verified checkpoint");
        }
        var proof = NereusKafkaM5GcInputsV1.fact(selection.validation());
        var checkpoint = NereusKafkaM5GcInputsV1.fact(nativeScope);
        var result = new ArrayList<KafkaBookKeeperReplacementSemanticsV2.SourceEvidence>();
        for (var extent : selection.sourceCut().sources()) {
            var raw = roots.readSelectedRootFact(extent.physicalKey()).toCompletableFuture().join().orElseThrow().record();
            if (extent.kind() != SourceKind.BOOKKEEPER_LEDGER || !raw.resource().namespace().equals(selection.bookKeeper())
                    || !extent.ledgerIdentitySha256().orElseThrow().equals(raw.resource().sha256())) {
                throw new IllegalStateException("Kafka Object replacement names another original BK resource");
            }
            var transfers = new ArrayList<SemanticTransfer>();
            for (var aspect : SemanticAspect.values()) {
                var covered = extent.coverage();
                transfers.add(switch (aspect) {
                    case READ_RESULTS -> new SemanticTransfer(aspect, covered,
                            validation.payloadEqualityOrSemanticRootSha256(), validation.payloadEqualityOrSemanticRootSha256(), proof);
                    case TIMESTAMP_LOOKUP -> new SemanticTransfer(aspect, covered,
                            validation.lookupBoundaryRootSha256(), validation.lookupBoundaryRootSha256(), proof);
                    case INDEX_COVERAGE -> new SemanticTransfer(aspect, covered,
                            validation.coverageRootSha256(), validation.coverageRootSha256(), proof);
                    case NATIVE_SOURCE_AUTHORITY -> new SemanticTransfer(aspect, covered,
                            validation.authorityFenceRootSha256(), validation.authorityFenceRootSha256(), proof);
                    case RECOVERY, PRODUCER_STATE, TRANSACTIONS, LEADER_EPOCH -> new SemanticTransfer(aspect, covered,
                            scope.checkpointFrame(), carrier.checkpointFrame(), checkpoint);
                });
            }
            result.add(new KafkaBookKeeperReplacementSemanticsV2.SourceEvidence(
                    raw.resource(), extent.sourceIdentitySha256(), extent.coverage(), transfers));
        }
        return List.copyOf(result);
    }

    record Selection(NereusKafkaObjectRecovery.Selected selected, KafkaBookKeeperClosedHistoryRecoveryV1.ObjectPrefix nativePrefix,
            PhysicalResourceIdV2.Namespace bookKeeper, List<VersionedValue> nativeFacts, VersionedValue validation) {
        Selection {
            nativeFacts = List.copyOf(nativeFacts);
            if (!nativeFacts.contains(validation))
                throw new IllegalArgumentException("Object replacement lacks its exact validation record");
            if (!selected.prefix().manifestSha256().equals(nativePrefix.scope().manifest())
                    || !selected.plan().sourceCut().equals(nativePrefix.prefix().sourceCut())
                    || bookKeeper.providerKind() != PhysicalResourceIdV2.ProviderKind.BOOKKEEPER) {
                throw new IllegalArgumentException("native Object replacement has another selected physical cut");
            }
        }

        Sha256Digest taskId() {
            return selected.plan().taskIdSha256();
        }

        MaterializationSourceCut sourceCut() {
            return selected.plan().sourceCut();
        }

        KafkaBookKeeperPartitionV1.NativeInputSources currentProtocol(KafkaBookKeeperPartitionV1 storage,
                BindingReadSelector selector) {
            return storage.nativeObjectRetainedInputSources(selector, nativePrefix.scope());
        }

        List<PhysicalResourceIdV2> replacementResources(VersionedValue scope) {
            if (!scope.canonicalStoredBytes().equals(nativePrefix.scope().encode())) {
                throw new IllegalStateException("native Object replacement lacks its exact retained scope authority");
            }
            var carrier = new PhysicalResourceIdV2.BookKeeperLedger(bookKeeper, nativePrefix.scope().retainedLedger());
            return Stream.concat(Stream.<PhysicalResourceIdV2>of(carrier), selected.versions().stream()).sorted().toList();
        }
    }
}
