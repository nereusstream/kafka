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

import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1;
import com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.FloorAdapterResultV1;
import com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.ReferenceAdapterResultV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceDispositionV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceObservationV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceScanSummaryV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorObservationV1;

import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/** Actual cancelled-output or semantic-replacement observations; never advances the native logical trim. */
final class NereusKafkaM5GcReferencesV1 {
    private final NereusKafkaM5GcInputsV1 inputs;
    private final PhysicalResourceIdV2 resource;
    private final boolean publicReference;
    private final List<VersionedValue> epochFacts;

    NereusKafkaM5GcReferencesV1(NereusKafkaM5GcInputsV1 inputs, PhysicalResourceIdV2 resource) {
        this(inputs, resource, List.of());
    }

    NereusKafkaM5GcReferencesV1(NereusKafkaM5GcInputsV1 inputs, PhysicalResourceIdV2 resource,
            List<VersionedValue> epochFacts) {
        this.inputs = inputs;
        this.epochFacts = List.copyOf(epochFacts);
        this.resource = resource;
        if (!inputs.resources().contains(resource))
            throw new IllegalArgumentException("native reference target is outside the exact cancelled output");
        publicReference = inputs.hasPublicReference(resource);
    }

    M5RetentionEvidenceAssemblerV1 assembler(ExactMetadataTransactionStoreV1 otherFacts) {
        var floors = new EnumMap<FloorClassV1, M5RetentionEvidenceAssemblerV1.FloorAdapterV1>(FloorClassV1.class);
        var references = new EnumMap<ReferenceKindV1, M5RetentionEvidenceAssemblerV1.ReferenceAdapterV1>(ReferenceKindV1.class);
        for (var kind : FloorClassV1.values()) floors.put(kind, ignored -> CompletableFuture.completedFuture(floor(kind)));
        for (var kind : ReferenceKindV1.values()) references.put(kind, ignored -> CompletableFuture.completedFuture(reference(kind)));
        return new M5RetentionEvidenceAssemblerV1(inputs.readOnlyFacts(otherFacts), floors, references);
    }

    List<ReferenceObservationV1> initialObservations() {
        var rows = new ArrayList<ReferenceObservationV1>();
        for (var kind : ReferenceKindV1.values()) rows.addAll(reference(kind).observations());
        rows.sort(Comparator.comparing(ReferenceObservationV1::kind).thenComparing(row -> row.authority().key()));
        return List.copyOf(rows);
    }

    private FloorAdapterResultV1 floor(FloorClassV1 kind) {
        var authority = switch (kind) {
            case KAFKA_PRODUCER_TRANSACTION, KAFKA_REPLICATION_RECOVERY -> inputs.protocol();
            case LIFECYCLE_TASK -> inputs.lifecycleAuthority();
            default -> inputs.sourceAuthority();
        };
        // This physical cleanup preserves the actual already-published frontier. It cannot infer a newer expiry.
        boolean constraining = switch (kind) {
            case KAFKA_CONSUMER_GROUP, PULSAR_CURSOR_SUBSCRIPTION, PROJECTION_MIGRATION -> false;
            default -> true;
        };
        var row = new RetentionFloorObservationV1(kind, NereusKafkaM5GcInputsV1.fact(authority),
                PositionDomain.KAFKA_OFFSET, constraining ? inputs.retainedNativeFrontier() : -1, constraining, true);
        return new FloorAdapterResultV1(kind, 1, authority.canonicalStoredBytes().length(), List.of(row));
    }

    private ReferenceAdapterResultV1 reference(ReferenceKindV1 kind) {
        var actual = inputs.isReplacement() ? inputs.replacement().references().get(resource).stream()
                .filter(value -> value.kind() == kind).toList() : List.<ReferenceObservationV1>of();
        if (!actual.isEmpty()) {
            long bytes = actual.stream().map(ReferenceObservationV1::authority).distinct().mapToLong(authority ->
                    inputs.facts().stream().filter(value -> NereusKafkaM5GcInputsV1.fact(value).equals(authority))
                            .findFirst().orElseThrow(() -> new IllegalStateException("native replacement reference is outside its captured fact inventory"))
                            .canonicalStoredBytes().length()).sum();
            return new ReferenceAdapterResultV1(kind, new ReferenceScanSummaryV1(kind, actual.size(), 1, bytes, true),
                    actual.stream().sorted(Comparator.comparing(value -> value.authority().key())).toList());
        }
        var primary = referenceAuthority(kind);
        boolean present = switch (kind) {
            case MATERIALIZATION_OR_COMPACTION_TASK, RETIREMENT_OR_DELETE_RECONCILIATION -> inputs.unresolvedTask() || publicReference;
            case AUDIT_GRACE -> inputs.auditHeld(resource);
            default -> publicReference;
        };
        var facts = new TreeMap<String, VersionedValue>();
        facts.put(primary.key(), primary);
        if (kind == ReferenceKindV1.AUDIT_GRACE) {
            inputs.facts().forEach(value -> facts.put(value.key(), value));
            epochFacts.forEach(value -> facts.put(value.key(), value));
        }
        var rows = facts.values().stream().map(value -> new ReferenceObservationV1(kind,
                NereusKafkaM5GcInputsV1.fact(value), resource.sha256(), inputs.coverage(resource),
                present ? ReferenceDispositionV1.PRESENT : ReferenceDispositionV1.ABSENT, true)).toList();
        long bytes = facts.values().stream().mapToLong(value -> value.canonicalStoredBytes().length()).sum();
        return new ReferenceAdapterResultV1(kind, new ReferenceScanSummaryV1(kind, rows.size(), 1, bytes, true), rows);
    }
    private VersionedValue referenceAuthority(ReferenceKindV1 kind) {
        return switch (kind) {
            // Immutable cancellation excludes publication, hence also a generation handle to this output.
            // Actual selected/fallback actors and physical locators are additionally resolved by capture.
            case MANIFEST_SELECTED, MANIFEST_FALLBACK, READ_GENERATION_PIN_OR_OPEN_HANDLE, SOURCE_PROTECTION -> inputs.manifestAuthority();
            // Actual coherent K1 producer/transaction/leader-epoch and checkpoint scans remain required.
            case KAFKA_PRODUCER_RECOVERY, KAFKA_TRANSACTION_OR_ABORTED_RECOVERY,
                 KAFKA_REPLICA_OR_LEADER_EPOCH_RECOVERY, RECOVERY_CHECKPOINT_OR_SNAPSHOT -> inputs.protocol();
            case MATERIALIZATION_OR_COMPACTION_TASK, RETIREMENT_OR_DELETE_RECONCILIATION -> inputs.lifecycleAuthority();
            // Exclusions depend on actual first native policy and source-bound closed writer enrollment.
            case KAFKA_GROUP_RETENTION, PULSAR_SUBSCRIPTION_OR_REPLICATION_CURSOR,
                 PROJECTION_MIGRATION_OR_EXPORT -> inputs.sourceAuthority();
            // Capture rejects other physical members; business holds remain a separate real policy inventory.
            case SHARED_PHYSICAL_MEMBER, AUDIT_GRACE -> inputs.sourceAuthority();
        };
    }

}
