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
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Actual Broker cancellation prerequisite only; this fixture never manufactures eligibility or dispatches delete. */
final class KafkaBookKeeperM5UnpublishedRealScenario {
    private KafkaBookKeeperM5UnpublishedRealScenario() { }

    static void verifyClosedCaptureAndGc(NereusKafkaNativeInputV2.Captured closed, String mode,
            NereusKafkaOwnedProviderRuntime.Opened opened,
            com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.Task registered, java.util.Properties client,
            com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionAdmissionV1.CellCaps caps,
            NereusKafkaNativeInputV2.CaptureAllowance allowance) throws Exception {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, closed::batches);
        boolean dedicated = mode.startsWith("TASK_OUTPUT_GC");
        if (!dedicated && !sharedCandidate(mode, opened, registered)) return;
        var budget = KafkaBookKeeperNativeInputRealScenario.budget(opened);
        var task = java.util.Objects.requireNonNull(registered).taskIdSha256();
        java.util.Set<com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2> originalResources;
        com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2 accounting;
        try (var scan = opened.openNativeM5SourceScan().toCompletableFuture().get(30, TimeUnit.SECONDS)) {
            accounting = scan.physical();
            var original = NereusKafkaM5GcInputsV1.capture(scan, task).toCompletableFuture().get(30, TimeUnit.SECONDS);
            originalResources = original.unpublished().sealed().keySet();
            var before = scan.physical().quota().snapshot().toCompletableFuture().get(30, TimeUnit.SECONDS);
            if (dedicated && !mode.equals("TASK_OUTPUT_GC")) {
                verifyVeto(scan, task, original, before);
                return;
            }
            scan.reclaimCancelledTask(task).toCompletableFuture().get(30, TimeUnit.SECONDS);
            var after = scan.physical().quota().snapshot().toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertEquals(500_000_000, after.head().capacityBytes());
            System.out.println("M5_CANONICAL_CAPACITY_BYTES " + after.head().capacityBytes());
            assertEquals(before.head().reservedResources() - originalResources.size(), after.head().reservedResources());
            assertEquals(before.head().reservedResources() + before.head().settledResources(),
                    after.head().reservedResources() + after.head().settledResources());
            assertEquals(before.head().settledResources() + originalResources.size(), after.head().settledResources());
            assertTrue(after.availableCanonicalBytes() > before.availableCanonicalBytes());
            var permanent = scan.physical().members().captureNamespace(M5RetentionRecordsV1.MAX_REFERENCE_ROWS, M5RetentionRecordsV1.MAX_SCAN_BYTES).toCompletableFuture().get(30, TimeUnit.SECONDS);
            for (var resource : originalResources) {
                var retainedMember = permanent.resources().stream().filter(row -> row.resourceSha256().equals(resource.sha256()))
                        .findFirst().orElseThrow().members().orElseThrow();
                assertEquals(original.unpublished().members().get(resource), retainedMember);
                var exact = scan.physical().read(resource.authorityKey()).toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow();
                assertTrue(com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2.decode(exact).compactDone().isPresent());
                assertTrue(scan.physical().quota().requireExistingReservation(resource).toCompletableFuture().get(30, TimeUnit.SECONDS).settled());
                assertEquals(com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureOutcome.DEFINITIVELY_ABSENT,
                        scan.nativeClient().captureExactTarget(original.unpublished().sealed().get(resource).handle())
                                .toCompletableFuture().get(30, TimeUnit.SECONDS).outcome());
                assertFalse(retainedMember.inventory().members().isEmpty());
            }
        }
        if (!dedicated) return; // Original shared scenarios retain their data and ordering.
        verifyReadmission(opened, client, caps, allowance, budget, task, accounting, originalResources);
    }

    private static boolean sharedCandidate(String mode, NereusKafkaOwnedProviderRuntime.Opened opened,
            com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.Task registered) throws Exception {
        if (!Boolean.getBoolean("nereus.nsip.m5SealedGcProfile") || registered == null
                || mode.startsWith("TASK_OUTPUT_PUBLISH") || !opened.nativeM5GcAdmissionOpen().toCompletableFuture().get(30, TimeUnit.SECONDS)) return false;
        var stored = opened.nativeM5BindingRoute().compactionRecordFacts().getVersioned(
                com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperTaskTerminalV2.key(registered.taskIdSha256()))
                .toCompletableFuture().get(30, TimeUnit.SECONDS);
        return stored.isPresent() && com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperTaskTerminalV2
                .decode(stored.orElseThrow().canonicalStoredBytes()).physicalCut().stream().anyMatch(part -> part.sealed().isPresent());
    }

    private static void verifyVeto(NereusKafkaM5SourceScanV1 scan, Sha256Digest task, NereusKafkaM5GcInputsV1 original,
            com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2.Snapshot before) throws Exception {
        var originalResources = original.unpublished().sealed().keySet();
        var authorities = new java.util.HashMap<String, com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue>();
        for (var resource : originalResources) authorities.put(resource.authorityKey(), scan.physical().read(resource.authorityKey())
                .toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow());
        org.junit.jupiter.api.Assertions.assertThrows(java.util.concurrent.ExecutionException.class,
                () -> scan.reclaimCancelledTask(task).toCompletableFuture().get(30, TimeUnit.SECONDS));
        assertEquals(before, scan.physical().quota().snapshot().toCompletableFuture().get(30, TimeUnit.SECONDS));
        for (var entry : authorities.entrySet()) assertEquals(Optional.of(entry.getValue()),
                scan.physical().read(entry.getKey()).toCompletableFuture().get(30, TimeUnit.SECONDS));
        for (var resource : originalResources) assertEquals(
                com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureOutcome.EXACT_TARGET,
                scan.nativeClient().captureExactTarget(original.unpublished().sealed().get(resource).handle())
                        .toCompletableFuture().get(30, TimeUnit.SECONDS).outcome());
    }

    private static void verifyReadmission(NereusKafkaOwnedProviderRuntime.Opened opened, java.util.Properties client,
            com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionAdmissionV1.CellCaps caps,
            NereusKafkaNativeInputV2.CaptureAllowance allowance,
            com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperReadCellBudgetV2 budget, Sha256Digest task,
            com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2 accounting,
            java.util.Set<com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2> originalResources) throws Exception {
        KafkaBookKeeperRootVerifierRealScenario.produce(client, "after-gc", Math.toIntExact(opened.storage().capture().root().frontiers().durableEndOffset()));
        var fresh = KafkaBookKeeperNativeInputRealScenario.withCurrentMetadataImage(() -> opened.captureM5Inputs(
                budget, KafkaBookKeeperNativeInputRealScenario.BOUNDS, caps, allowance), true);
        try {
            var next = fresh.prepareTask(4096, 65536).toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertFalse(next.task().taskIdSha256().equals(task));
            var parts = next.materialize().toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertFalse(parts.isEmpty());
            for (var part : parts) {
                assertFalse(originalResources.contains(part.part().resource()));
                assertFalse(accounting.quota().requireExistingReservation(part.part().resource())
                        .toCompletableFuture().get(30, TimeUnit.SECONDS).settled());
                assertEquals(500_000_000, accounting.quota().snapshot().toCompletableFuture().get(30, TimeUnit.SECONDS).head().capacityBytes());
            }
        } finally {
            fresh.discard().toCompletableFuture().get(30, TimeUnit.SECONDS);
        }
    }

    static void verifyInputs(NereusKafkaOwnedProviderRuntime.Opened opened, Sha256Digest task) throws Exception {
        var before = opened.nativeM5BindingRoute().readBindingAuthority().toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow();
        var admitted = M5BindingAuthorityCodecV1.decodeAuthority(before.canonicalStoredBytes());
        if (admitted.kafkaPolicy().isEmpty()) return;
        try (var scan = opened.openNativeM5SourceScan().toCompletableFuture().get(30, TimeUnit.SECONDS)) {
            var captured = NereusKafkaM5GcInputsV1.capture(scan, task).toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertFalse(captured.unpublished().sealed().isEmpty(), "actual cancelled output was not materialized");
            for (var resource : captured.unpublished().sealed().keySet()) {
                var producer = new NereusKafkaM5GcReferencesV1(captured, resource);
                var rows = producer.initialObservations();
                assertEquals(15, rows.stream().map(M5RetentionRecordsV1.ReferenceObservationV1::kind).distinct().count());
                assertTrue(rows.stream().allMatch(row -> row.enumerationComplete()
                        && row.disposition() == M5RetentionRecordsV1.ReferenceDispositionV1.ABSENT));
                var facts = captured.readOnlyFacts(scan.physical());
                for (var row : rows) {
                    var exact = facts.read(row.authority().key()).toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow();
                    assertEquals(exact.metadataVersion(), row.authority().metadataVersion());
                    assertEquals(exact.canonicalStoredSha256(), row.authority().valueSha256());
                }
                var source = NereusKafkaM5GcInputsV1.fact(scan.exactAuthority());
                var request = new M5RetentionEvidenceAssemblerV1.FloorSnapshotRequestV1(captured.sourceCut().identity(),
                        captured.sourceCut().coverage().domain(), admitted.authorityGeneration(), captured.retainedNativeFrontier(),
                        Sha256Digest.hash(captured.policy().encode()), source, source);
                var floors = producer.assembler(scan.physical()).buildFloorSnapshot(request).toCompletableFuture().get(30, TimeUnit.SECONDS);
                assertEquals(10, floors.rows().size());
                var group = floors.rows().stream().filter(row -> row.floorClass()
                        == M5RetentionRecordsV1.FloorClassV1.KAFKA_CONSUMER_GROUP).findFirst().orElseThrow();
                assertFalse(group.constraining());
                assertEquals(-1, group.safeFloor());
                assertEquals(captured.retainedNativeFrontier(), floors.minimumSafeFloor());
            }
            scan.requireCurrent().toCompletableFuture().get(30, TimeUnit.SECONDS);
        }
        var after = M5BindingAuthorityCodecV1.decodeAuthority(opened.nativeM5BindingRoute().readBindingAuthority()
                .toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow().canonicalStoredBytes());
        assertEquals(Optional.empty(), after.sourceScan());
        assertEquals(admitted.kafkaPolicy(), after.kafkaPolicy());
        assertEquals(admitted.logicalTrim(), after.logicalTrim());
    }
}
