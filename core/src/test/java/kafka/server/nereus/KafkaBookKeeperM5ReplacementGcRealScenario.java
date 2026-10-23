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

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Calls the owned production replacement GC only after the original native read/retirement assertions. */
final class KafkaBookKeeperM5ReplacementGcRealScenario {
    private KafkaBookKeeperM5ReplacementGcRealScenario() { }

    static void reclaimIfAdmitted(NereusKafkaOwnedProviderRuntime.Opened opened, boolean required) throws Exception {
        boolean admitted = Boolean.getBoolean("nereus.nsip.m5SealedGcProfile")
                && opened.nativeM5GcAdmissionOpen().toCompletableFuture().get(30, TimeUnit.SECONDS);
        if (required) assertTrue(admitted);
        if (!admitted) return;
        var exact = opened.nativeM5BindingRoute().readBindingAuthority().toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow();
        var binding = com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
        if (required) assertEquals(com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode.PREFERRED_ONLY, binding.selectorProjection().mode());
        if (binding.selectorProjection().mode() != com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode.PREFERRED_ONLY)
            return;
        assertTrue(binding.kafkaPolicy().isPresent());
        var selectedBefore = selectedBytes(opened);
        try (var scan = opened.openNativeM5SourceScan().toCompletableFuture().get(30, TimeUnit.SECONDS)) {
            var inputs = NereusKafkaM5GcInputsV1.captureReplacement(scan).toCompletableFuture().get(30, TimeUnit.SECONDS);
            // A real pending recovery/Task reference is retained; the existing scenario's assertions still apply.
            boolean retained = inputs.resources().stream().anyMatch(resource -> new NereusKafkaM5GcReferencesV1(inputs, resource).initialObservations()
                    .stream().anyMatch(row -> row.disposition() != com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceDispositionV1.ABSENT));
            if (required) assertTrue(!retained && !inputs.resources().isEmpty(), "actual replacement was retained or had no physical target");
            if (retained) return;
            var before = scan.physical().quota().snapshot().toCompletableFuture().get(30, TimeUnit.SECONDS);
            awaitGc(scan, inputs, before);
            var after = scan.physical().quota().snapshot().toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertEquals(500_000_000, after.head().capacityBytes());
            System.out.println("M5_CANONICAL_CAPACITY_BYTES " + after.head().capacityBytes());
            assertEquals(before.head().reservedResources() - inputs.resources().size(), after.head().reservedResources());
            assertEquals(before.head().settledResources() + inputs.resources().size(), after.head().settledResources());
            assertEquals(before.head().reservedResources() + before.head().settledResources(),
                    after.head().reservedResources() + after.head().settledResources());
            var permanent = scan.physical().members().captureNamespace(
                    com.nereusstream.storage.object.retention.M5RetentionRecordsV1.MAX_REFERENCE_ROWS,
                    com.nereusstream.storage.object.retention.M5RetentionRecordsV1.MAX_SCAN_BYTES).toCompletableFuture().get(30, TimeUnit.SECONDS);
            for (var resource : inputs.resources()) {
                var retainedMember = permanent.resources().stream().filter(value -> value.resourceSha256().equals(resource.sha256()))
                        .findFirst().orElseThrow().members().orElseThrow();
                assertEquals(inputs.members().get(resource), retainedMember);
                var done = scan.physical().read(resource.authorityKey()).toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow();
                assertTrue(com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2.decode(done).compactDone().isPresent());
                assertTrue(scan.physical().quota().requireExistingReservation(resource).toCompletableFuture().get(30, TimeUnit.SECONDS).settled());
                assertEquals(com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureOutcome.DEFINITIVELY_ABSENT,
                        scan.nativeClient().captureExactTarget(inputs.handle(resource)).toCompletableFuture().get(30, TimeUnit.SECONDS).outcome());
            }
        }
        assertEquals(selectedBefore, selectedBytes(opened), "replacement GC changed the actual selected bytes");
    }

    static java.util.List<com.nereusstream.domain.bytes.CanonicalBytes> selectedBytes(
            NereusKafkaOwnedProviderRuntime.Opened opened) throws Exception {
        var request = new com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperSequentialReadRequestV1(
                opened.storage().capture().root().frontiers().trimStartOffset(),
                com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1.REPLICA, Long.MAX_VALUE, java.util.Optional.empty());
        var read = opened.storage().read(request).toCompletableFuture().get(30, TimeUnit.SECONDS);
        assertEquals(com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadOutcomeV1.FOUND, read.outcome(), read.detail());
        return read.batches().stream().map(value -> value.rawAssignedRecordBatch()).toList();
    }

    private static void awaitGc(NereusKafkaM5SourceScanV1 scan, NereusKafkaM5GcInputsV1 inputs,
            com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2.Snapshot before) throws Exception {
        try {
            // The owned scope processes targets serially; keep the existing30s observation budget per target.
            scan.reclaimReplacedSources().toCompletableFuture().get(Math.multiplyExact(30L, Math.max(1, inputs.resources().size())), TimeUnit.SECONDS);
        } catch (java.util.concurrent.TimeoutException timeout) {
            var states = new java.util.TreeMap<String, String>();
            for (var resource : inputs.resources()) {
                var actual = scan.physical().read(resource.authorityKey()).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
                var stored = com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2.decode(actual);
                states.put(resource.toString(), stored.state().name());
            }
            var current = scan.physical().quota().snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS);
            throw new IllegalStateException("replacement GC wait expired: targets=" + inputs.resources().size()
                    + ", settledBefore=" + before.head().settledResources() + ", settledNow=" + current.head().settledResources()
                    + ", actualStates=" + states, timeout);
        }
    }

}
