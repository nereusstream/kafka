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
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryCodecV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperTaskTerminalV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaKafkaBookKeeperM4ReadRouteV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.retention.M5TaskSelectionDecisionV2;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static kafka.server.nereus.NereusKafkaReadScopeV2.record;
import static kafka.server.nereus.NereusKafkaReadScopeV2.requireCurrentMemberFacts;
import static kafka.server.nereus.NereusKafkaReadScopeV2.requireCurrentPhysicalFacts;
import static kafka.server.nereus.NereusKafkaReadScopeV2.taskCreateScope;

/** Cancelled native Task/part facts read through the existing Owner and its bound BK/Oxia connections. */
final class NereusKafkaUnpublishedScopeV2 {
    private NereusKafkaUnpublishedScopeV2() { }

    /** Original terminal seal plus a separate current native observation; a missing ledger is never a live seal. */
    record SealedPartFacts(M5BookKeeperNativeCreateClientV2.FencedLedgerScopeFacts fenced,
            com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.BookKeeperDeleteTargetV1 closedMetadata,
            Optional<M5BookKeeperNativeCreateClientV2.SealedScopeFacts> live) {
        com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1 handle() {
            return fenced.handle();
        }

        com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateSpecV2 createScope() {
            return fenced.scope().createScope();
        }

        M5BookKeeperNativeCreateClientV2.NativeRecordFact reservation() {
            return fenced.reservation();
        }
    }

    /** Actual cancelled output facts; reference/grace scans and GC admission remain required. */
    record Evidence(KafkaBookKeeperTaskTerminalV2 terminal, List<VersionedValue> records, VersionedValue decision,
            VersionedValue nativeTerminal, VersionedValue nativeDecision, List<VersionedValue> completed,
            java.util.Map<String, Optional<VersionedValue>> partInventory,
            OxiaKafkaBookKeeperM4ReadRouteV2.SelectorFact selector,
            M5BookKeeperNativeCreateClientV2.TaskCreateScopeFact createScope,
            java.util.Map<PhysicalResourceIdV2, SealedPartFacts> sealed,
            java.util.Map<PhysicalResourceIdV2, OxiaQuotaTargetDeleteStoreV2.ResourceFact> physical,
            java.util.Map<PhysicalResourceIdV2, com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Fact> members) {
        Evidence {
            records = List.copyOf(records);
            completed = List.copyOf(completed);
            partInventory = java.util.Map.copyOf(partInventory);
            sealed = java.util.Map.copyOf(sealed);
            physical = java.util.Map.copyOf(physical);
            members = java.util.Map.copyOf(members);
        }

        /** Original fenced/cancelled Task facts only; a complete reference/grace proof is still required for GC. */
        com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.UnpublishedEvidence unpublishedTaskEvidence(
                PhysicalResourceIdV2 resource) {
            if (!sealed.containsKey(resource) || !physical.containsKey(resource) || !members.containsKey(resource))
                throw new IllegalArgumentException("resource is outside this original cancelled Task cut");
            var terminalFact = authority(nativeTerminal);
            var decisionFact = authority(nativeDecision);
            return new com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.UnpublishedEvidence(
                    terminal.task().taskIdSha256(), terminalFact, terminalFact, decisionFact, Optional.of(decisionFact));
        }

        private static com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1 authority(
                VersionedValue value) {
            return new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1(
                    value.key(), value.metadataVersion(), value.canonicalStoredSha256());
        }
    }

    static Evidence capture(OxiaKafkaBookKeeperM4ReadRouteV2 route,
            OxiaQuotaTargetDeleteStoreV2 physicalRoute, Sha256Digest protocolCell,
            M5BookKeeperNativeCreateClientV2 nativeClient, Sha256Digest taskId) {
        return capture(route, physicalRoute, protocolCell, nativeClient, taskId,
                route.readOpenSelectorFact().toCompletableFuture().join().orElseThrow());
    }

    static Evidence captureAtSourceScan(OxiaKafkaBookKeeperM4ReadRouteV2 route,
            OxiaQuotaTargetDeleteStoreV2 physicalRoute, Sha256Digest protocolCell,
            M5BookKeeperNativeCreateClientV2 nativeClient, Sha256Digest taskId, VersionedValue exactScan) {
        return capture(route, physicalRoute, protocolCell, nativeClient, taskId,
                route.readSourceScanSelectorFact(exactScan).toCompletableFuture().join());
    }

    private static Evidence capture(OxiaKafkaBookKeeperM4ReadRouteV2 route,
            OxiaQuotaTargetDeleteStoreV2 physicalRoute, Sha256Digest protocolCell,
            M5BookKeeperNativeCreateClientV2 nativeClient, Sha256Digest taskId,
            OxiaKafkaBookKeeperM4ReadRouteV2.SelectorFact selector) {
        var transport = route.compactionRecordFacts();
        var records = new ArrayList<VersionedValue>();
        var inventory = record(transport, KafkaBookKeeperInventoryV2.taskKey(taskId), records);
        var task = KafkaBookKeeperInventoryCodecV2.decodeTask(inventory.canonicalStoredBytes());
        var binding = M5MaterializationCodecV1.decodeSourceCut(task.sourceCut()).identity().binding();
        var decision = route.readTaskDecisionFact(binding, taskId).toCompletableFuture().join().orElseThrow();
        var selected = M5TaskSelectionDecisionV2.decode(decision.canonicalStoredBytes());
        requireCancelledTask(taskId, task, selector, nativeClient, selected);
        var terminalRecord = record(transport, KafkaBookKeeperTaskTerminalV2.key(taskId), records);
        var terminal = KafkaBookKeeperTaskTerminalV2.decode(terminalRecord.canonicalStoredBytes());
        var expected = taskCreateScope(task, nativeClient.spec().nativeInstanceId());
        if (!terminal.task().equals(task) || !terminal.selection().equals(selected)
                || !terminal.nativeCreateScope().equals(expected))
            throw new IllegalStateException("unpublished native terminal differs from its exact Task or create scope");
        var scope = nativeClient.readTaskCreateScopeFact(expected).toCompletableFuture().join();
        if (!scope.fenced()) throw new IllegalStateException("unpublished native Task create scope is not fenced");
        var sealed = new java.util.TreeMap<PhysicalResourceIdV2, SealedPartFacts>();
        var physical = new java.util.TreeMap<PhysicalResourceIdV2, OxiaQuotaTargetDeleteStoreV2.ResourceFact>();
        var members = new java.util.TreeMap<PhysicalResourceIdV2, com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Fact>();
        var completed = new ArrayList<VersionedValue>();
        var parts = new java.util.LinkedHashMap<String, Optional<VersionedValue>>();
        long bytes = scope.instanceIdentity().rawStoredBytes().length() + (long) scope.taskFence().rawStoredBytes().length();
        for (int ordinal = 0; ordinal < task.parts().size(); ordinal++) {
            var slot = terminal.physicalCut().get(ordinal);
            var key = KafkaBookKeeperInventoryV2.partKey(taskId, ordinal);
            var stored = transport.getVersioned(key).toCompletableFuture().join();
            parts.put(key, stored);
            var decoded = requireUnpublishedPart(task, ordinal, slot, stored);
            bytes += stored.map(value -> value.canonicalStoredBytes().length()).orElse(0);
            if (slot.sealed().isEmpty()) continue;
            boolean gcScan = com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1.isAuthorityValue(selector.stored().canonicalStoredBytes())
                    && com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1.decodeAuthority(selector.stored().canonicalStoredBytes()).sourceScan().isPresent();
            var storedAuthority = physicalRoute.read(decoded.orElseThrow().resource().authorityKey()).toCompletableFuture().join().orElseThrow();
            var storedDelete = com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2.decode(storedAuthority);
            if (gcScan && storedDelete.state() == com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1.DELETE_DONE_V1) {
                if (!storedDelete.resource().equals(decoded.orElseThrow().resource()))
                    throw new IllegalStateException("permanent native DONE belongs to another Task output resource");
                completed.add(storedAuthority);
                continue;
            }
            var value = captureUnpublishedPart(task, slot, decoded.orElseThrow(), nativeClient, physicalRoute, protocolCell, gcScan);
            var resource = decoded.orElseThrow().resource();
            sealed.put(resource, value.sealed());
            physical.put(resource, value.physical());
            members.put(resource, value.members());
            bytes += value.sealed().reservation().rawStoredBytes().length() + (long) value.sealed().live().map(live -> live.ledgerMetadata().rawStoredBytes().length()).orElse(0)
                    + value.physical().reservation().canonicalStoredBytes().length() + value.physical().authority().canonicalStoredBytes().length()
                    + value.members().canonicalStoredBytes().length();
        }
        bytes += decision.canonicalStoredBytes().length();
        bytes += records.stream().mapToLong(value -> value.canonicalStoredBytes().length()).sum();
        if (bytes > com.nereusstream.storage.object.retention.M5RetentionRecordsV1.MAX_SCAN_BYTES)
            throw new IllegalStateException("unpublished native output exceeds the retained fact byte budget");
        var nativeTerminal = transport.nativeFact(terminalRecord).toCompletableFuture().join();
        var nativeDecision = route.nativeTaskDecisionFact(binding, taskId, decision).toCompletableFuture().join();
        bytes += nativeTerminal.canonicalStoredBytes().length() + (long) nativeDecision.canonicalStoredBytes().length();
        if (bytes > com.nereusstream.storage.object.retention.M5RetentionRecordsV1.MAX_SCAN_BYTES)
            throw new IllegalStateException("unpublished native authority facts exceed the retained byte budget");
        var result = new Evidence(terminal, records, decision, nativeTerminal, nativeDecision, completed, parts, selector, scope, sealed, physical, members);
        requireCurrentUnpublished(result, route, physicalRoute, nativeClient);
        return result;
    }

    private static void requireCancelledTask(Sha256Digest taskId, KafkaBookKeeperInventoryV2.Task task,
            OxiaKafkaBookKeeperM4ReadRouteV2.SelectorFact selector, M5BookKeeperNativeCreateClientV2 nativeClient,
            M5TaskSelectionDecisionV2 selected) {
        var binding = M5MaterializationCodecV1.decodeSourceCut(task.sourceCut()).identity().binding();
        if (!task.taskIdSha256().equals(taskId) || !binding.equals(selector.selector().binding())
                || !task.namespace().equals(nativeClient.spec().namespace()))
            throw new IllegalStateException("unpublished native Task differs from its actual Binding or namespace");
        if (!selected.taskId().equals(taskId) || !selected.binding().equals(binding)
                || selected.outcome() != M5TaskSelectionDecisionV2.Outcome.SELECTION_CANCELLED)
            throw new IllegalStateException("unpublished native Task lacks its cancelled selection decision");
    }

    private static void requireCurrentUnpublished(Evidence facts, OxiaKafkaBookKeeperM4ReadRouteV2 route,
            OxiaQuotaTargetDeleteStoreV2 physicalRoute, M5BookKeeperNativeCreateClientV2 nativeClient) {
        var transport = route.compactionRecordFacts();
        for (var value : facts.records())
            if (!transport.getVersioned(value.key()).toCompletableFuture().join().equals(Optional.of(value)))
                throw new IllegalStateException("unpublished native metadata changed during fact capture: " + value.key());
        for (var value : facts.partInventory().entrySet())
            if (!transport.getVersioned(value.getKey()).toCompletableFuture().join().equals(value.getValue()))
                throw new IllegalStateException("unpublished native Part inventory changed during fact capture");
        var expected = facts.createScope().createScope();
        var binding = facts.terminal().selection().binding();
        var decision = facts.decision();
        if (!route.readTaskDecisionFact(binding, facts.terminal().task().taskIdSha256()).toCompletableFuture().join().equals(Optional.of(decision))
                || !nativeClient.readTaskCreateScopeFact(expected).toCompletableFuture().join().equals(facts.createScope()))
            throw new IllegalStateException("unpublished native selection or create fence changed during fact capture");
        for (var value : facts.sealed().values()) requireCurrentNativePart(value, expected, nativeClient);
        for (var value : facts.completed())
            if (!physicalRoute.read(value.key()).toCompletableFuture().join().equals(Optional.of(value)))
                throw new IllegalStateException("native permanent DONE changed during Task inventory capture");
        requireCurrentPhysicalFacts(facts.physical(), physicalRoute);
        requireCurrentMemberFacts(facts.members(), physicalRoute);
        if (!route.readBindingAuthority().toCompletableFuture().join().equals(Optional.of(facts.selector().stored())))
            throw new IllegalStateException("unpublished native Binding authority changed during fact capture");
    }

    private static void requireCurrentNativePart(SealedPartFacts value,
            com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateSpecV2 expected,
            M5BookKeeperNativeCreateClientV2 nativeClient) {
        if (!nativeClient.readFencedLedgerScopeFacts(expected, value.handle()).toCompletableFuture().join().equals(value.fenced()))
            throw new IllegalStateException("unpublished native output create fence changed during fact capture");
        if (value.live().isPresent()) {
            if (!nativeClient.readSealedScopeFacts(expected, value.handle()).toCompletableFuture().join().equals(value.live().orElseThrow()))
                throw new IllegalStateException("unpublished native output seal changed during fact capture");
        } else if (nativeClient.captureExactTarget(value.handle()).toCompletableFuture().join().outcome()
                != com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureOutcome.DEFINITIVELY_ABSENT) {
            throw new IllegalStateException("unpublished native output absence changed during fact capture");
        }
    }

    private static Optional<KafkaBookKeeperInventoryV2.Part> requireUnpublishedPart(KafkaBookKeeperInventoryV2.Task task, int ordinal,
            KafkaBookKeeperTaskTerminalV2.DrainedPart slot, Optional<VersionedValue> stored) {
        if (slot.reservedId().isPresent() != stored.isPresent())
            throw new IllegalStateException("unpublished native Part inventory differs from the terminal cut");
        return stored.map(value -> {
            var part = KafkaBookKeeperInventoryCodecV2.decodePart(value.canonicalStoredBytes());
            var configuration = task.configuration(ordinal);
            if (!part.taskIdSha256().equals(task.taskIdSha256()) || part.ordinal() != ordinal
                    || !part.resource().namespace().equals(task.namespace()))
                throw new IllegalStateException("unpublished native Part has another Task or namespace");
            if (!part.handle().providerScopeId().equals(configuration.providerScopeId())
                    || !part.handle().runId().equals(configuration.runId())
                    || !part.handle().configurationDigest().equals(configuration.configurationDigest())
                    || !slot.reservedId().equals(Optional.of(part.handle().ledgerIdentity())))
                throw new IllegalStateException("unpublished native Part has another reserved identity");
            return part;
        });
    }

    private record UnpublishedPartFacts(SealedPartFacts sealed,
            OxiaQuotaTargetDeleteStoreV2.ResourceFact physical,
            com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Fact members) { }

    private static UnpublishedPartFacts captureUnpublishedPart(KafkaBookKeeperInventoryV2.Task task,
            KafkaBookKeeperTaskTerminalV2.DrainedPart slot, KafkaBookKeeperInventoryV2.Part part,
            M5BookKeeperNativeCreateClientV2 nativeClient, OxiaQuotaTargetDeleteStoreV2 physicalRoute, Sha256Digest protocolCell, boolean gcScan) {
        var expected = taskCreateScope(task, nativeClient.spec().nativeInstanceId());
        var physical = physicalRoute.readResourceFact(part.resource()).toCompletableFuture().join();
        var authority = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                physical.authority().canonicalStoredBytes());
        var open = authority.state() == com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1.OPEN_V1;
        var recovering = authority.state() == com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1.READ_FENCED_V1
                || authority.state() == com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1.DELETE_INTENT_V1;
        var fenced = nativeClient.readFencedLedgerScopeFacts(expected, part.handle()).toCompletableFuture().join();
        if ((!open && !(gcScan && recovering)) || !authority.activeWriterTickets().isEmpty()
                || !physical.binding().equals(fenced.scope().namespaceBinding()))
            throw new IllegalStateException("unpublished native output retains physical writer tickets or changed admission");
        var target = nativeClient.captureExactTarget(part.handle()).toCompletableFuture().join();
        Optional<M5BookKeeperNativeCreateClientV2.SealedScopeFacts> live;
        switch (target.outcome()) {
            case EXACT_TARGET:
                live = Optional.of(nativeClient.readSealedScopeFacts(expected, part.handle()).toCompletableFuture().join());
                if (!live.orElseThrow().closedMetadata().equals(slot.sealed().orElseThrow()))
                    throw new IllegalStateException("unpublished native Part differs from its drained terminal seal");
                break;
            case DEFINITIVELY_ABSENT:
                if (!gcScan || authority.state() != com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1.DELETE_INTENT_V1)
                    throw new IllegalStateException("native absence requires this original persisted delete INTENT");
                live = Optional.empty();
                break;
            default:
                throw new IllegalStateException("unpublished native output identity is changed or unknown");
        }
        var nativePart = new SealedPartFacts(fenced, slot.sealed().orElseThrow(), live);
        var members = physicalRoute.members().readFact(part.resource()).toCompletableFuture().join();
        var binding = M5MaterializationCodecV1.decodeSourceCut(task.sourceCut()).identity().binding();
        var expectedMember = new com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Member(
                com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain.KAFKA_OFFSET,
                task.capability().providerScopeId().digest(), binding);
        if (!members.inventory().protocolCell().equals(protocolCell) || !members.inventory().members().contains(expectedMember))
            throw new IllegalStateException("unpublished native output differs from its complete physical member inventory");
        return new UnpublishedPartFacts(nativePart, physical, members);
    }

}
