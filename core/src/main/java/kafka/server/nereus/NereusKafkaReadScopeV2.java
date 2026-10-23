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
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperNativeFallbackRetirementV2;
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperControlMetadataStoreV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperNativeReadScopeV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperNativeRecoveryScopeV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperReadViewV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperReadOwnerV2;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperNativeRootVerifierV2;
import com.nereusstream.metadata.oxia.v2.compaction.OxiaKafkaRunRootAuthorityV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaKafkaBookKeeperM4ReadRouteV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import java.util.Optional;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperReplacementSemanticsV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperDescriptorV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperDescriptorCodecV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionAdmissionV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryCodecV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperTaskTerminalV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateSpecV2;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.retention.M5TaskSelectionDecisionV2;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.M4ReleaseBindingV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceObservationV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceDispositionV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState;
import java.util.ArrayList;

/** Cold recovery qualifications and low-frequency native read-scope evidence persistence. */
final class NereusKafkaReadScopeV2 {
    private NereusKafkaReadScopeV2() { }

    /** Actual current actor/metadata observations; other references, floors, grace and GC admission remain separate. */
    record ReplacementEvidence(KafkaBookKeeperPartitionV1.NativeSelectedInputSources selected,
            List<KafkaBookKeeperReplacementSemanticsV2.SourceEvidence> sources,
            List<M4ReleaseBindingV1> releases, List<VersionedValue> records,
            VersionedValue nativeDescriptor, VersionedValue nativeSemantics,
            OxiaKafkaBookKeeperM4ReadRouteV2.SelectorFact selector,
            java.util.Map<PhysicalResourceIdV2, List<ReferenceObservationV1>> references,
            List<VersionedValue> quiescence,
            java.util.Map<PhysicalResourceIdV2, OxiaQuotaTargetDeleteStoreV2.ResourceFact> physical,
            java.util.Map<PhysicalResourceIdV2, SealedSourceFact> sealed,
            java.util.Map<PhysicalResourceIdV2, com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Fact> members,
            TaskFacts tasks, ReaderFacts readers, KafkaBookKeeperPartitionV1.NativeInputSources recovery, List<VersionedValue> completed) {
        ReplacementEvidence {
            completed = List.copyOf(completed);
            sources = List.copyOf(sources);
            releases = List.copyOf(releases);
            records = List.copyOf(records);
            references = references.entrySet().stream().collect(java.util.stream.Collectors.toUnmodifiableMap(
                    java.util.Map.Entry::getKey, entry -> List.copyOf(entry.getValue())));
            quiescence = List.copyOf(quiescence);
            physical = java.util.Map.copyOf(physical);
            sealed = java.util.Map.copyOf(sealed);
            members = java.util.Map.copyOf(members);
            java.util.Objects.requireNonNull(tasks, "tasks");
            java.util.Objects.requireNonNull(readers, "readers");
            java.util.Objects.requireNonNull(recovery, "recovery");
        }

        /** Native facts for one fully replaced resource; full member/reference/grace admission remains required. */
        com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.ReplacementEvidence replacementTaskEvidence(
                PhysicalResourceIdV2 resource) {
            var source = sources.stream().filter(value -> value.resource().equals(resource)).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("resource is outside the original replacement cut"));
            if (!sealed.containsKey(resource) || !physical.containsKey(resource) || !members.containsKey(resource))
                throw new IllegalStateException("native replacement resource lacks its sealed physical member facts");
            var verifier = fact(nativeSemantics);
            var transfers = source.transfers().stream().map(row -> {
                if (!row.verifierAuthority().valueSha256().equals(verifier.valueSha256()))
                    throw new IllegalStateException("native replacement verifier differs from its captured comparison");
                return new com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.SemanticTransfer(
                        row.aspect(), row.coverage(), row.priorSemanticRoot(), row.replacementSemanticRoot(), verifier);
            }).toList();
            var descriptor = selected.descriptor();
            if (!nativeDescriptor.canonicalStoredBytes().equals(descriptor.encode()))
                throw new IllegalStateException("native selected descriptor differs from its original captured generation");
            var replacements = descriptor.sealedParts().stream()
                    .map(part -> (PhysicalResourceIdV2) new PhysicalResourceIdV2.BookKeeperLedger(
                            descriptor.task().namespace(), part.handle().ledgerIdentity().ledgerId()))
                    .sorted().toList();
            return new com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.ReplacementEvidence(
                    fact(nativeDescriptor), replacements, transfers);
        }
    }

    record SealedSourceFact(Optional<OxiaKafkaRunRootAuthorityV2.RunRootFact> rawRoot,
            Optional<M5BookKeeperNativeCreateClientV2.SealedScopeFacts> live,
            Optional<M5BookKeeperNativeCreateClientV2.FencedLedgerScopeFacts> absentScope) {
        SealedSourceFact {
            java.util.Objects.requireNonNull(rawRoot, "rawRoot");
            java.util.Objects.requireNonNull(live, "live");
            java.util.Objects.requireNonNull(absentScope, "absentScope");
            if (live.isPresent() == absentScope.isPresent())
                throw new IllegalArgumentException("source requires exact live metadata or fenced absent INTENT scope");
        }

        SealedSourceFact(Optional<OxiaKafkaRunRootAuthorityV2.RunRootFact> root,
                M5BookKeeperNativeCreateClientV2.SealedScopeFacts scope) {
            this(root, Optional.of(scope), Optional.empty());
        }

        com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1 handle() {
            return live.map(M5BookKeeperNativeCreateClientV2.SealedScopeFacts::handle).orElseGet(() -> absentScope.orElseThrow().handle());
        }

        M5BookKeeperNativeCreateSpecV2 createScope() {
            return live.map(M5BookKeeperNativeCreateClientV2.SealedScopeFacts::createScope)
                    .orElseGet(() -> absentScope.orElseThrow().scope().createScope());
        }
    }

    /** Missing inventory denotes unresolved preparation/cache ownership, never a terminal or reference-free Task. */
    record TaskFact(KafkaCompactionAdmissionV1.Reservation reservation, Optional<VersionedValue> inventory,
            Optional<VersionedValue> decision, Optional<VersionedValue> terminal,
            Optional<VersionedValue> selectedOutput, Optional<M5BookKeeperNativeCreateClientV2.TaskCreateScopeFact> nativeScope) {
        TaskFact {
            inventory = java.util.Objects.requireNonNull(inventory, "inventory");
            decision = java.util.Objects.requireNonNull(decision, "decision");
            terminal = java.util.Objects.requireNonNull(terminal, "terminal");
            selectedOutput = java.util.Objects.requireNonNull(selectedOutput, "selectedOutput");
            nativeScope = java.util.Objects.requireNonNull(nativeScope, "nativeScope");
            java.util.Objects.requireNonNull(reservation, "reservation");
            if (inventory.isEmpty()) {
                if (decision.isPresent() || terminal.isPresent() || selectedOutput.isPresent() || nativeScope.isPresent())
                    throw new IllegalStateException("unresolved native preparation has Task terminal facts");
            } else {
                var value = inventory.orElseThrow();
                var task = KafkaBookKeeperInventoryCodecV2.decodeTask(value.canonicalStoredBytes());
                if (!value.key().equals(KafkaBookKeeperInventoryV2.taskKey(reservation.compactionTaskIdSha256()))
                        || !task.taskIdSha256().equals(reservation.compactionTaskIdSha256()))
                    throw new IllegalStateException("native Task reservation differs from its immutable inventory");
                requireTaskDecision(task, decision, terminal, nativeScope);
                requireTaskSelectedOutput(task, decision, selectedOutput);
            }
        }

        boolean closed() {
            if (decision.isEmpty() || nativeScope.filter(M5BookKeeperNativeCreateClientV2.TaskCreateScopeFact::fenced).isEmpty()) return false;
            return M5TaskSelectionDecisionV2.decode(decision.orElseThrow().canonicalStoredBytes()).outcome()
                    == M5TaskSelectionDecisionV2.Outcome.SELECTED || terminal.isPresent();
        }

        List<VersionedValue> records() {
            return java.util.stream.Stream.of(inventory, decision, terminal, selectedOutput).flatMap(Optional::stream).toList();
        }
    }

    private static void requireTaskSelectedOutput(KafkaBookKeeperInventoryV2.Task task, Optional<VersionedValue> decision,
            Optional<VersionedValue> output) {
        var identity = decision.flatMap(value -> M5TaskSelectionDecisionV2.decode(value.canonicalStoredBytes()).selectedOutput());
        if (identity.isPresent() != output.isPresent())
            throw new IllegalStateException("native selected Task lacks its exact published output");
        output.ifPresent(value -> {
            var descriptor = KafkaSealedBookKeeperDescriptorCodecV2.decode(value.canonicalStoredBytes());
            if (!descriptor.task().equals(task) || !identity.equals(Optional.of(descriptor.descriptorSha256()))
                    || !value.key().equals("v2/kafka-bk-compaction-v2/views/" + descriptor.descriptorSha256().toHex() + "/descriptor"))
                throw new IllegalStateException("native Task decision selects a different immutable output");
        });
    }

    private static void requireTaskDecision(KafkaBookKeeperInventoryV2.Task task, Optional<VersionedValue> decision,
            Optional<VersionedValue> terminal, Optional<M5BookKeeperNativeCreateClientV2.TaskCreateScopeFact> nativeScope) {
        var binding = M5MaterializationCodecV1.decodeSourceCut(task.sourceCut()).identity().binding();
        var selected = decision.map(value -> {
            var decoded = M5TaskSelectionDecisionV2.decode(value.canonicalStoredBytes());
            if (!value.key().equals(decoded.key()) || !decoded.taskId().equals(task.taskIdSha256()) || !decoded.binding().equals(binding))
                throw new IllegalStateException("native Task decision differs from its immutable inventory");
            return decoded;
        });
        terminal.ifPresent(value -> {
            var decoded = KafkaBookKeeperTaskTerminalV2.decode(value.canonicalStoredBytes());
            if (!value.key().equals(KafkaBookKeeperTaskTerminalV2.key(task.taskIdSha256())) || !decoded.task().equals(task)
                    || !selected.equals(Optional.of(decoded.selection())))
                throw new IllegalStateException("native Task cancellation terminal differs from its exact decision");
        });
        nativeScope.ifPresent(value -> {
            var expected = taskCreateScope(task, value.createScope().nativeInstanceId());
            if (!value.createScope().equals(expected) || !expected.namespace().equals(task.namespace()))
                throw new IllegalStateException("native Task create fence differs from its immutable inventory");
        });
    }

    static M5BookKeeperNativeCreateSpecV2 taskCreateScope(KafkaBookKeeperInventoryV2.Task task, String nativeInstance) {
        return M5BookKeeperNativeCreateSpecV2.of(nativeInstance, task.taskIdSha256(),
                java.util.stream.IntStream.range(0, task.parts().size()).mapToObj(task::configuration).toList());
    }

    record TaskFacts(VersionedValue admission, List<TaskFact> reservations) {
        TaskFacts {
            reservations = List.copyOf(reservations);
            var state = KafkaCompactionAdmissionV1.decode(admission.canonicalStoredBytes());
            if (!state.reservations().equals(reservations.stream().map(TaskFact::reservation).toList()))
                throw new IllegalArgumentException("native Task directory omitted a persisted Cell reservation");
        }
    }

    /** One enrolled native Binding's reader cut; absent historical scopes keep this class unresolved. */
    record ReaderFacts(OxiaKafkaRunRootAuthorityV2.OwnerAdmissionFact owner,
            java.util.Map<String, Optional<VersionedValue>> scopes,
            Optional<OxiaKafkaRunRootAuthorityV2.OwnerAdmissionFact> closedOwner) {
        ReaderFacts {
            java.util.Objects.requireNonNull(owner, "owner");
            scopes = java.util.Map.copyOf(scopes);
            java.util.Objects.requireNonNull(closedOwner, "closedOwner");
        }

        boolean closed() {
            return scopes.values().stream().anyMatch(Optional::isPresent);
        }

        List<VersionedValue> records() {
            return scopes.entrySet().stream().sorted(java.util.Map.Entry.comparingByKey())
                    .flatMap(entry -> entry.getValue().stream()).toList();
        }
    }

    static ReplacementEvidence captureReplacement(OxiaKafkaBookKeeperM4ReadRouteV2 route, KafkaBookKeeperPartitionV1 storage,
            OxiaQuotaTargetDeleteStoreV2 physicalRoute, Sha256Digest protocolCell, M5BookKeeperNativeCreateClientV2 nativeClient,
            OxiaKafkaRunRootAuthorityV2 roots, OxiaKafkaRunRootAuthorityV2.OwnerAdmissionFact owner,
            Function<KafkaRunRootRecordV2, M5BookKeeperNativeCreateSpecV2> nativeRunSpec) {
        return captureReplacement(route, storage, physicalRoute, protocolCell, nativeClient, roots, owner,
                nativeRunSpec, route.readOpenSelectorFact().toCompletableFuture().join().orElseThrow());
    }

    static ReplacementEvidence captureReplacementAtSourceScan(OxiaKafkaBookKeeperM4ReadRouteV2 route,
            KafkaBookKeeperPartitionV1 storage, OxiaQuotaTargetDeleteStoreV2 physicalRoute,
            Sha256Digest protocolCell, M5BookKeeperNativeCreateClientV2 nativeClient,
            OxiaKafkaRunRootAuthorityV2 roots, OxiaKafkaRunRootAuthorityV2.OwnerAdmissionFact owner,
            Function<KafkaRunRootRecordV2, M5BookKeeperNativeCreateSpecV2> nativeRunSpec, VersionedValue exactScan) {
        return captureReplacement(route, storage, physicalRoute, protocolCell, nativeClient, roots, owner,
                nativeRunSpec, route.readSourceScanSelectorFact(exactScan).toCompletableFuture().join());
    }

    private static ReplacementEvidence captureReplacement(OxiaKafkaBookKeeperM4ReadRouteV2 route,
            KafkaBookKeeperPartitionV1 storage, OxiaQuotaTargetDeleteStoreV2 physicalRoute,
            Sha256Digest protocolCell, M5BookKeeperNativeCreateClientV2 nativeClient,
            OxiaKafkaRunRootAuthorityV2 roots, OxiaKafkaRunRootAuthorityV2.OwnerAdmissionFact owner,
            Function<KafkaRunRootRecordV2, M5BookKeeperNativeCreateSpecV2> nativeRunSpec,
            OxiaKafkaBookKeeperM4ReadRouteV2.SelectorFact selector) {
        var inputs = storage.nativeInputSources();
        var selected = storage.nativeSelectedInputSources(inputs, selector.selector()).orElseThrow();
        var recovery = selected.recoveryInputs();
        var descriptor = selected.descriptor();
        var transport = route.compactionRecordFacts();
        var records = new ArrayList<VersionedValue>();
        var descriptorBytes = record(transport, "v2/kafka-bk-compaction-v2/views/" + descriptor.descriptorSha256().toHex() + "/descriptor", records);
        if (!descriptorBytes.canonicalStoredBytes().equals(descriptor.encode()))
            throw new IllegalStateException("native replacement descriptor differs from its installed actual cache");
        var semantics = record(transport, KafkaBookKeeperReplacementSemanticsV2.key(descriptor.descriptorSha256()), records);
        var targets = record(transport, "v2/kafka-bk-compaction-v2/views/" + descriptor.descriptorSha256().toHex() + "/publication-targets", records);
        Optional<KafkaSealedBookKeeperDescriptorV2> previous = Optional.empty();
        var first = descriptor.sourceCut().sources().get(0);
        if (first.kind() == com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind.KAFKA_BK_COMPACTED_GENERATION_V2) {
            var prior = record(transport, "v2/kafka-bk-compaction-v2/views/" + first.formatRootSha256().toHex() + "/descriptor", records);
            previous = Optional.of(KafkaSealedBookKeeperDescriptorCodecV2.decode(prior.canonicalStoredBytes()));
        }
        var comparison = KafkaBookKeeperReplacementSemanticsV2.decode(semantics.canonicalStoredBytes());
        var sources = comparison.sources(descriptor, fact(semantics), targets.canonicalStoredBytes(), previous);
        var nativeDescriptor = transport.nativeFact(descriptorBytes).toCompletableFuture().join();
        var nativeSemantics = transport.nativeFact(semantics).toCompletableFuture().join();
        var live = new java.util.HashSet<PhysicalResourceIdV2>();
        inputs.sources().sealedRoots().forEach(root -> live.add(new PhysicalResourceIdV2.BookKeeperLedger(descriptor.task().namespace(), root.ledgerIdentity().ledgerId())));
        live.add(new PhysicalResourceIdV2.BookKeeperLedger(descriptor.task().namespace(), inputs.sources().activeRoot().ledgerIdentity().ledgerId()));
        descriptor.sealedParts().forEach(part -> live.add(new PhysicalResourceIdV2.BookKeeperLedger(descriptor.task().namespace(), part.handle().ledgerIdentity().ledgerId())));
        if (sources.stream().anyMatch(source -> live.contains(source.resource())))
            throw new IllegalStateException("native replacement still has a current physical source reference");
        var releases = new ArrayList<M4ReleaseBindingV1>();
        for (var source : descriptor.sourceCut().sources()) {
            var observed = route.readProtectionFact(source.sourceIdentitySha256(), descriptor.sourceGeneration()).toCompletableFuture().join().orElseThrow();
            var protection = M4ReadControlCodecV1.decodeProtection(observed.canonicalStoredBytes());
            if (protection.state() != ProtectionState.RELEASED)
                throw new IllegalStateException("native replacement source has no exact M4 RELEASED proof");
            releases.add(new M4ReleaseBindingV1(source.sourceIdentitySha256(), descriptor.sourceGeneration(), fact(observed),
                    observed.canonicalStoredBytes(), protection.releasedByBatchSha256().orElseThrow(), protection.releaseProofHeadSha256().orElseThrow()));
            if (!route.readProtectionFact(source.sourceIdentitySha256(), descriptor.sourceGeneration()).toCompletableFuture().join().equals(Optional.of(observed)))
                throw new IllegalStateException("native replacement protection changed during fact capture");
        }
        var quiescence = route.readQuiescenceFacts(selector, releases);
        var cut = replacementPhysicalCut(sources, physicalRoute, selector);
        var activeSources = cut.sources();
        var completed = cut.completed();
        var physical = physicalFacts(activeSources, physicalRoute, cut.sourceScan());
        var tasks = taskFacts(route, protocolCell, nativeClient);
        var readers = readerFacts(route, roots, owner, inputs, selected, previous, releases, quiescence);
        var sealed = sealedSourceFacts(activeSources, descriptor, previous, roots, nativeClient, nativeRunSpec, physical, cut.sourceScan(), nativeDescriptor);
        var members = memberFacts(activeSources, physicalRoute, protocolCell, selector.selector().binding(),
                descriptor.task().capability().providerScopeId().digest());
        var references = referenceFacts(activeSources, releases, selector.stored(), tasks, readers, quiescence, recovery);
        requireNativeAuthorityRecordBudget(records, nativeDescriptor, nativeSemantics);
        for (var observed : records) {
            if (!transport.getVersioned(observed.key()).toCompletableFuture().join().equals(Optional.of(observed)))
                throw new IllegalStateException("native replacement record changed during fact capture");
        }
        selected.requireCurrent();
        requireCurrentPhysicalFacts(physical, physicalRoute);
        requireCurrentTaskFacts(tasks, route, protocolCell, nativeClient);
        requireCurrentReaderFacts(readers, route, roots);
        requireCurrentSealedSourceFacts(sealed, roots, nativeClient);
        requireCurrentMemberFacts(members, physicalRoute);
        if (!route.readQuiescenceFacts(selector, releases).equals(quiescence))
            throw new IllegalStateException("native quiescence authority changed during fact capture");
        if (!route.readBindingAuthority().toCompletableFuture().join().equals(Optional.of(selector.stored())))
            throw new IllegalStateException("native replacement Binding authority changed during fact capture");
        requireCurrentCompleted(completed, physicalRoute);
        selected.recoveryInputs();
        return new ReplacementEvidence(selected, sources, releases, records, nativeDescriptor, nativeSemantics,
                selector, references, quiescence, physical, sealed, members, tasks, readers, recovery, completed);
    }

    private record ReplacementPhysicalCut(List<KafkaBookKeeperReplacementSemanticsV2.SourceEvidence> sources,
            List<VersionedValue> completed, boolean sourceScan) { }

    private static ReplacementPhysicalCut replacementPhysicalCut(List<KafkaBookKeeperReplacementSemanticsV2.SourceEvidence> sources,
            OxiaQuotaTargetDeleteStoreV2 route, OxiaKafkaBookKeeperM4ReadRouteV2.SelectorFact selector) {
        boolean sourceScan = com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1.decodeAuthority(selector.stored().canonicalStoredBytes()).state()
                == com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingAuthorityStateV1.SOURCE_SCAN_FENCED_V2;
        if (!sourceScan) return new ReplacementPhysicalCut(sources, List.of(), false);
        var completed = new ArrayList<VersionedValue>();
        var active = new ArrayList<KafkaBookKeeperReplacementSemanticsV2.SourceEvidence>();
        for (var source : sources) {
            var value = route.read(source.resource().authorityKey()).toCompletableFuture().join().orElseThrow();
            if (sourceScan && com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2.decode(value).state()
                    == com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1.DELETE_DONE_V1) completed.add(value);
            else active.add(source);
        }
        return new ReplacementPhysicalCut(List.copyOf(active), List.copyOf(completed), sourceScan);
    }

    private static void requireCurrentCompleted(List<VersionedValue> completed, OxiaQuotaTargetDeleteStoreV2 route) {
        for (var value : completed)
            if (!route.read(value.key()).toCompletableFuture().join().equals(Optional.of(value)))
                throw new IllegalStateException("native replacement DONE changed during capture");
    }

    private static void requireNativeAuthorityRecordBudget(
            List<VersionedValue> records, VersionedValue nativeDescriptor, VersionedValue nativeSemantics) {
        long recordBytes = nativeDescriptor.canonicalStoredBytes().length() + (long) nativeSemantics.canonicalStoredBytes().length();
        for (var observed : records) recordBytes = Math.addExact(recordBytes, observed.canonicalStoredBytes().length());
        if (recordBytes > com.nereusstream.storage.object.retention.M5RetentionRecordsV1.MAX_SCAN_BYTES)
            throw new IllegalStateException("native replacement authority facts exceed the retained record byte budget");
    }

    private static java.util.Map<PhysicalResourceIdV2, com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Fact> memberFacts(
            List<KafkaBookKeeperReplacementSemanticsV2.SourceEvidence> sources, OxiaQuotaTargetDeleteStoreV2 route,
            Sha256Digest cell, com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity binding,
            Sha256Digest provider) {
        var expected = new com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Member(
                com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain.KAFKA_OFFSET, provider, binding);
        var result = new java.util.HashMap<PhysicalResourceIdV2, com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Fact>();
        for (var source : sources) {
            var observed = route.members().readFact(source.resource()).toCompletableFuture().join();
            if (!observed.inventory().protocolCell().equals(cell) || !observed.inventory().members().contains(expected))
                throw new IllegalStateException("native replacement source has another physical member identity");
            result.put(source.resource(), observed);
        }
        return java.util.Map.copyOf(result);
    }

    static void requireCurrentMemberFacts(
            java.util.Map<PhysicalResourceIdV2, com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Fact> members,
            OxiaQuotaTargetDeleteStoreV2 route) {
        for (var entry : members.entrySet())
            if (!route.members().readFact(entry.getKey()).toCompletableFuture().join().equals(entry.getValue()))
                throw new IllegalStateException("native physical member inventory changed during fact capture");
    }

    private static java.util.Map<PhysicalResourceIdV2, SealedSourceFact> sealedSourceFacts(
            List<KafkaBookKeeperReplacementSemanticsV2.SourceEvidence> sources, KafkaSealedBookKeeperDescriptorV2 descriptor,
            Optional<KafkaSealedBookKeeperDescriptorV2> previous, OxiaKafkaRunRootAuthorityV2 roots,
            M5BookKeeperNativeCreateClientV2 nativeClient, Function<KafkaRunRootRecordV2, M5BookKeeperNativeCreateSpecV2> nativeRunSpec,
            java.util.Map<PhysicalResourceIdV2, OxiaQuotaTargetDeleteStoreV2.ResourceFact> physical, boolean sourceScan, VersionedValue nativeDescriptor) {
        var result = new java.util.TreeMap<PhysicalResourceIdV2, SealedSourceFact>();
        for (var source : sources) {
            var extent = descriptor.sourceCut().sources().stream().filter(value -> value.sourceIdentitySha256().equals(source.sourceIdentity())).findFirst().orElseThrow();
            SealedSourceFact observed;
            if (extent.kind() == com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind.BOOKKEEPER_LEDGER) {
                var raw = roots.readSelectedRootFact(extent.physicalKey()).toCompletableFuture().join().orElseThrow();
                var root = raw.record().root();
                if (!raw.record().resource().equals(source.resource()) || root.state() != com.nereusstream.storage.api.kafka.KafkaRunRootStateV1.SEALED
                        || root.kafkaStartOffset() != source.coverage().inclusiveStart()
                        || root.kafkaEndOffsetExclusive().orElseThrow() != source.coverage().exclusiveEnd())
                    throw new IllegalStateException("sealed native source differs from its actual admitted run root");
                var handle = new com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1(root.providerScopeId(), root.runId(), root.ledgerIdentity(), nativeClient.capabilitySnapshot().configurationDigest());
                var expected = nativeRunSpec.apply(raw.record());
                var absent = absentIntentScope(source, nativeDescriptor, physical.get(source.resource()), sourceScan, nativeClient, expected, handle);
                if (absent.isPresent()) {
                    result.put(source.resource(), new SealedSourceFact(Optional.of(raw), Optional.empty(), absent));
                    continue;
                }
                var scope = nativeClient.readSealedScopeFacts(expected, handle).toCompletableFuture().join();
                var stable = new KafkaRunRootRecordV2(raw.record().resource(), root, true, Optional.empty(), false, raw.record().recoveryCut()).encode();
                var identity = Sha256Digest.hash(com.nereusstream.domain.bytes.CanonicalBytes.copyOf(java.nio.ByteBuffer.allocate(84 + stable.length())
                        .putInt(0x4d354e53).put(stable.toByteArray()).putLong(scope.closedMetadata().sealedLastEntryId())
                        .putLong(scope.closedMetadata().sealedLength()).put(extent.formatRootSha256().bytes().toByteArray())
                        .put(extent.bodySha256().bytes().toByteArray()).array()));
                if (!identity.equals(source.sourceIdentity()))
                    throw new IllegalStateException("sealed native metadata differs from the replaced raw source identity");
                observed = new SealedSourceFact(Optional.of(raw), scope);
            } else {
                var predecessor = previous.orElseThrow();
                var part = predecessor.sealedParts().stream().filter(value -> value.handle().ledgerIdentity().ledgerId()
                        == ((PhysicalResourceIdV2.BookKeeperLedger) source.resource()).ledgerId()).findFirst().orElseThrow();
                var expected = taskCreateScope(predecessor.task(), nativeClient.spec().nativeInstanceId());
                var absent = absentIntentScope(source, nativeDescriptor, physical.get(source.resource()), sourceScan, nativeClient, expected, part.handle());
                if (absent.isPresent()) {
                    result.put(source.resource(), new SealedSourceFact(Optional.empty(), Optional.empty(), absent));
                    continue;
                }
                var scope = nativeClient.readSealedScopeFacts(expected, part.handle()).toCompletableFuture().join();
                if (!scope.closedMetadata().equals(part))
                    throw new IllegalStateException("sealed native metadata differs from its predecessor Part descriptor");
                observed = new SealedSourceFact(Optional.empty(), scope);
            }
            if (!observed.createScope().namespace().equals(source.resource().namespace()))
                throw new IllegalStateException("sealed native source has another physical namespace");
            result.put(source.resource(), observed);
        }
        return result;
    }

    private static Optional<M5BookKeeperNativeCreateClientV2.FencedLedgerScopeFacts> absentIntentScope(
            KafkaBookKeeperReplacementSemanticsV2.SourceEvidence source, VersionedValue nativeDescriptor,
            OxiaQuotaTargetDeleteStoreV2.ResourceFact physical, boolean sourceScan, M5BookKeeperNativeCreateClientV2 nativeClient,
            M5BookKeeperNativeCreateSpecV2 expected, com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1 handle) {
        if (!sourceScan) return Optional.empty();
        var authority = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(physical.authority().canonicalStoredBytes());
        if (authority.state() != com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1.DELETE_INTENT_V1)
            return Optional.empty();
        var eligibility = authority.eligibilitySnapshot().orElseThrow();
        if (!eligibility.resource().equals(source.resource())
                || eligibility.reason() != com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.ReclamationReason.REPLACED_REPRESENTATION
                || eligibility.members().size() != 1
                || !eligibility.members().get(0).replacement().orElseThrow().selectedGeneration().equals(fact(nativeDescriptor)))
            throw new IllegalStateException("original native INTENT belongs to another replacement cut");
        var target = nativeClient.captureExactTarget(handle).toCompletableFuture().join();
        if (target.outcome() == com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureOutcome.EXACT_TARGET)
            return Optional.empty();
        if (target.outcome() != com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureOutcome.DEFINITIVELY_ABSENT)
            throw new IllegalStateException("original native INTENT target is changed or unknown");
        var fenced = nativeClient.readFencedLedgerScopeFacts(expected, handle).toCompletableFuture().join();
        if (!physical.binding().equals(fenced.scope().namespaceBinding()) || !authority.activeWriterTickets().isEmpty())
            throw new IllegalStateException("original native INTENT retains writers or another permanent scope");
        return Optional.of(fenced);
    }

    private static void requireCurrentSealedSourceFacts(java.util.Map<PhysicalResourceIdV2, SealedSourceFact> facts,
            OxiaKafkaRunRootAuthorityV2 roots, M5BookKeeperNativeCreateClientV2 nativeClient) {
        for (var observed : facts.values()) {
            observed.rawRoot().ifPresent(raw -> {
                if (!roots.readSelectedRootFact(raw.authority().key()).toCompletableFuture().join().equals(Optional.of(raw)))
                    throw new IllegalStateException("sealed native source root changed during fact capture");
            });
            if (observed.live().isPresent()) {
                if (!nativeClient.readSealedScopeFacts(observed.createScope(), observed.handle()).toCompletableFuture().join().equals(observed.live().orElseThrow()))
                    throw new IllegalStateException("sealed native source metadata changed during fact capture");
            } else if (!nativeClient.readFencedLedgerScopeFacts(observed.createScope(), observed.handle()).toCompletableFuture().join().equals(observed.absentScope().orElseThrow())
                    || nativeClient.captureExactTarget(observed.handle()).toCompletableFuture().join().outcome()
                    != com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureOutcome.DEFINITIVELY_ABSENT) {
                throw new IllegalStateException("original native INTENT absent scope changed during capture");
            }
        }
    }

    private static ReaderFacts readerFacts(OxiaKafkaBookKeeperM4ReadRouteV2 route, OxiaKafkaRunRootAuthorityV2 roots,
            OxiaKafkaRunRootAuthorityV2.OwnerAdmissionFact owner, KafkaBookKeeperPartitionV1.NativeInputSources inputs,
            KafkaBookKeeperPartitionV1.NativeSelectedInputSources selected,
            Optional<KafkaSealedBookKeeperDescriptorV2> previous, List<M4ReleaseBindingV1> releases, List<VersionedValue> quiescence) {
        var selector = selected.selector();
        var descriptor = selected.descriptor();
        var scope = Sha256Digest.hash(KafkaRunRootRecordV2.Scope.of(inputs.sources().activeRoot()).encode());
        if (owner.admission().closed() || !owner.admission().scopeSha256().equals(scope)
                || owner.admission().owner().ownerEpoch() != selector.ownerEpoch()
                || owner.admission().owner().kafkaLeaderEpoch() != inputs.protocol().root().fence().kafkaLeaderEpoch())
            throw new IllegalStateException("native reader capture differs from its actual open Owner");
        var batches = selector.activeBatches().stream().filter(batch -> releases.stream()
                .allMatch(release -> release.releasedByBatchSha256().equals(batch.batchIdSha256()))).toList();
        if (batches.size() != 1) throw new IllegalStateException("native reader capture lacks its exact released batch");
        var batch = batches.get(0);
        String warmKey = KafkaBookKeeperNativeReadScopeV2.key(descriptor.descriptorSha256(), selector.ownerEpoch(),
                batch.sharedLastFallbackCapableReadAdmissionEpoch());
        String coldKey = "v2/kafka-bk-compaction-v2/views/" + descriptor.descriptorSha256().toHex()
                + "/native-recovery-scope/" + selector.ownerEpoch() + "/" + selector.readAdmissionEpoch();
        var transport = route.compactionRecordFacts();
        var warm = transport.getVersioned(warmKey).toCompletableFuture().join();
        var cold = transport.getVersioned(coldKey).toCompletableFuture().join();
        warm.ifPresent(value -> requireWarmReader(value, owner, selected, previous, batch, quiescence));
        var closed = cold.map(value -> {
            var observed = KafkaBookKeeperNativeRecoveryScopeV2.decode(value.canonicalStoredBytes());
            var archive = roots.readClosedOwnerFact(observed.predecessor().ownerEpoch()).toCompletableFuture().join()
                    .orElseThrow(() -> new IllegalStateException("native cold reader lacks its exact closed Owner archive"));
            requireColdReader(value, observed, owner, archive, selected, previous);
            return archive;
        });
        return new ReaderFacts(owner, java.util.Map.of(warmKey, warm, coldKey, cold), closed);
    }

    private static void requireWarmReader(VersionedValue value, OxiaKafkaRunRootAuthorityV2.OwnerAdmissionFact owner,
            KafkaBookKeeperPartitionV1.NativeSelectedInputSources selected, Optional<KafkaSealedBookKeeperDescriptorV2> previous,
            com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceRetirementBatch batch, List<VersionedValue> quiescence) {
        var scope = KafkaBookKeeperNativeReadScopeV2.decode(value.canonicalStoredBytes());
        scope.requireDescriptor(selected.descriptor(), previous);
        var selector = selected.selector();
        var prefix = quiescence.stream().filter(fact -> fact.key().endsWith("/proof-head")).findFirst().orElseThrow().key();
        prefix = prefix.substring(0, prefix.lastIndexOf('/'));
        var epoch = String.format(java.util.Locale.ROOT, "%020d", scope.closedReadEpoch());
        String terminalKey = prefix + "/terminals/" + epoch;
        String proofKey = prefix + "/proofs/" + epoch;
        var terminal = quiescence.stream().filter(fact -> fact.key().equals(terminalKey)).findFirst().orElseThrow();
        var proof = quiescence.stream().filter(fact -> fact.key().equals(proofKey)).findFirst().orElseThrow();
        if (!value.key().equals(scope.key()) || !scope.owner().equals(owner.admission().owner())
                || !scope.nativeScopeSha256().equals(owner.admission().scopeSha256())
                || !scope.ownerFact().key().equals(owner.authority().key()))
            throw new IllegalStateException("native live reader scope differs from its actual Owner route");
        if (!scope.batchId().equals(batch.batchIdSha256())
                || scope.closedReadEpoch() != batch.sharedLastFallbackCapableReadAdmissionEpoch()
                || selector.readAdmissionEpoch() <= scope.closedReadEpoch()
                || !scope.selectorCoreSha256().equals(batch.successorSelectorCoreSha256())
                || !scope.terminalSha256().equals(terminal.canonicalStoredSha256())
                || !scope.proofSha256().equals(proof.canonicalStoredSha256()))
            throw new IllegalStateException("native live reader scope differs from its verified M4 terminal interval");
    }

    private static void requireColdReader(VersionedValue value, KafkaBookKeeperNativeRecoveryScopeV2 scope,
            OxiaKafkaRunRootAuthorityV2.OwnerAdmissionFact owner, OxiaKafkaRunRootAuthorityV2.OwnerAdmissionFact archive,
            KafkaBookKeeperPartitionV1.NativeSelectedInputSources selected, Optional<KafkaSealedBookKeeperDescriptorV2> previous) {
        scope.requireDescriptor(selected.descriptor(), previous);
        var selector = selected.selector();
        var predecessor = new KafkaOwnerAdmissionV1.Previous(archive.admission().owner().ownerEpoch(),
                archive.authority().valueSha256(), archive.admission().tail());
        if (!value.key().equals(scope.key()) || !scope.owner().equals(owner.admission().owner())
                || !scope.nativeScopeSha256().equals(owner.admission().scopeSha256())
                || !scope.ownerFact().key().equals(owner.authority().key()))
            throw new IllegalStateException("native cold reader scope differs from its actual Owner facts");
        // The scope retains the revision seen at installation. Bind its exact immutable closure bytes to
        // the newly read archive; the returned cut pins and rereads that archive's current backend revision.
        if (!scope.closedOwnerFact().key().equals(archive.authority().key())
                || !scope.closedOwnerFact().valueSha256().equals(archive.authority().valueSha256()))
            throw new IllegalStateException("native cold reader scope differs from its exact closed Owner bytes");
        if (!archive.admission().closed() || !scope.predecessor().equals(archive.admission().owner())
                || !archive.admission().scopeSha256().equals(scope.nativeScopeSha256())
                || !owner.admission().previous().equals(Optional.of(predecessor)))
            throw new IllegalStateException("native cold reader lacks its exact predecessor adoption closure");
        if (scope.mode() != SelectorMode.PREFERRED_ONLY || scope.readEpoch() != selector.readAdmissionEpoch()
                || scope.sourceGeneration() != selector.sourceGeneration()
                || !scope.selectorCoreSha256().equals(M4ReadControlCodecV1.selectorCoreSha256(selector)))
            throw new IllegalStateException("native cold reader scope differs from its installed selector");
    }

    private static void requireCurrentReaderFacts(ReaderFacts readers, OxiaKafkaBookKeeperM4ReadRouteV2 route,
            OxiaKafkaRunRootAuthorityV2 roots) {
        for (var entry : readers.scopes().entrySet()) {
            if (!route.compactionRecordFacts().getVersioned(entry.getKey()).toCompletableFuture().join().equals(entry.getValue()))
                throw new IllegalStateException("native reader scope changed during fact capture");
        }
        readers.closedOwner().ifPresent(archive -> {
            if (!roots.readClosedOwnerFact(archive.admission().owner().ownerEpoch()).toCompletableFuture().join().equals(Optional.of(archive)))
                throw new IllegalStateException("native reader closed Owner changed during fact capture");
        });
        if (!roots.readOwnerAdmissionFact().toCompletableFuture().join().equals(Optional.of(readers.owner())))
            throw new IllegalStateException("native reader Owner changed during fact capture");
    }

    static TaskFacts taskFacts(OxiaKafkaBookKeeperM4ReadRouteV2 route, Sha256Digest protocolCell,
            M5BookKeeperNativeCreateClientV2 nativeClient) {
        var admission = route.readCompactionAdmissionFact(protocolCell).orElseThrow(
                () -> new IllegalStateException("native Task Cell directory is missing"));
        var state = KafkaCompactionAdmissionV1.decode(admission.canonicalStoredBytes());
        var transport = route.compactionRecordFacts();
        var tasks = new ArrayList<TaskFact>();
        long scannedBytes = admission.canonicalStoredBytes().length();
        for (var reservation : state.reservations()) {
            var inventory = transport.getVersioned(KafkaBookKeeperInventoryV2.taskKey(reservation.compactionTaskIdSha256()))
                    .toCompletableFuture().join();
            var observed = observeTask(reservation, inventory, route, nativeClient);
            scannedBytes += observed.records().stream().mapToLong(value -> value.canonicalStoredBytes().length()).sum();
            scannedBytes += observed.nativeScope().map(value -> (long) value.instanceIdentity().rawStoredBytes().length()
                    + value.taskFence().rawStoredBytes().length()).orElse(0L);
            if (scannedBytes > com.nereusstream.storage.object.retention.M5RetentionRecordsV1.MAX_SCAN_BYTES)
                throw new IllegalStateException("native Task directory exceeds the retained fact byte budget");
            tasks.add(observed);
        }
        return new TaskFacts(admission, tasks);
    }

    private static TaskFact observeTask(KafkaCompactionAdmissionV1.Reservation reservation, Optional<VersionedValue> inventory,
            OxiaKafkaBookKeeperM4ReadRouteV2 route, M5BookKeeperNativeCreateClientV2 nativeClient) {
        if (inventory.isEmpty()) return new TaskFact(reservation, inventory, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        var task = KafkaBookKeeperInventoryCodecV2.decodeTask(inventory.orElseThrow().canonicalStoredBytes());
        var binding = M5MaterializationCodecV1.decodeSourceCut(task.sourceCut()).identity().binding();
        var decision = route.readTaskDecisionFact(binding, task.taskIdSha256()).toCompletableFuture().join();
        var terminal = route.compactionRecordFacts().getVersioned(KafkaBookKeeperTaskTerminalV2.key(task.taskIdSha256())).toCompletableFuture().join();
        var selectedOutput = decision.flatMap(value -> M5TaskSelectionDecisionV2.decode(value.canonicalStoredBytes()).selectedOutput())
                .map(identity -> route.compactionRecordFacts().getVersioned("v2/kafka-bk-compaction-v2/views/" + identity.toHex() + "/descriptor")
                        .toCompletableFuture().join().orElseThrow());
        Optional<M5BookKeeperNativeCreateClientV2.TaskCreateScopeFact> nativeScope = Optional.empty();
        if (decision.isPresent() && (M5TaskSelectionDecisionV2.decode(decision.orElseThrow().canonicalStoredBytes()).outcome()
                == M5TaskSelectionDecisionV2.Outcome.SELECTED || terminal.isPresent()))
            nativeScope = Optional.of(nativeClient.readTaskCreateScopeFact(taskCreateScope(task, nativeClient.spec().nativeInstanceId()))
                    .toCompletableFuture().join());
        return new TaskFact(reservation, inventory, decision, terminal, selectedOutput, nativeScope);
    }

    static void requireCurrentTaskFacts(TaskFacts tasks, OxiaKafkaBookKeeperM4ReadRouteV2 route,
            Sha256Digest protocolCell, M5BookKeeperNativeCreateClientV2 nativeClient) {
        var transport = route.compactionRecordFacts();
        for (var task : tasks.reservations()) {
            var key = KafkaBookKeeperInventoryV2.taskKey(task.reservation().compactionTaskIdSha256());
            if (!transport.getVersioned(key).toCompletableFuture().join().equals(task.inventory()))
                throw new IllegalStateException("native Task inventory changed during fact capture");
            if (task.inventory().isPresent()) {
                var inventory = KafkaBookKeeperInventoryCodecV2.decodeTask(task.inventory().orElseThrow().canonicalStoredBytes());
                var binding = M5MaterializationCodecV1.decodeSourceCut(inventory.sourceCut()).identity().binding();
                if (!route.readTaskDecisionFact(binding, inventory.taskIdSha256()).toCompletableFuture().join().equals(task.decision()))
                    throw new IllegalStateException("native Task decision changed during fact capture");
                if (!transport.getVersioned(KafkaBookKeeperTaskTerminalV2.key(inventory.taskIdSha256())).toCompletableFuture().join().equals(task.terminal()))
                    throw new IllegalStateException("native Task cancellation terminal changed during fact capture");
                task.selectedOutput().ifPresent(output -> {
                    if (!transport.getVersioned(output.key()).toCompletableFuture().join().equals(Optional.of(output)))
                        throw new IllegalStateException("native Task selected output changed during fact capture");
                });
                task.nativeScope().ifPresent(scope -> {
                    if (!nativeClient.readTaskCreateScopeFact(scope.createScope()).toCompletableFuture().join().equals(scope))
                        throw new IllegalStateException("native Task create scope changed during fact capture");
                });
            }
        }
        if (!route.readCompactionAdmissionFact(protocolCell).equals(Optional.of(tasks.admission())))
            throw new IllegalStateException("native Task Cell directory changed during fact capture");
    }

    private static java.util.Map<PhysicalResourceIdV2, OxiaQuotaTargetDeleteStoreV2.ResourceFact> physicalFacts(
            List<KafkaBookKeeperReplacementSemanticsV2.SourceEvidence> sources, OxiaQuotaTargetDeleteStoreV2 route, boolean sourceScan) {
        var result = new java.util.TreeMap<PhysicalResourceIdV2, OxiaQuotaTargetDeleteStoreV2.ResourceFact>();
        for (var source : sources) {
            var fact = route.readResourceFact(source.resource()).toCompletableFuture().join();
            var authority = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(fact.authority().canonicalStoredBytes());
            if ((!sourceScan && authority.state() != com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1.OPEN_V1)
                    || !authority.activeWriterTickets().isEmpty())
                throw new IllegalStateException("native replacement resource has fenced admission or active physical tickets");
            result.put(source.resource(), fact);
        }
        return result;
    }

    static void requireCurrentPhysicalFacts(
            java.util.Map<PhysicalResourceIdV2, OxiaQuotaTargetDeleteStoreV2.ResourceFact> facts, OxiaQuotaTargetDeleteStoreV2 route) {
        for (var fact : facts.values()) {
            if (!route.readResourceFact(fact.resource()).toCompletableFuture().join().equals(fact))
                throw new IllegalStateException("native replacement physical resource changed during fact capture");
        }
    }

    /** Closure is scoped to the enrolled native Cell/Binding and its exact empty input ticket cut. */
    private static java.util.Map<PhysicalResourceIdV2, List<ReferenceObservationV1>> referenceFacts(
            List<KafkaBookKeeperReplacementSemanticsV2.SourceEvidence> sources, List<M4ReleaseBindingV1> releases,
            VersionedValue selector, TaskFacts tasks, ReaderFacts readers, List<VersionedValue> quiescence,
            KafkaBookKeeperPartitionV1.NativeInputSources recovery) {
        var result = new java.util.TreeMap<PhysicalResourceIdV2, List<ReferenceObservationV1>>();
        for (var source : sources) {
            var release = releases.stream().filter(value -> value.sourceIdentitySha256().equals(source.sourceIdentity())).findFirst().orElseThrow();
            var rows = new ArrayList<ReferenceObservationV1>();
            rows.add(reference(source, ReferenceKindV1.MANIFEST_SELECTED, fact(selector)));
            rows.add(reference(source, ReferenceKindV1.MANIFEST_FALLBACK, fact(selector)));
            rows.add(reference(source, ReferenceKindV1.SOURCE_PROTECTION, release.protectionAuthority()));
            rows.addAll(readerReferences(source, selector, readers, quiescence));
            rows.addAll(protocolReferences(source, selector, readers, ReferenceKindV1.RECOVERY_CHECKPOINT_OR_SNAPSHOT,
                    com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.SemanticAspect.RECOVERY, readers.closed()));
            boolean protocolClosed = readers.closed() && recovery.protocol().speculativeQueue().commits().isEmpty();
            rows.addAll(protocolReferences(source, selector, readers, ReferenceKindV1.KAFKA_PRODUCER_RECOVERY,
                    com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.SemanticAspect.PRODUCER_STATE, protocolClosed));
            rows.addAll(protocolReferences(source, selector, readers, ReferenceKindV1.KAFKA_TRANSACTION_OR_ABORTED_RECOVERY,
                    com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.SemanticAspect.TRANSACTIONS, protocolClosed));
            rows.addAll(protocolReferences(source, selector, readers, ReferenceKindV1.KAFKA_REPLICA_OR_LEADER_EPOCH_RECOVERY,
                    com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.SemanticAspect.LEADER_EPOCH, protocolClosed));
            boolean closed = tasks.reservations().stream().allMatch(TaskFact::closed);
            rows.add(taskReference(source, tasks.admission(), closed));
            for (var task : tasks.reservations())
                for (var observed : task.records()) rows.add(taskReference(source, observed, closed));
            result.put(source.resource(), List.copyOf(rows));
        }
        return java.util.Map.copyOf(result);
    }

    private static List<ReferenceObservationV1> protocolReferences(KafkaBookKeeperReplacementSemanticsV2.SourceEvidence source,
            VersionedValue selector, ReaderFacts readers, ReferenceKindV1 kind,
            com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.SemanticAspect aspect, boolean closed) {
        var facts = new ArrayList<AuthorityFactV1>();
        facts.add(fact(selector));
        source.transfers().stream().filter(transfer -> transfer.aspect() == aspect)
                .forEach(transfer -> facts.add(transfer.verifierAuthority()));
        facts.add(readers.owner().authority());
        readers.closedOwner().ifPresent(owner -> facts.add(owner.authority()));
        readers.records().forEach(value -> facts.add(fact(value)));
        return facts.stream().distinct().map(authority -> new ReferenceObservationV1(kind,
                authority, source.resource().sha256(), source.coverage(),
                closed ? ReferenceDispositionV1.ABSENT : ReferenceDispositionV1.PRESENT, closed)).toList();
    }

    private static List<ReferenceObservationV1> readerReferences(KafkaBookKeeperReplacementSemanticsV2.SourceEvidence source,
            VersionedValue selector, ReaderFacts readers, List<VersionedValue> quiescence) {
        var facts = new ArrayList<AuthorityFactV1>();
        facts.add(fact(selector));
        facts.add(readers.owner().authority());
        readers.closedOwner().ifPresent(owner -> facts.add(owner.authority()));
        readers.records().forEach(value -> facts.add(fact(value)));
        quiescence.forEach(value -> facts.add(fact(value)));
        return facts.stream().distinct().map(authority -> new ReferenceObservationV1(ReferenceKindV1.READ_GENERATION_PIN_OR_OPEN_HANDLE,
                authority, source.resource().sha256(), source.coverage(),
                readers.closed() ? ReferenceDispositionV1.ABSENT : ReferenceDispositionV1.PRESENT, readers.closed())).toList();
    }

    private static ReferenceObservationV1 taskReference(KafkaBookKeeperReplacementSemanticsV2.SourceEvidence source,
            VersionedValue observed, boolean closed) {
        return new ReferenceObservationV1(ReferenceKindV1.MATERIALIZATION_OR_COMPACTION_TASK, fact(observed), source.resource().sha256(),
                source.coverage(), closed ? ReferenceDispositionV1.ABSENT : ReferenceDispositionV1.PRESENT, closed);
    }

    private static ReferenceObservationV1 reference(KafkaBookKeeperReplacementSemanticsV2.SourceEvidence source,
            ReferenceKindV1 kind, AuthorityFactV1 authority) {
        return new ReferenceObservationV1(kind, authority, source.resource().sha256(), source.coverage(), ReferenceDispositionV1.ABSENT, true);
    }

    static VersionedValue record(com.nereusstream.metadata.oxia.v2.compaction.OxiaKafkaBookKeeperRecordStoreV2 transport,
            String key, List<VersionedValue> records) {
        var value = transport.getVersioned(key).toCompletableFuture().join().orElseThrow();
        records.add(value);
        return value;
    }

    private static AuthorityFactV1 fact(VersionedValue value) {
        return new AuthorityFactV1(value.key(), value.metadataVersion(), value.canonicalStoredSha256());
    }

    static boolean selectedTicketCleanup(Throwable failure) {
        return failure instanceof KafkaBookKeeperReadOwnerV2.TicketCleanupException;
    }

    static CompletionStage<List<PhysicalResourceIdV2>> reconcileSelectedTickets(Throwable failure) {
        if (!(failure instanceof KafkaBookKeeperReadOwnerV2.TicketCleanupException cleanup))
            throw new IllegalArgumentException("native selected input lacks a known terminal cleanup");
        return cleanup.reconcileTickets();
    }

    static CompletionStage<Void> reconcileSelectedTickets(Set<IllegalStateException> tickets) {
        CompletionStage<Void> work = CompletableFuture.completedFuture(null);
        for (var ticket : List.copyOf(tickets)) {
            work = work.thenCompose(ignored -> reconcileSelectedTickets(ticket).thenAccept(left -> {
                if (!left.isEmpty()) throw ticket;
                tickets.remove(ticket);
            }));
        }
        return work;
    }

    static KafkaBookKeeperNativeRootVerifierV2 rootVerifier(M5BookKeeperNativeCreateClientV2 source,
            KafkaBookKeeperNativeRootVerifierV2.ReadOwner reader, Supplier<RecoveredBirth> recovered) {
        return new KafkaBookKeeperNativeRootVerifierV2(source, new KafkaBookKeeperNativeRootVerifierV2.ReadOwner() {
            @Override
            public CompletionStage<Void> read(KafkaRunRootRecordV2 record, int maximumHandles,
                    Function<RealBookKeeperCellSessionV1, CompletionStage<Void>> verification) {
                return reader.read(record, maximumHandles, verification);
            }

            @Override
            public CompletionStage<Boolean> permitsRetiredSuccessor(KafkaRunRootRecordV2 parent,
                    KafkaRunRootSnapshotV1 successor) {
                var birth = recovered.get();
                return birth == null ? CompletableFuture.completedFuture(false) : birth.permits(parent, successor);
            }
        });
    }

    /** Captured only after actual native SDK selected recovery and exact closed-archive observation. */
    static RecoveredBirth recoveredBirth(KafkaBookKeeperReadViewV2 view, BindingReadSelector selector,
            OxiaKafkaRunRootAuthorityV2.OwnerAdmissionFact closed, KafkaOwnerIdentityV1 owner,
            OxiaKafkaRunRootAuthorityV2 roots, OxiaKafkaBookKeeperM4ReadRouteV2 route,
            BooleanSupplier current, BooleanSupplier opening) {
        return new RecoveredBirth(view, selector, closed, owner, roots, route, current, opening);
    }

    static final class RecoveredBirth {
        private final KafkaBookKeeperReadViewV2 view;
        private final BindingReadSelector selector;
        private final OxiaKafkaRunRootAuthorityV2.OwnerAdmissionFact closed;
        private final KafkaOwnerIdentityV1 owner;
        private final OxiaKafkaRunRootAuthorityV2 roots;
        private final OxiaKafkaBookKeeperM4ReadRouteV2 route;
        private final BooleanSupplier current;
        private final BooleanSupplier opening;

        private RecoveredBirth(KafkaBookKeeperReadViewV2 view, BindingReadSelector selector,
                OxiaKafkaRunRootAuthorityV2.OwnerAdmissionFact closed, KafkaOwnerIdentityV1 owner,
                OxiaKafkaRunRootAuthorityV2 roots, OxiaKafkaBookKeeperM4ReadRouteV2 route,
                BooleanSupplier current, BooleanSupplier opening) {
            this.view = view;
            this.selector = selector;
            this.closed = closed;
            this.owner = owner;
            this.roots = roots;
            this.route = route;
            this.current = current;
            this.opening = opening;
        }

        void requireSelection() {
            if (!current.getAsBoolean() || !route.readSelector().equals(Optional.of(selector)))
                throw new IllegalStateException("native selected recovery changed before source activation");
        }

        private CompletionStage<Boolean> permits(KafkaRunRootRecordV2 parent, KafkaRunRootSnapshotV1 successor) {
            if (!opening.getAsBoolean() || !current.getAsBoolean() || !matchesBirth(parent, successor))
                return CompletableFuture.completedFuture(false);
            return roots.readOwnerAdmissionFact().thenCompose(before -> {
                if (before.filter(this::matchesOwner).isEmpty()) return CompletableFuture.completedFuture(false);
                return requireArchiveAndSelection(before);
            });
        }

        private boolean matchesOwner(OxiaKafkaRunRootAuthorityV2.OwnerAdmissionFact fact) {
            var admitted = fact.admission();
            var previous = new KafkaOwnerAdmissionV1.Previous(closed.admission().owner().ownerEpoch(),
                    Sha256Digest.hash(closed.admission().encode()), closed.admission().tail());
            return !admitted.closed() && admitted.owner().equals(owner) && admitted.runs().isEmpty()
                    && admitted.previous().equals(Optional.of(previous));
        }

        private CompletionStage<Boolean> requireArchiveAndSelection(
                Optional<OxiaKafkaRunRootAuthorityV2.OwnerAdmissionFact> before) {
            return roots.readClosedOwnerFact(closed.admission().owner().ownerEpoch()).thenCompose(archive -> {
                if (!archive.equals(Optional.of(closed))) return CompletableFuture.completedFuture(false);
                return route.readOpenSelectorAsync().thenCompose(observed -> {
                    if (!observed.equals(Optional.of(selector)) || !current.getAsBoolean())
                        return CompletableFuture.completedFuture(false);
                    return roots.readOwnerAdmissionFact().thenApply(after -> after.equals(before)
                            && current.getAsBoolean() && opening.getAsBoolean());
                });
            });
        }

        private boolean matchesBirth(KafkaRunRootRecordV2 parent, KafkaRunRootSnapshotV1 successor) {
            if (!parent.retired() || selector.mode() != SelectorMode.PREFERRED_ONLY
                    || !closed.admission().tail().equals(Optional.of(parent.initialLink()))) return false;
            long end = view.descriptor().sourceCut().coverage().exclusiveEnd();
            return parent.root().kafkaEndOffsetExclusive().orElseThrow() == end && successor.kafkaStartOffset() == end
                    && successor.creatorOwnerEpoch() == owner.ownerEpoch()
                    && successor.kafkaLeaderEpoch() == owner.kafkaLeaderEpoch();
        }
    }


    static void persist(OxiaKafkaBookKeeperM4ReadRouteV2 route, int partition,
            KafkaBookKeeperNativeFallbackRetirementV2.Result result, OxiaKafkaRunRootAuthorityV2.OwnerAdmissionFact owner) {
        var descriptor = result.transfer().original().descriptor();
        var metadata = new KafkaBookKeeperControlMetadataStoreV2(route.compactionRecords(), route.rawBindingControl(),
                partition, result.selector().binding(), descriptor.task().namespace(), descriptor.task().capability().providerScopeId());
        if (!route.readSelector().filter(value -> value.equals(result.selector())).isPresent())
            throw new IllegalStateException("native read scope selector changed before evidence persistence");
        var scope = KafkaBookKeeperNativeReadScopeV2.verify(result, owner.admission(), owner.authority());
        var bytes = scope.encode();
        var outcome = metadata.putIfAbsent(scope.key(), bytes);
        if (outcome != ControlMutationOutcome.APPLIED)
            throw new IllegalStateException("native read scope publication is unresolved: " + outcome);
        if (!metadata.get(scope.key()).filter(bytes::equals).isPresent())
            throw new IllegalStateException("native read scope publication lacks its exact authoritative bytes");
    }

    /** Runs during actual selected cold recovery, before the Opened owner can become request-ready. */
    static void persistRecovery(OxiaKafkaBookKeeperM4ReadRouteV2 route, int partition,
            OxiaKafkaRunRootAuthorityV2 roots, KafkaBookKeeperPartitionV1 storage,
            Optional<com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperClosedHistoryRecoveryV1.SelectedPrefix> selected) {
        if (selected.isEmpty()) return;
        var selector = route.readSelector().orElseThrow();
        var observation = storage.nativeRecoveryReadScope(selector);
        var current = roots.readOwnerAdmissionFact().toCompletableFuture().join().orElseThrow();
        var closed = roots.readClosedOwnerFact(observation.predecessor().owner().ownerEpoch())
                .toCompletableFuture().join().orElseThrow();
        var scope = KafkaBookKeeperNativeRecoveryScopeV2.verify(observation,
                current.admission(), current.authority(), closed.admission(), closed.authority(), selected.orElseThrow().predecessorDescriptor());
        var descriptor = observation.descriptor();
        var metadata = new KafkaBookKeeperControlMetadataStoreV2(route.compactionRecords(), route.rawBindingControl(),
                partition, selector.binding(), descriptor.task().namespace(), descriptor.task().capability().providerScopeId());
        var bytes = scope.encode();
        if (metadata.putIfAbsent(scope.key(), bytes) != ControlMutationOutcome.APPLIED
                || !metadata.get(scope.key()).filter(bytes::equals).isPresent())
            throw new IllegalStateException("native recovery scope publication is unresolved");
        storage.requireNativeRecoveryReadScope(observation);
        if (!route.readSelector().filter(selector::equals).isPresent()
                || !roots.readOwnerAdmissionFact().toCompletableFuture().join().filter(current::equals).isPresent()
                || !roots.readClosedOwnerFact(closed.admission().owner().ownerEpoch())
                        .toCompletableFuture().join().filter(closed::equals).isPresent())
            throw new IllegalStateException("native recovery scope Owner/selector facts changed during persistence");
    }
}
