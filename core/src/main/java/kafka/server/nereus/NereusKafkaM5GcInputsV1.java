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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2.NativeRecordFact;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5KafkaBindingPolicyV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/** Actual cancelled or replaced resources and complete admitted native facts under one owned source scan. */
final class NereusKafkaM5GcInputsV1 {
    private final NereusKafkaM5SourceScanV1 scan;
    private final NereusKafkaUnpublishedScopeV2.Evidence unpublished;
    private final NereusKafkaReadScopeV2.ReplacementEvidence replacement;
    private final NereusKafkaNativeInputV2.NativeCut nativeCut;
    private final NereusKafkaReadScopeV2.TaskFacts tasks;
    private final M5KafkaBindingPolicyV1 policy;
    private final VersionedValue protocol;
    private final Map<String, Supplier<CompletionStage<Optional<VersionedValue>>>> readers = new TreeMap<>();
    private final Map<String, VersionedValue> retained = new TreeMap<>();
    private final List<PhysicalResourceIdV2> publicResources;
    private long retainedBytes;

    private NereusKafkaM5GcInputsV1(NereusKafkaM5SourceScanV1 scan,
            NereusKafkaUnpublishedScopeV2.Evidence unpublished, NereusKafkaReadScopeV2.ReplacementEvidence replacement) {
        this.scan = scan;
        this.unpublished = unpublished;
        this.replacement = replacement;
        nativeCut = scan.opened().nativeM5InputCut();
        var authority = M5BindingAuthorityCodecV1.decodeAuthority(scan.exactAuthority().canonicalStoredBytes());
        policy = authority.kafkaPolicy().orElseThrow();
        if (!authority.writerEnrollment().isPresent() || authority.bootstrap().isEmpty())
            throw new IllegalStateException("native cleanup lacks its first admitted writer inventory");
        tasks = isReplacement() ? replacement.tasks()
                : NereusKafkaReadScopeV2.taskFacts(scan.route(), scan.opened().nativeM5ProtocolCell(), scan.nativeClient());
        var currentProtocol = isReplacement() ? replacement.recovery() : nativeCut.inputs();
        requireProtocolBounds(currentProtocol);
        publicResources = publicResources(currentProtocol);
        protocol = protocolValue(currentProtocol);
        retain(scan.exactAuthority(), () -> scan.route().readBindingAuthority());
        var owner = scan.owner();
        var nativeOwner = VersionedValue.of(owner.authority().key(), owner.admission().encode(), owner.authority().metadataVersion());
        retain(nativeOwner, () -> scan.physical().read(nativeOwner.key()));
        retain(protocol, () -> scan.requireCurrent().thenApply(ignored -> Optional.of(protocolValue(currentProtocol()))));
        if (isReplacement()) retainReplacement();
        else retainUnpublished();
        retainTaskFacts();
        retainPhysicalFacts(authority.binding());
        NereusKafkaReadScopeV2.requireCurrentTaskFacts(tasks, scan.route(), scan.opened().nativeM5ProtocolCell(), scan.nativeClient());
    }

    private void retainTaskFacts() {
        retain(tasks.admission(), () -> scan.observe(() -> scan.route().readCompactionAdmissionFact(scan.opened().nativeM5ProtocolCell())));
        for (var task : tasks.reservations()) {
            for (var value : task.records()) {
                if (task.decision().filter(value::equals).isPresent()) {
                    var binding = task.inventory().map(row -> com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryCodecV2
                            .decodeTask(row.canonicalStoredBytes())).map(row -> M5MaterializationCodecV1.decodeSourceCut(row.sourceCut()).identity().binding()).orElseThrow();
                    retain(value, () -> scan.route().readTaskDecisionFact(binding, task.reservation().compactionTaskIdSha256()));
                } else retain(value, () -> scan.route().compactionRecordFacts().getVersioned(value.key()));
            }
            task.nativeScope().ifPresent(scope -> {
                retainNative(scope.instanceIdentity(), () -> scan.nativeClient().readTaskCreateScopeFact(scope.createScope())
                        .thenApply(value -> value.instanceIdentity()));
                retainNative(scope.taskFence(), () -> scan.nativeClient().readTaskCreateScopeFact(scope.createScope())
                        .thenApply(value -> value.taskFence()));
            });
        }
    }

    private void retainPhysicalFacts(com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity binding) {
        for (var resource : resources()) {
            var scope = scan.nativeClient().readFencedLedgerScopeFacts(createScope(resource), handle(resource)).toCompletableFuture().join();
            retainNative(scope.scope().instanceIdentity(), () -> scan.nativeClient().readTaskCreateScopeFact(createScope(resource))
                    .thenApply(value -> value.instanceIdentity()));
            retainNative(scope.scope().taskFence(), () -> scan.nativeClient().readTaskCreateScopeFact(createScope(resource))
                    .thenApply(value -> value.taskFence()));
            retainNative(scope.reservation(), () -> scan.nativeClient().readFencedLedgerScopeFacts(createScope(resource), handle(resource))
                    .thenApply(value -> value.reservation()));
        }
        for (var member : members().values()) {
            var value = VersionedValue.of(member.authority().key(), member.canonicalStoredBytes(), member.authority().metadataVersion());
            retain(value, () -> scan.physical().read(value.key()));
            if (member.inventory().members().size() != 1 || member.inventory().members().stream().anyMatch(row ->
                    !row.binding().equals(binding)
                            || !row.providerScope().equals(nativeCut.inputs().sources().activeRoot().providerScopeId().digest())))
                throw new IllegalStateException("native cleanup requires every actual physical member's scope");
        }
    }

    static CompletionStage<NereusKafkaM5GcInputsV1> capture(NereusKafkaM5SourceScanV1 scan, Sha256Digest taskId) {
        return scan.captureUnpublished(taskId).thenCompose(value -> scan.observe(() -> new NereusKafkaM5GcInputsV1(scan, value, null)));
    }

    static CompletionStage<NereusKafkaM5GcInputsV1> captureReplacement(NereusKafkaM5SourceScanV1 scan) {
        return scan.captureReplacement().thenCompose(value -> scan.observe(() -> new NereusKafkaM5GcInputsV1(scan, null, value)));
    }

    boolean isReplacement() {
        return replacement != null;
    }

    NereusKafkaReadScopeV2.ReplacementEvidence replacement() {
        return java.util.Objects.requireNonNull(replacement);
    }

    Sha256Digest scopeIdentity() {
        return isReplacement() ? replacement.selected().descriptor().task().taskIdSha256() : unpublished.terminal().task().taskIdSha256();
    }

    java.util.Set<PhysicalResourceIdV2> resources() {
        return isReplacement() ? replacement.sealed().keySet() : unpublished.sealed().keySet();
    }

    List<VersionedValue> completed() {
        return isReplacement() ? replacement.completed() : unpublished.completed();
    }

    com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1 handle(PhysicalResourceIdV2 resource) {
        return isReplacement() ? replacement.sealed().get(resource).handle() : unpublished.sealed().get(resource).handle();
    }

    com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateSpecV2 createScope(PhysicalResourceIdV2 resource) {
        return isReplacement() ? replacement.sealed().get(resource).createScope() : unpublished.sealed().get(resource).createScope();
    }

    Map<PhysicalResourceIdV2, com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Fact> members() {
        return isReplacement() ? replacement.members() : unpublished.members();
    }

    VersionedValue lifecycleAuthority() {
        return isReplacement() ? replacement.nativeDescriptor() : unpublished.nativeTerminal();
    }

    VersionedValue manifestAuthority() {
        return isReplacement() ? replacement.nativeDescriptor() : unpublished.nativeDecision();
    }

    com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage coverage(PhysicalResourceIdV2 resource) {
        return isReplacement() ? replacement.sources().stream().filter(value -> value.resource().equals(resource))
                .findFirst().orElseThrow().coverage() : sourceCut().coverage();
    }

    List<M5RetentionRecordsV1.M4ReleaseBindingV1> releases(PhysicalResourceIdV2 resource) {
        if (!isReplacement()) return List.of();
        var source = replacement.sources().stream().filter(value -> value.resource().equals(resource)).findFirst().orElseThrow();
        return replacement.releases().stream().filter(value -> value.sourceIdentitySha256().equals(source.sourceIdentity())).toList();
    }

    com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.ReclamationReason reason() {
        return isReplacement() ? com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.ReclamationReason.REPLACED_REPRESENTATION
                : com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.ReclamationReason.UNPUBLISHED_ARTIFACT;
    }

    com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.MemberSnapshot member(PhysicalResourceIdV2 resource,
            M5RetentionRecordsV1.RetentionFloorSnapshotV1 floors, M5RetentionRecordsV1.ReferenceFreeProofV1 proof) {
        return new com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.MemberSnapshot(floors, proof, fact(sourceAuthority()),
                isReplacement() ? com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.NativeDisposition.REPLACEMENT_ALLOWED
                        : com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.NativeDisposition.UNPUBLISHED_CLEANUP_ALLOWED,
                isReplacement() ? Optional.of(replacement.replacementTaskEvidence(resource)) : Optional.empty(), Optional.empty(),
                isReplacement() ? Optional.empty() : Optional.of(unpublished.unpublishedTaskEvidence(resource)));
    }

    private KafkaBookKeeperPartitionV1.NativeInputSources currentProtocol() {
        var current = scan.opened().storage().nativeInputSources();
        return isReplacement() ? scan.opened().storage().nativeSelectedInputSources(current,
                replacement.selector().selector()).orElseThrow().recoveryInputs() : current;
    }

    private void retainUnpublished() {
        for (var value : unpublished.records())
            retain(value, () -> scan.route().compactionRecordFacts().getVersioned(value.key()));
        retain(unpublished.nativeTerminal(), () -> scan.physical().read(unpublished.nativeTerminal().key()));
        retain(unpublished.nativeDecision(), () -> scan.physical().read(unpublished.nativeDecision().key()));
    }

    private void retainReplacement() {
        var transport = scan.route().compactionRecordFacts();
        for (var value : replacement.records()) retain(value, () -> transport.getVersioned(value.key()));
        for (var value : replacement.readers().records()) retain(value, () -> transport.getVersioned(value.key()));
        for (var value : replacement.quiescence()) retain(value, () -> scan.readLifecycleFact(value.key()));
        var nativeFacts = new ArrayList<VersionedValue>();
        nativeFacts.add(replacement.nativeDescriptor());
        nativeFacts.add(replacement.nativeSemantics());
        replacement.readers().closedOwner().ifPresent(owner -> nativeFacts.add(VersionedValue.of(
                owner.authority().key(), owner.admission().encode(), owner.authority().metadataVersion())));
        replacement.sealed().values().forEach(value -> value.rawRoot().ifPresent(root -> nativeFacts.add(VersionedValue.of(
                root.authority().key(), root.record().encode(), root.authority().metadataVersion()))));
        nativeFacts.addAll(replacement.completed());
        for (var value : nativeFacts) retain(value, () -> scan.physical().read(value.key()));
    }

    NereusKafkaUnpublishedScopeV2.Evidence unpublished() {
        return unpublished;
    }

    M5KafkaBindingPolicyV1 policy() {
        return policy;
    }

    VersionedValue protocol() {
        return protocol;
    }

    com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationSourceCut sourceCut() {
        return isReplacement() ? replacement.selected().descriptor().sourceCut()
                : M5MaterializationCodecV1.decodeSourceCut(unpublished.terminal().task().sourceCut());
    }

    VersionedValue sourceAuthority() {
        return scan.exactAuthority();
    }

    boolean unresolvedTask() {
        // A preparation without its real native terminal remains a veto, even if unrelated to this target.
        return tasks.reservations().stream().anyMatch(task -> !task.closed());
    }

    List<VersionedValue> facts() {
        return List.copyOf(retained.values());
    }

    boolean hasPublicReference(PhysicalResourceIdV2 resource) {
        if (!resources().contains(resource)) throw new IllegalArgumentException("resource is outside the original physical cut");
        if (publicResources.contains(resource)) return true;
        if (isReplacement()) return false; // Selected recovery obligations use the exact semantic-transfer observations.
        var run = handle(resource).runId();
        return nativeCut.inputs().recoveryCheckpoint().stream().anyMatch(checkpoint ->
                checkpoint.vector().runBinding().runId().equals(run)
                        || checkpoint.readIndexes().stream().anyMatch(index -> index.runId().equals(run)));
    }

    /** Current already published native trim only; physical cleanup does not invent a newer expiry frontier. */
    long retainedNativeFrontier() {
        return nativeCut.inputs().protocol().root().frontiers().trimStartOffset();
    }

    boolean auditHeld(PhysicalResourceIdV2 resource) {
        return policy.auditHolds().stream().anyMatch(hold -> hold.resource().equals(resource));
    }

    ExactMetadataTransactionStoreV1 readOnlyFacts(ExactMetadataTransactionStoreV1 otherFacts) {
        return new ExactMetadataTransactionStoreV1() {
            public CompletionStage<Optional<VersionedValue>> read(String key) {
                var reader = readers.get(key);
                return reader == null ? otherFacts.read(key) : reader.get();
            }

            public CompletionStage<MutationOutcome> compareAndSet(Optional<VersionedValue> previous, String key, CanonicalBytes candidate) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException("native input facts are read only"));
            }

            public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
                return CompletableFuture.completedFuture(TransactionOutcome.UNSUPPORTED);
            }

            public boolean supportsAtomicMultiKeyTransactions() {
                return false;
            }
        };
    }

    private List<PhysicalResourceIdV2> publicResources(KafkaBookKeeperPartitionV1.NativeInputSources inputs) {
        var namespace = policy.physicalBinding().physicalNamespace();
        var result = new ArrayList<PhysicalResourceIdV2>();
        result.add(new PhysicalResourceIdV2.BookKeeperLedger(namespace, inputs.sources().activeRoot().ledgerIdentity().ledgerId()));
        inputs.sources().sealedRoots().forEach(root -> result.add(new PhysicalResourceIdV2.BookKeeperLedger(namespace, root.ledgerIdentity().ledgerId())));
        inputs.protocol().activeTail().locators().forEach(locator -> result.add(new PhysicalResourceIdV2.BookKeeperLedger(namespace, locator.handle().ledgerIdentity().ledgerId())));
        var selector = M5BindingAuthorityCodecV1.decodeAuthority(scan.exactAuthority().canonicalStoredBytes()).selectorProjection();
        var selected = scan.opened().storage().nativeSelectedInputSources(inputs, selector);
        if (selected.isPresent()) {
            var actual = selected.orElseThrow();
            actual.descriptor().sealedParts().forEach(part -> result.add(new PhysicalResourceIdV2.BookKeeperLedger(namespace, part.handle().ledgerIdentity().ledgerId())));
            actual.requireCurrent();
        } else if (!selector.activeBatches().isEmpty() || !selector.selectedViewSha256().equals(inputs.sources().selectedViewSha256())) {
            throw new IllegalStateException("native cleanup cannot resolve its actual selected/fallback sources");
        }
        return result.stream().distinct().sorted().toList();
    }

    private static void requireProtocolBounds(KafkaBookKeeperPartitionV1.NativeInputSources inputs) {
        var protocol = inputs.protocol();
        long rows = (long) protocol.committedProducerState().producers().size()
                + protocol.transactionState().ongoingTransactions().size()
                + protocol.transactionState().completedTransactions().size()
                + protocol.leaderEpochIndex().startOffsets().size()
                + protocol.speculativeQueue().commits().size()
                + protocol.activeTail().locators().size();
        rows += inputs.recoveryCheckpoint().stream().flatMap(value -> value.readIndexes().stream()).mapToLong(value -> value.rows().size()).sum();
        if (rows > M5RetentionRecordsV1.MAX_REFERENCE_ROWS)
            throw new IllegalStateException("native producer/transaction/replica/recovery cut exceeds its scan row bound");
    }

    private VersionedValue protocolValue(KafkaBookKeeperPartitionV1.NativeInputSources inputs) {
        requireProtocolBounds(inputs);
        var root = inputs.protocol().root();
        var refs = root.references();
        var components = List.of(refs.runTable(), refs.activeTail(), refs.sourceMap(), refs.committedProducerState(),
                refs.speculativeProducerQueue(), refs.transactionIndex(), refs.leaderEpochIndex(), refs.checkpointVector(), refs.sourceProtection());
        var bytes = ByteBuffer.allocate(4 + 32 + 8 + 48 + components.size() * 40);
        bytes.putInt(0x4b474331).put(scan.owner().authority().valueSha256().bytes().toByteArray()).putLong(root.stateVersion());
        var frontiers = root.frontiers();
        bytes.putLong(frontiers.trimStartOffset()).putLong(frontiers.allocatedEndOffset()).putLong(frontiers.durableEndOffset())
                .putLong(frontiers.readableEndOffset()).putLong(frontiers.highWatermark()).putLong(frontiers.lastStableOffset());
        components.forEach(value -> bytes.putLong(value.generation()).put(value.contentDigest().bytes().toByteArray()));
        var version = ByteBuffer.allocate(40).put(scan.owner().authority().valueSha256().bytes().toByteArray()).putLong(root.stateVersion());
        return VersionedValue.of("v2/kafka-native-coherent-cut/" + sourceCut().identity().binding().bindingId().digest().toHex(),
                CanonicalBytes.copyOf(bytes.array()), new MetadataVersion(CanonicalBytes.copyOf(version.array())));
    }

    private void retainNative(NativeRecordFact value, Supplier<CompletionStage<NativeRecordFact>> reader) {
        retain(nativeValue(value), () -> reader.get().thenApply(actual -> Optional.of(nativeValue(actual))));
    }

    private static VersionedValue nativeValue(NativeRecordFact value) {
        var version = ByteBuffer.allocate(24).putInt(0x4b475a31).putLong(value.nativeCreateZxid())
                .putLong(value.nativeModifyZxid()).putInt(value.nativeVersion());
        return VersionedValue.of(value.nativePath(), value.rawStoredBytes(), new MetadataVersion(CanonicalBytes.copyOf(version.array())));
    }

    private void retain(VersionedValue value, Supplier<CompletionStage<Optional<VersionedValue>>> reader) {
        var prior = retained.putIfAbsent(value.key(), value);
        if (prior != null && !prior.equals(value)) throw new IllegalStateException("native cleanup captured two revisions of one fact");
        if (prior == null) retainedBytes += value.key().getBytes(java.nio.charset.StandardCharsets.UTF_8).length + (long) value.canonicalStoredBytes().length();
        if (retainedBytes > M5RetentionRecordsV1.MAX_SCAN_BYTES || retained.size() > M5RetentionRecordsV1.MAX_REFERENCE_ROWS)
            throw new IllegalStateException("native cleanup fact workspace exceeds its scan bounds");
        readers.putIfAbsent(value.key(), reader);
    }

    static AuthorityFactV1 fact(VersionedValue value) {
        return new AuthorityFactV1(value.key(), value.metadataVersion(), value.canonicalStoredSha256());
    }
}
