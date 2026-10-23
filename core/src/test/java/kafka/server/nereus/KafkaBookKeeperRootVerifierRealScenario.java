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

import kafka.server.nsip.KafkaBookKeeperOwnerRelocationRealTest;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.nereus.KafkaTopicBindingAggregateMapperV1;
import org.apache.kafka.server.common.MetadataVersion;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlKeysV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState;
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperM4BrokerReadOwnerV2;
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadResultV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadOutcomeV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperSequentialReadRequestV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperClosedHistoryRecoveryV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Real verifier/history reads and native close precede injected response uncertainty across Broker generations. */
public final class KafkaBookKeeperRootVerifierRealScenario {
    private KafkaBookKeeperRootVerifierRealScenario() { }

    static boolean releaseKnownNoTask(boolean registered, NereusKafkaNativeInputV2.Captured captured,
            com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionAdmissionV1 admission,
            java.util.concurrent.ExecutionException failure) throws Exception {
        if (registered || !(failure.getCause() instanceof IllegalStateException)
                || !"native M5 metadata image changed during capture".equals(failure.getCause().getMessage())) return false;
        captured.discard().toCompletableFuture().get(30, TimeUnit.SECONDS);
        assertTrue(admission.read().orElseThrow().reservations().isEmpty(), "fresh preparation retained the previous capture capacity");
        return true;
    }

    static CompletionStage<CompletableFuture<com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome>>
            publicationUntilOutputClose(NereusKafkaPreparedTaskV2 prepared, CompletableFuture<Void> closeSeen) {
        return publicationUntilOutputClose(prepared, closeSeen, System.nanoTime() + TimeUnit.SECONDS.toNanos(30));
    }

    static CompletionStage<com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome>
            publicationUntilKnown(NereusKafkaPreparedTaskV2 prepared) {
        return publicationUntilOutputClose(prepared, new CompletableFuture<>()).thenCompose(attempt -> attempt);
    }

    private static CompletionStage<CompletableFuture<com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome>>
            publicationUntilOutputClose(NereusKafkaPreparedTaskV2 prepared, CompletableFuture<Void> closeSeen, long deadline) {
        if (System.nanoTime() >= deadline)
            return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("same native Task publication remained unknown"));
        var attempt = prepared.publish().toCompletableFuture();
        return CompletableFuture.anyOf(attempt, closeSeen).thenCompose(ignored -> {
            if (closeSeen.isDone() || attempt.getNow(null)
                    != com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome.OUTCOME_UNKNOWN)
                return CompletableFuture.completedFuture(attempt);
            return publicationUntilOutputClose(prepared, closeSeen, deadline);
        });
    }

    static List<com.nereusstream.domain.bytes.CanonicalBytes> selectedBytes(NereusKafkaPreparedTaskV2 prepared, com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperDescriptorV2 descriptor,
            NereusKafkaOwnedProviderRuntime.Opened opened) throws Exception {
        var expected = new java.util.ArrayList<>(prepared.semantic().batchOutputs().stream().flatMap(batch -> batch.outputBody().stream()).toList());
        var tail = opened.storage().read(new KafkaBookKeeperSequentialReadRequestV1(descriptor.sourceCut().coverage().exclusiveEnd(),
                KafkaReadIsolationV1.REPLICA, Long.MAX_VALUE, Optional.empty())).toCompletableFuture().get(30, TimeUnit.SECONDS);
        assertEquals(KafkaBookKeeperReadOutcomeV1.FOUND, tail.outcome(), tail.detail());
        tail.batches().forEach(batch -> expected.add(batch.rawAssignedRecordBatch()));
        return expected;
    }

    static void verifyReleased(io.oxia.client.api.AsyncOxiaClient actual, String nativeTaskKey, BindingReadSelector predecessor,
            com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperNativeFallbackRetirementV2.Result result, BindingReadSelector selected) throws Exception {
        var keys = new M4ReadControlKeysV1(0, predecessor.binding());
        assertEquals(com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode.PREFERRED_ONLY, selected.mode());
        assertEquals(predecessor.selectedViewSha256(), selected.selectedViewSha256());
        assertEquals(predecessor.ownerEpoch(), selected.ownerEpoch());
        assertEquals(predecessor.readAdmissionEpoch() + 1, selected.readAdmissionEpoch());
        assertEquals(predecessor.sourceGeneration() + 1, selected.sourceGeneration());
        assertEquals(result.terminal(), M4ReadControlCodecV1.decodeTerminal(m4Stored(actual, nativeTaskKey, keys.terminal(predecessor.readAdmissionEpoch()))));
        assertEquals(result.proof(), M4ReadControlCodecV1.decodeProof(m4Stored(actual, nativeTaskKey, keys.proof(predecessor.readAdmissionEpoch()))));
        assertEquals(predecessor.ownerEpoch(), result.terminal().ownerEpoch());
        assertEquals(predecessor.sourceGeneration(), result.terminal().lastAdmittedAndDrainedReadViewGeneration());
        assertEquals(predecessor.capability(), result.terminal().capability());
        assertEquals(M4ReadControlCodecV1.terminalSha256(result.terminal()), result.proof().terminalCutSha256());
        var anchor = selected.pendingAnchors().stream()
                .filter(value -> value.closedReadAdmissionEpoch() == predecessor.readAdmissionEpoch()).findFirst().orElseThrow();
        assertEquals(M4ReadControlCodecV1.anchorSha256(anchor), result.terminal().closureAnchorSha256());
        assertTrue(selected.activeBatches().contains(result.batch()));
        var head = Sha256Digest.hash(m4Stored(actual, nativeTaskKey, keys.proofHead()));
        for (var source : result.batch().sources()) {
            var protection = M4ReadControlCodecV1.decodeProtection(m4Stored(actual, nativeTaskKey,
                    keys.protection(source.sourceIdentitySha256(), source.protectionGeneration())));
            assertEquals(source, protection.identity());
            assertEquals(ProtectionState.RELEASED, protection.state());
            assertEquals(Optional.of(result.batch().batchIdSha256()), protection.releasedByBatchSha256());
            assertEquals(Optional.of(head), protection.releaseProofHeadSha256());
        }
    }

    static void verifyProtected(io.oxia.client.api.AsyncOxiaClient actual, String nativeTaskKey, BindingReadSelector closed) throws Exception {
        var keys = new M4ReadControlKeysV1(0, closed.binding());
        assertFalse(closed.activeBatches().isEmpty());
        for (var batch : closed.activeBatches()) {
            for (var source : batch.sources()) {
                var protection = M4ReadControlCodecV1.decodeProtection(m4Stored(actual, nativeTaskKey,
                        keys.protection(source.sourceIdentitySha256(), source.protectionGeneration())));
                assertEquals(ProtectionState.PROTECTED, protection.state());
            }
        }
    }

    private static CanonicalBytes m4Stored(io.oxia.client.api.AsyncOxiaClient actual, String nativeTaskKey, String key) throws Exception {
        String root = nativeTaskKey.substring(0, nativeTaskKey.indexOf("/v2/"));
        return CanonicalBytes.copyOf(actual.get(root + "/" + key).get(30, java.util.concurrent.TimeUnit.SECONDS).value());
    }

    static void changeNativeOwner(io.oxia.client.api.AsyncOxiaClient actual, String key, boolean close) throws Exception {
        assertNotNull(key, "actual native Owner admission key was not observed");
        var before = actual.get(key).get(30, java.util.concurrent.TimeUnit.SECONDS);
        var admission = com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1.decode(CanonicalBytes.copyOf(before.value()));
        assertFalse(admission.closed());
        var changed = close ? admission.close() : admission;
        actual.put(key, changed.encode().toByteArray(), java.util.Set.of(io.oxia.client.api.options.PutOption.IfVersionIdEquals(before.version().versionId())))
                .get(30, java.util.concurrent.TimeUnit.SECONDS);
        var after = actual.get(key).get(30, java.util.concurrent.TimeUnit.SECONDS);
        assertEquals(changed, com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1.decode(CanonicalBytes.copyOf(after.value())));
        assertTrue(after.version().versionId() > before.version().versionId());
    }

    static com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperSelectedCacheBudgetV1 selectedCacheBudget(
            io.oxia.client.api.AsyncOxiaClient actual, String recordRoot,
            com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperDescriptorV2 descriptor) throws Exception {
        String key = com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.taskKey(descriptor.task().taskIdSha256()) + "/cache-budget";
        var budget = com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperSelectedCacheBudgetV1.decode(
                com.nereusstream.domain.bytes.CanonicalBytes.copyOf(actual.get(recordRoot + "/" + key).get(30, java.util.concurrent.TimeUnit.SECONDS).value()));
        budget.requireDescriptor(descriptor);
        return budget;
    }

    static void verifyReplacementSemantics(io.oxia.client.api.AsyncOxiaClient actual, String recordRoot,
            com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperDescriptorV2 descriptor) throws Exception {
        String key = com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperReplacementSemanticsV2.key(descriptor.descriptorSha256());
        var stored = actual.get(recordRoot + "/" + key).get(30, java.util.concurrent.TimeUnit.SECONDS);
        var bytes = com.nereusstream.domain.bytes.CanonicalBytes.copyOf(stored.value());
        var comparison = com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperReplacementSemanticsV2.decode(bytes);
        comparison.requireDescriptor(descriptor);
        var membership = actual.get(recordRoot + "/v2/kafka-bk-compaction-v2/views/" + descriptor.descriptorSha256().toHex()
                + "/publication-targets").get(30, java.util.concurrent.TimeUnit.SECONDS);
        assertEquals(com.nereusstream.domain.bytes.Sha256Digest.hash(com.nereusstream.domain.bytes.CanonicalBytes.copyOf(membership.value())),
                comparison.publicationTargetsSha256(), "semantic comparison lost actual original publication membership");
        var fact = new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1(key,
                new com.nereusstream.metadata.spi.model.MetadataVersion(com.nereusstream.domain.bytes.CanonicalBytes.copyOf(
                        java.nio.ByteBuffer.allocate(Long.BYTES).putLong(stored.version().versionId()).array())),
                com.nereusstream.domain.bytes.Sha256Digest.hash(bytes));
        var transfers = comparison.transfers(descriptor, fact);
        assertEquals(List.of(com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.SemanticAspect.values()),
                transfers.stream().map(com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.SemanticTransfer::aspect).toList());
        for (var transfer : transfers) {
            assertEquals(descriptor.sourceCut().coverage(), transfer.coverage());
            assertEquals(transfer.priorSemanticRoot(), transfer.replacementSemanticRoot());
        }
        var reread = actual.get(recordRoot + "/" + key).get(30, java.util.concurrent.TimeUnit.SECONDS);
        assertEquals(stored.version().versionId(), reread.version().versionId());
        assertEquals(bytes, com.nereusstream.domain.bytes.CanonicalBytes.copyOf(reread.value()));
    }

    static void verifyNativeSourceTransfer(KafkaBookKeeperPartitionV1 storage,
            com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperDescriptorV2 descriptor,
            KafkaBookKeeperPartitionV1.NativeInputSources before) {
        var after = storage.nativeInputSources();
        long end = descriptor.sourceCut().coverage().exclusiveEnd();
        var expectedRoots = before.sources().sealedRoots().stream().filter(root -> root.kafkaStartOffset() >= end).toList();
        assertEquals(expectedRoots, after.sources().sealedRoots(), "hot selected Owner retained its covered raw roots");
        assertEquals(before.sources().sealedReadRuns().stream().filter(run -> run.startOffset() >= end).toList(),
                after.sources().sealedReadRuns(), "hot selected Owner retained its covered read indexes");
        assertEquals(before.sources().activeRoot(), after.sources().activeRoot(), "source transfer replaced the live raw tail");
        var old = before.protocol();
        var current = after.protocol();
        assertEquals(old.root().frontiers(), current.root().frontiers());
        assertEquals(old.root().fence(), current.root().fence());
        assertEquals(old.activeTail(), current.activeTail());
        assertEquals(old.committedProducerState(), current.committedProducerState());
        assertEquals(old.transactionState(), current.transactionState());
        assertEquals(old.leaderEpochIndex(), current.leaderEpochIndex());
        var checkpoint = before.recoveryCheckpoint().orElseThrow();
        assertEquals(old.root().references().activeTail(), current.root().references().activeTail());
        assertTrue(current.root().references().sourceMap().generation() > old.root().references().sourceMap().generation());
        var removed = before.sources().sealedRoots().stream().filter(root -> root.kafkaStartOffset() < end)
                .map(root -> root.runId()).toList();
        // A fresh live tail still has the independently selected checkpoint, even without a remaining raw sealed run.
        var selected = current.root().references().checkpointVector();
        assertTrue(selected.generation() > old.root().references().checkpointVector().generation());
        var next = after.recoveryCheckpoint().orElseThrow();
        assertEquals(checkpoint.components(), next.components());
        assertEquals(checkpoint.vector().recoveryCoveredThrough(), next.vector().recoveryCoveredThrough());
        assertTrue(next.readIndexes().stream().noneMatch(index -> removed.contains(index.runId())));
        assertTrue(!removed.contains(next.vector().runBinding().runId()), "checkpoint still names deleted raw storage");
    }

    public static void run(Optional<Path> receipt, String mode) throws Exception {
        Path inputs = Files.createTempDirectory("nereus-nsip-root-reader-");
        var fault = new RootCloseFault(mode);
        boolean verified = false;
        boolean unknownReported = false;
        try {
            KafkaBookKeeperOwnerRelocationRealTest.writeCapability(inputs, receipt);
            try (var cluster = cluster(inputs, fault)) {
                cluster.format();
                cluster.startup();
                cluster.waitForReadyBrokers();
                try {
                    exercise(cluster, fault);
                    verified = true;
                } finally {
                    // Release held test responses before the cluster's resource-owning close, including assertion failure.
                    if (!fault.unknown()) fault.release.complete(null);
                    fault.controlBlocked.set(false);
                }
            } catch (java.util.concurrent.ExecutionException | org.apache.kafka.server.fault.FaultHandlerException failure) {
                if (!verified) throw failure;
                if (fault.unknown()) {
                    requireFailure(failure, fault.nativeLabel() + " close response lost");
                    unknownReported = true;
                } else if (fault.capacity()) {
                    requireFailure(failure, fault.activationFailure());
                    requireFailure(fault.admissionFailure.get(), fault.capacityLabel.get() + " Cell read capacity is full");
                } else throw failure;
            }
            if (fault.unknown()) assertTrue(unknownReported, "UNKNOWN verifier close must fail source shutdown");
        } finally {
            fault.release.complete(null);
            try (var files = Files.walk(inputs)) {
                for (var file : files.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(file);
            }
        }
    }

    private static KafkaClusterTestKit cluster(Path inputs, RootCloseFault fault) throws Exception {
        var bootstrap = BootstrapMetadata.fromVersion(MetadataVersion.latestTesting(), "nsip-root-reader")
                .copyWithFeatureRecord("nereus.storage.version", (short) 2);
        var nodes = new TestKitNodes.Builder(bootstrap).setNumBrokerNodes(3).setNumControllerNodes(1).build();
        var builder = new KafkaClusterTestKit.Builder(nodes)
                .setControllerStorageRuntimeFactory(new NereusControllerStorageRuntimeFactory())
                .setBrokerStorageRuntimeFactory(context -> {
                    var provider = new NereusKafkaOwnedProviderRuntime(context, fault.observer(context.config()));
                    return new NereusBrokerStorageRuntime(context, provider);
                });
        KafkaBookKeeperOwnerRelocationRealTest.nativeConfig(inputs,
                System.getProperty("nereus.nsip.bkUri"), System.getProperty("nereus.nsip.oxia"))
                .forEach(builder::setConfigProp);
        if (fault.capacity() || fault.fetch()) builder.setConfigProp("nereus.kafka.storage.bookkeeper.max.reads.inflight", "3");
        if (fault.historyPhase() == KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase.CHECKPOINT || fault.local()) {
            builder.setConfigProp("nereus.kafka.storage.bookkeeper.max.entries.per.ledger", "8");
            builder.setConfigProp("nereus.kafka.storage.bookkeeper.max.bytes.per.ledger", "1048576");
            builder.setConfigProp("message.max.bytes", "4096");
        }
        return builder.build();
    }

    private static void exercise(KafkaClusterTestKit cluster, RootCloseFault fault) throws Exception {
        var client = new Properties();
        client.put("bootstrap.servers", cluster.bootstrapServers());
        try (Admin admin = Admin.create(client)) {
            var partition = new TopicPartition("native-cold", 0);
            admin.createTopics(List.of(new NewTopic(partition.topic(), Map.of(0, List.of(0)))))
                    .all().get(30, TimeUnit.SECONDS);
            var expected = new java.util.ArrayList<String>();
            int beforeCount = fault.local() ? 10
                    : fault.historyPhase() == KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase.CHECKPOINT ? 4 : 1;
            for (int offset = 0; offset < beforeCount; offset++) {
                var value = offset == 0 ? "before" : "before-" + offset;
                produce(client, value, offset);
                expected.add(value);
            }
            var topicId = admin.describeTopics(List.of(partition.topic())).allTopicNames()
                    .get(30, TimeUnit.SECONDS).get(partition.topic()).topicId();
            var zero = cluster.brokers().get(0);
            var aggregate = KafkaTopicBindingAggregateMapperV1.toRecord(zero.metadataCache().getImage()
                    .topics().getTopic(topicId).nereusAggregate().orElseThrow());
            try (var probe = new NereusClosedOwnerProbe(zero.config(), topicId, partition, aggregate, 1)) {
                reassign(admin, partition, 1, 0);
                var fetch = fault.beginFetch();
                if (fault.local()) {
                    exerciseLocalFetch(admin, partition, client, expected, fault, fetch);
                    return;
                }
                assertTrue(fault.entered.await(30, TimeUnit.SECONDS), "real verifier close was not observed");
                if (fault.closing()) {
                    exerciseClosing(admin, partition, client, expected, probe, fault, fetch);
                    return;
                }
                exerciseHeldReader(admin, partition, client, expected, probe, fault, fetch);
            }
        }
    }

    private static void exerciseHeldReader(Admin admin, TopicPartition partition, Properties client, List<String> expected,
            NereusClosedOwnerProbe probe, RootCloseFault fault, CompletableFuture<KafkaBookKeeperReadResultV1> fetch) throws Exception {
        var handle = fault.handle.get();
        var heldOperation = heldReaderOperation(probe, fault, handle);
        fault.verifyFetchLifetime(fetch);
        var closeObservers = fault.beginConcurrentClose();
        if (fault.historyPhase() != null && !fault.fetch() && fault.unknown()) {
            assertTrue(fault.recoveryRejected.await(30, TimeUnit.SECONDS),
                    "UNKNOWN history close must prevent successful first activation");
            requireFailure(fault.recoveryFailure.get(), "native selected history close response lost");
        }
        fault.controlBlocked.set(false);
        reassign(admin, partition, 2, 1);
        produce(client, "after", expected.size());
        expected.add("after");
        assertEquals(expected, KafkaBookKeeperOwnerRelocationRealTest.consume(client, expected.size()));
        verifyRetainedSource(probe, fault, handle, heldOperation);
        fault.verifyFetchProtection();
        if (fault.capacity()) {
            reassign(admin, partition, 1, 2);
            assertTrue(fault.capacityRejected.await(30, TimeUnit.SECONDS), "retained verifier capacity was bypassed");
            requireFailure(fault.admissionFailure.get(), fault.capacityLabel.get() + " Cell read capacity is full");
            assertEquals(2, fault.rejectedHandles.get(), "recovered verifier must reserve its two-handle peak");
            assertTrue(fault.recoveryRejected.await(30, TimeUnit.SECONDS));
            requireFailure(fault.recoveryFailure.get(), fault.activationFailure());
            assertEquals(1, fault.observedCloses.get(), "rejection must precede another verifier native read");
            assertTrue(probe.activeReaderOperations(handle).contains(heldOperation));
        }
        if (fault.historyPhase() != null) {
            assertEquals(1, fault.historySessions.get(), "another accepted history session bypassed retained capacity");
            assertEquals(1, fault.historyNativeCloses.get(), "owned history close must run once");
            assertTrue(fault.historyNativeOpens.get() > 0, "history test must open the real ledger");
        }
        if (!fault.unknown()) {
            fault.controlBlocked.set(fault.fetch());
            fault.release.complete(null);
            if (!fault.fetch()) awaitReaderClosed(probe, handle, heldOperation);
            fault.verifyFetchDrain();
            for (var close : closeObservers) close.get(10, TimeUnit.SECONDS);
            if (!closeObservers.isEmpty()) assertEquals(1, fault.closeAttempts.get());
            assertEquals(0, fault.controlCalls.get(), "ordinary Fetch or its actual cleanup accessed Oxia control state");
            fault.controlBlocked.set(false);
        }
    }

    private static com.nereusstream.domain.bytes.Sha256Digest heldReaderOperation(NereusClosedOwnerProbe probe,
            RootCloseFault fault, RunLedgerHandleV1 handle) throws Exception {
        var heldOperations = probe.activeReaderOperations(handle);
        assertEquals(fault.fetch() ? 0 : 1, heldOperations.size());
        var heldOperation = fault.fetch() ? null : heldOperations.get(0);
        if (fault.fetch()) assertTrue(probe.admittedSource(handle, 1), "captured source lost its durable catalog reference");
        return heldOperation;
    }

    private static void verifyRetainedSource(NereusClosedOwnerProbe probe, RootCloseFault fault, RunLedgerHandleV1 handle,
            com.nereusstream.domain.bytes.Sha256Digest heldOperation) throws Exception {
        if (fault.fetch()) {
            assertEquals(0, probe.activeReaderTickets(handle), "ordinary M4 Fetch created a durable reader nonce");
            assertTrue(probe.admittedSource(handle, 1), "takeover removed the captured physical source reference");
            assertTrue(probe.retainedOpenQuota(handle), "captured native source was retired or quota settled");
        } else assertTrue(probe.activeReaderOperations(handle).contains(heldOperation),
                "another owner's verification removed the exact old invocation pin");
    }

    private static void exerciseLocalFetch(Admin admin, TopicPartition partition, Properties client, List<String> expected,
            RootCloseFault fault, CompletableFuture<KafkaBookKeeperReadResultV1> fetch) throws Exception {
        var first = fetch.get(10, TimeUnit.SECONDS);
        assertEquals(KafkaBookKeeperReadOutcomeV1.FOUND, first.outcome());
        assertTrue(fault.historyNativeOpens.get() > 1, "real Fetch did not cross historical run boundaries");
        var firstOpens = fault.historyNativeOpens.get();
        var firstReads = fault.historyNativeReads.get();
        var cached = fault.beginFetch().get(10, TimeUnit.SECONDS);
        assertEquals(first.batches(), cached.batches(), "cached index changed actual RecordBatch bytes");
        assertTrue(fault.historyNativeReads.get() - firstReads < firstReads,
                "repeated Fetch did not use the previously fetched native index blocks");
        assertEquals(expected, KafkaBookKeeperOwnerRelocationRealTest.consume(client, expected.size()));
        assertEquals(expected, KafkaBookKeeperOwnerRelocationRealTest.consume(client, expected.size()));
        assertTrue(fault.historyNativeOpens.get() > firstOpens, "cached index replay did not read native DATA");
        assertEquals(fault.historyNativeOpens.get(), fault.historyNativeCloses.get(), "serial native readers did not close");
        assertEquals(0, fault.controlCalls.get(), "normal/cached historical Fetch accessed Oxia control metadata");
        fault.controlBlocked.set(false);
        reassign(admin, partition, 2, 1);
        produce(client, "after", expected.size());
        expected.add("after");
        assertEquals(expected, KafkaBookKeeperOwnerRelocationRealTest.consume(client, expected.size()));
    }

    private static void exerciseClosing(Admin admin, TopicPartition partition, Properties client, List<String> expected,
            NereusClosedOwnerProbe probe, RootCloseFault fault, CompletableFuture<KafkaBookKeeperReadResultV1> fetch) throws Exception {
        var handle = fault.handle.get();
        assertEquals(0, probe.activeReaderTickets(handle), "metadata hold must precede the native reader pin");
        assertTrue(!fetch.isDone());
        fault.verifyFetchProtection();
        var owner = fault.nativeOwner.get(5, TimeUnit.SECONDS);
        var first = owner.closeAsync();
        var second = owner.closeAsync();
        assertTrue(fault.closeStarted.await(5, TimeUnit.SECONDS), "real close did not freeze reader admission");
        assertTrue(!first.isDone() && !second.isDone(), "accepted Fetch must retain both close observers");
        var metadataRelease = CompletableFuture.runAsync(() -> fault.release.complete(null));
        first.get(10, TimeUnit.SECONDS);
        second.get(10, TimeUnit.SECONDS);
        assertEquals(1, fault.closeAttempts.get(), "concurrent close callers must share one resource close attempt");
        metadataRelease.get(5, TimeUnit.SECONDS);
        assertTrue(fetch.isCompletedExceptionally(), "closing owner must reject the pending native admission");
        fault.verifyFetchDrain();
        assertEquals(0, probe.activeReaderTickets(handle));
        assertEquals(0, fault.historySessions.get());
        assertEquals(0, fault.historyNativeOpens.get());
        reassign(admin, partition, 2, 1);
        produce(client, "after", expected.size());
        expected.add("after");
        assertEquals(expected, KafkaBookKeeperOwnerRelocationRealTest.consume(client, expected.size()));
    }

    static void verifyProduceFenced(NereusKafkaOwnedProviderRuntime.Opened opened,
            Properties client) throws Exception {
        var before = opened.storage().nativeInputSources().protocol().root();
        var failed = new Properties();
        failed.putAll(client);
        failed.put("retries", "0");
        failed.put("delivery.timeout.ms", "5000");
        failed.put("request.timeout.ms", "1000");
        assertThrows(java.util.concurrent.ExecutionException.class,
                () -> produce(failed, "fenced", before.frontiers().durableEndOffset()),
                "ordinary Produce crossed the native fallback publication fence");
        assertEquals(before, opened.storage().nativeInputSources().protocol().root(),
                "fenced Produce changed the actual coherent protocol root");
    }


    static void produce(Properties client, String value, long offset) throws Exception {
        var config = new Properties();
        config.putAll(client);
        config.put("key.serializer", StringSerializer.class.getName());
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("acks", "all");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            assertEquals(offset, producer.send(new ProducerRecord<>("native-cold", 0, "key", value))
                    .get(30, TimeUnit.SECONDS).offset());
        }
    }

    static void reassign(Admin admin, TopicPartition partition, int owner, int previous) throws Exception {
        admin.alterPartitionReassignments(Map.of(partition, Optional.of(new NewPartitionReassignment(List.of(owner)))))
                .all().get(30, TimeUnit.SECONDS);
        assertEquals(owner, KafkaBookKeeperOwnerRelocationRealTest.awaitLeader(admin, previous));
    }

    private static void awaitReaderClosed(NereusClosedOwnerProbe probe, RunLedgerHandleV1 handle,
            com.nereusstream.domain.bytes.Sha256Digest operation) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < until && probe.activeReaderOperations(handle).contains(operation)) Thread.sleep(25);
        assertTrue(!probe.activeReaderOperations(handle).contains(operation), "the exact held invocation pin was not cleared");
    }

    private static void requireFailure(Throwable failure, String text) {
        Throwable current = failure;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().contains(text)) return;
            for (var suppressed : current.getSuppressed()) {
                if (suppressed.toString().contains(text)) return;
            }
            current = current.getCause();
        }
        throw new AssertionError("missing expected failure: " + text, failure);
    }

    private static final class RootCloseFault {
        private final String mode;
        private final CompletableFuture<Void> release = new CompletableFuture<>();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch capacityRejected = new CountDownLatch(1);
        private final AtomicReference<RunLedgerHandleV1> handle = new AtomicReference<>();
        private final AtomicReference<Throwable> recoveryFailure = new AtomicReference<>();
        private final AtomicReference<Throwable> admissionFailure = new AtomicReference<>();
        private final AtomicInteger rejectedHandles = new AtomicInteger();
        private final CountDownLatch recoveryRejected = new CountDownLatch(1);
        private final AtomicInteger observedCloses = new AtomicInteger();
        private final AtomicInteger historySessions = new AtomicInteger();
        private final AtomicInteger historyNativeOpens = new AtomicInteger();
        private final AtomicInteger historyNativeCloses = new AtomicInteger();
        private final AtomicInteger historyNativeReads = new AtomicInteger();
        private final AtomicReference<String> capacityLabel = new AtomicReference<>();
        private final CompletableFuture<KafkaBookKeeperPartitionV1> ready = new CompletableFuture<>();
        private final CompletableFuture<NereusKafkaOwnedProviderRuntime.Opened> nativeOwner = new CompletableFuture<>();
        private final CountDownLatch closeStarted = new CountDownLatch(1);
        private final AtomicInteger closeAttempts = new AtomicInteger();
        private final AtomicBoolean controlBlocked = new AtomicBoolean();
        private final AtomicInteger controlCalls = new AtomicInteger();

        private RootCloseFault(String mode) {
            this.mode = mode;
            capacityLabel.set(nativeLabel());
        }
        private boolean capacity() {
            return mode.endsWith("_CAPACITY");
        }
        private boolean unknown() {
            return mode.endsWith("_UNKNOWN") || mode.endsWith("_THROWN");
        }
        private KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase historyPhase() {
            return mode.startsWith("HISTORY_") ? KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase.valueOf(
                    mode.substring("HISTORY_".length(), mode.lastIndexOf('_'))) : null;
        }
        private boolean fetch() {
            return historyPhase() == KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase.FETCH;
        }
        private boolean closing() {
            return mode.endsWith("_CLOSING");
        }
        private boolean local() {
            return mode.endsWith("_LOCAL");
        }
        private List<CompletableFuture<Void>> beginConcurrentClose() throws Exception {
            if (!mode.endsWith("_CLOSE")) return List.of();
            var owner = nativeOwner.get(5, TimeUnit.SECONDS);
            var first = owner.closeAsync();
            var second = owner.closeAsync();
            assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
            assertTrue(!first.isDone() && !second.isDone(), "actual M4 reader must retain concurrent owner closes");
            return List.of(first, second);
        }
        private CompletableFuture<KafkaBookKeeperReadResultV1> beginFetch() throws Exception {
            if (!fetch()) return null;
            var storage = ready.get(30, TimeUnit.SECONDS);
            controlBlocked.set(!closing());
            return storage.read(new KafkaBookKeeperSequentialReadRequestV1(
                    0, KafkaReadIsolationV1.READ_UNCOMMITTED, 8192, Optional.empty())).toCompletableFuture();
        }
        private KafkaBookKeeperM4BrokerReadOwnerV2 fetchOwner() throws Exception {
            var storage = ready.get(5, TimeUnit.SECONDS);
            var field = KafkaBookKeeperPartitionV1.class.getDeclaredField("m4ReadOwner");
            field.setAccessible(true);
            return (KafkaBookKeeperM4BrokerReadOwnerV2) field.get(storage);
        }
        private int fetchPermits(KafkaBookKeeperM4BrokerReadOwnerV2 owner, String name) throws Exception {
            var field = KafkaBookKeeperM4BrokerReadOwnerV2.class.getDeclaredField(name);
            field.setAccessible(true);
            return ((java.util.concurrent.Semaphore) field.get(owner)).availablePermits();
        }
        private boolean fetchControlWindow() throws Exception {
            if (local()) return true;
            var owner = fetchOwner();
            var reads = KafkaBookKeeperM4BrokerReadOwnerV2.class.getDeclaredField("inFlight");
            reads.setAccessible(true);
            synchronized (owner) {
                // Actual owner-close metadata is permitted only after the local read lifetime has drained.
                return (int) reads.get(owner) != 0;
            }
        }
        private void verifyFetchLifetime(CompletableFuture<KafkaBookKeeperReadResultV1> fetchResult) throws Exception {
            if (!fetch()) return;
            if (unknown()) assertEquals(KafkaBookKeeperReadOutcomeV1.PROVIDER_FAILURE,
                    fetchResult.get(5, TimeUnit.SECONDS).outcome());
            else if (mode.endsWith("_CANCEL")) assertTrue(fetchResult.cancel(true));
            else assertTrue(!fetchResult.isDone(), "held actual close must retain the Fetch observer");
            verifyFetchProtection();
        }
        private void verifyFetchProtection() throws Exception {
            if (!fetch()) return;
            assertEquals(0, controlCalls.get(), "ordinary Fetch issued remote control I/O");
            var owner = fetchOwner();
            if (closing()) {
                assertEquals(null, owner, "metadata race must exercise the supported no-M4 entry");
                var reads = KafkaBookKeeperPartitionV1.class.getDeclaredField("activeReads");
                reads.setAccessible(true);
                assertEquals(1, reads.get(ready.getNow(null)), "accepted metadata reply must retain its source close");
                return;
            }
            assertEquals(2, fetchPermits(owner, "cellReadSlots"));
            assertEquals(2, fetchPermits(owner, "bindingReadSlots"));
            var captured = owner.selectorRuntime().currentAuthority().get();
            assertEquals(com.nereusstream.storage.object.read.BindingReadHazardPoolV1.ScanOutcome.PINNED,
                    owner.hazards().scan(captured.bindingId(), captured.sourceGeneration()));
        }
        private void verifyFetchDrain() throws Exception {
            if (!fetch()) return;
            var owner = fetchOwner();
            if (closing()) {
                var reads = KafkaBookKeeperPartitionV1.class.getDeclaredField("activeReads");
                reads.setAccessible(true);
                assertEquals(0, reads.get(ready.getNow(null)), "no-M4 source read did not actually drain");
                return;
            }
            owner.drain().toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertEquals(3, fetchPermits(owner, "cellReadSlots"));
            assertEquals(3, fetchPermits(owner, "bindingReadSlots"));
            var captured = owner.selectorRuntime().currentAuthority().get();
            assertEquals(com.nereusstream.storage.object.read.BindingReadHazardPoolV1.ScanOutcome.CLEAN,
                    owner.hazards().scan(captured.bindingId(), captured.sourceGeneration()));
        }
        private String nativeLabel() {
            return historyPhase() == null ? "native root verifier" : "native selected history";
        }
        private String activationFailure() {
            return capacityLabel.get().equals("native root verifier") ? "BK authority mutation is unresolved: OUTCOME_UNKNOWN"
                    : capacityLabel.get() + " Cell read capacity is full";
        }
        private boolean matches(int broker, KafkaRunRootRecordV2 record) {
            return broker == 1 && record.root().topicIncarnation().topicName().value().equals("native-cold");
        }
        private CompletionStage<Void> observeClose(KafkaRunRootRecordV2 record,
                com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1 capability,
                CompletionStage<Void> actual) {
            return actual.thenCompose(ignored -> {
                var root = record.root();
                handle.compareAndSet(null, new RunLedgerHandleV1(root.providerScopeId(), root.runId(),
                        root.ledgerIdentity(), capability.configurationDigest()));
                observedCloses.incrementAndGet();
                entered.countDown();
                if (mode.endsWith("_THROWN")) throw new IllegalStateException(nativeLabel() + " close response lost");
                return unknown() ? CompletableFuture.failedFuture(
                        new java.util.concurrent.TimeoutException(nativeLabel() + " close response lost")) : release;
            });
        }

        private NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver observer(kafka.server.KafkaConfig config) {
            int broker = config.brokerId();
            final com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1 capability;
            try {
                capability = NereusBookKeeperCapability.loadAndValidate(config);
            } catch (Exception failure) {
                throw new java.util.concurrent.CompletionException(failure);
            }
            return new NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver() {
                @Override
                public io.oxia.client.api.AsyncOxiaClient controlClient(io.oxia.client.api.AsyncOxiaClient client) {
                    return (io.oxia.client.api.AsyncOxiaClient) java.lang.reflect.Proxy.newProxyInstance(
                            getClass().getClassLoader(), new Class<?>[] {io.oxia.client.api.AsyncOxiaClient.class}, (proxy, method, args) -> {
                                if (broker == 1 && controlBlocked.get() && method.getDeclaringClass() != Object.class
                                        && !method.getName().equals("close") && fetchControlWindow()) {
                                    controlCalls.incrementAndGet();
                                    throw new AssertionError("ordinary native Fetch attempted Oxia " + method.getName());
                                }
                                try {
                                    return method.invoke(client, args);
                                } catch (java.lang.reflect.InvocationTargetException failure) {
                                    throw failure.getCause();
                                }
                            });
                }
                @Override
                public CompletionStage<Void> afterRootVerifierClose(KafkaRunRootRecordV2 record, CompletionStage<Void> actual) {
                    if (historyPhase() != null || !matches(broker, record)
                            || (mode.equals("HEADER_HELD") ? record.root().creatorOwnerEpoch() != 2
                                    || record.recoveryCut().isPresent() : record.recoveryCut().isEmpty())) return actual;
                    return local() ? actual : observeClose(record, capability, actual);
                }

                @Override
                public BookKeeperCellSession historyReaderSession(KafkaRunRootRecordV2 record,
                        KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase phase, BookKeeperCellSession session) {
                    if (!matches(broker, record) || phase != historyPhase() || record.root().creatorOwnerEpoch() != 1)
                        return session;
                    historySessions.incrementAndGet();
                    return (BookKeeperCellSession) java.lang.reflect.Proxy.newProxyInstance(
                            getClass().getClassLoader(), new Class<?>[] {BookKeeperCellSession.class}, (proxy, method, args) -> {
                                if (method.getName().equals("openRunLedger")) historyNativeOpens.incrementAndGet();
                                if (method.getName().equals("closeAsync")) historyNativeCloses.incrementAndGet();
                                if (method.getName().equals("readExactEntry")) historyNativeReads.incrementAndGet();
                                try {
                                    return method.invoke(session, args);
                                } catch (java.lang.reflect.InvocationTargetException failure) {
                                    throw failure.getCause();
                                }
                            });
                }

                @Override
                public CompletionStage<Void> afterHistoryReaderClose(KafkaRunRootRecordV2 record,
                        KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase phase, CompletionStage<Void> actual) {
                    if (closing() || !matches(broker, record) || phase != historyPhase() || record.root().creatorOwnerEpoch() != 1)
                        return actual;
                    return local() ? actual : observeClose(record, capability, actual);
                }

                @Override
                public void afterHistoryReaderAdmissionFailure(KafkaRunRootRecordV2 record,
                        KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase phase, Throwable failure) {
                    if (matches(broker, record) && capacity()) {
                        capacityLabel.set("native selected history");
                        admissionFailure.set(failure);
                        rejectedHandles.set(phase.maximumHandles());
                        capacityRejected.countDown();
                    }
                }

                @Override
                public void afterReady(org.apache.kafka.common.TopicIdPartition partition, KafkaBookKeeperPartitionV1 storage) {
                    if (broker == 1 && fetch() && partition.topicPartition().topic().equals("native-cold")) {
                        ready.complete(storage);
                    }
                }

                @Override
                public void afterReady(org.apache.kafka.common.TopicIdPartition partition, NereusKafkaOwnedProviderRuntime.Opened owner) {
                    afterReady(partition, owner.storage());
                    if (broker == 1 && fetch() && partition.topicPartition().topic().equals("native-cold"))
                        nativeOwner.complete(owner);
                }

                @Override
                public CompletionStage<Optional<KafkaRunRootRecordV2>> afterHistoricalFetchRecordRead(
                        com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1 root,
                        CompletionStage<Optional<KafkaRunRootRecordV2>> actual) {
                    if (broker != 1 || !closing() || !root.topicIncarnation().topicName().value().equals("native-cold"))
                        return actual;
                    return actual.thenCompose(record -> {
                        handle.compareAndSet(null, new RunLedgerHandleV1(root.providerScopeId(), root.runId(),
                                root.ledgerIdentity(), capability.configurationDigest()));
                        entered.countDown();
                        return release.thenApply(ignored -> record);
                    });
                }

                @Override
                public void afterReadAdmissionClosed(NereusKafkaOwnedProviderRuntime.Opened owner) {
                    if (broker == 1 && fetch() && owner == nativeOwner.getNow(null)) {
                        closeAttempts.incrementAndGet();
                        closeStarted.countDown();
                    }
                }

                @Override
                public void afterRecovery(org.apache.kafka.common.TopicIdPartition partition, Throwable failure) {
                    if (broker == 1 && (capacity() || historyPhase() != null && unknown()) && failure != null) {
                        recoveryFailure.set(failure);
                        recoveryRejected.countDown();
                    }
                }

                @Override
                public void afterRootVerifierAdmissionFailure(KafkaRunRootRecordV2 record, int maximumHandles, Throwable failure) {
                    if (broker == 1 && capacity()) {
                        admissionFailure.set(failure);
                        rejectedHandles.set(maximumHandles);
                        capacityRejected.countDown();
                    }
                }
            };
        }
    }
}
