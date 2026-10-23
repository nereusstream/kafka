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
import kafka.server.metadata.DynamicConfigPublisher;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.MetadataVersion;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperReadCellBudgetV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperRunSourceV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryCodecV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperTaskTerminalV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionWriterV2.VerifiedPart;
import com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecoveryStateCodecV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionAdmissionV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Caps;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.CompactionPlan;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionSemanticOutputV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.OutputCompressionPolicy;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.TransactionOutcome;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Disposition;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSemanticCompactorV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionSemanticValidatorV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaBatchDuplicateIdentityV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperClosedHistoryRecoveryV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperReadViewV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperDescriptorV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperReaderV2;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadSnapshotV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadRunV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperRunTableV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperTargetedReaderV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperSequentialReadRequestV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadOutcomeV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.AppendQuorumProofV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.gc.BoundPhysicalDeleteAuthorityRouteV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlKeysV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState;
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5TaskSelectionDecisionV2;
import io.oxia.client.api.AsyncOxiaClient;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Actual Broker Produce/rollover sources; no imported logical cut, frontier, worker lease or semantic proof. */
public final class KafkaBookKeeperNativeInputRealScenario {
    static final KafkaBookKeeperRunSourceV2.Bounds BOUNDS =
            new KafkaBookKeeperRunSourceV2.Bounds(256, 128, 256, 4_194_304, 1_048_576, 4_194_304);

    private KafkaBookKeeperNativeInputRealScenario() { }

    public static void run(Path receipt, String mode) throws Exception {
        run(receipt, mode, Boolean.getBoolean("nereus.nsip.m5SealedGcProfile"));
    }

    public static void run(Path receipt, String mode, boolean sealedGcProfile) throws Exception {
        var inputs = Files.createTempDirectory("nereus-nsip-native-input-");
        var state = new State(mode);
        try {
            KafkaBookKeeperOwnerRelocationRealTest.writeCapability(inputs, Optional.of(receipt));
            try (var cluster = cluster(inputs, state, sealedGcProfile)) {
                cluster.format();
                cluster.startup();
                cluster.waitForReadyBrokers();
                if (mode.equals("CHECKPOINT") || mode.equals("PLAN")) exerciseCheckpoint(cluster, state);
                else if (mode.startsWith("TASK")) Task.exercise(cluster, state);
                else if (mode.equals("CAPACITY")) Capacity.exercise(cluster, state);
                else exercise(cluster, state, mode);
            }
        } finally {
            state.releaseClose.complete(null);
            try (var files = Files.walk(inputs)) {
                for (var file : files.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(file);
            }
        }
    }

    private static KafkaClusterTestKit cluster(Path inputs, State state, boolean sealedGcProfile) throws Exception {
        var bootstrap = BootstrapMetadata.fromVersion(MetadataVersion.latestTesting(), "nsip-native-input")
                .copyWithFeatureRecord("nereus.storage.version", (short) 2);
        var nodes = new TestKitNodes.Builder(bootstrap).setNumBrokerNodes(3).setNumControllerNodes(1).build();
        var builder = new KafkaClusterTestKit.Builder(nodes)
                .setControllerStorageRuntimeFactory(new NereusControllerStorageRuntimeFactory())
                .setBrokerStorageRuntimeFactory(context -> new NereusBrokerStorageRuntime(context,
                        new NereusKafkaOwnedProviderRuntime(context, state.observer(context.config().brokerId()))));
        var nativeConfig = KafkaBookKeeperOwnerRelocationRealTest.nativeConfig(inputs, System.getProperty("nereus.nsip.bkUri"),
                System.getProperty("nereus.nsip.oxia"));
        KafkaBookKeeperM5PolicyRealScenario.configure(builder, inputs, nativeConfig, sealedGcProfile, state.mode);
        builder.setConfigProp("nereus.kafka.storage.bookkeeper.max.bytes.per.ledger", "1048576");
        builder.setConfigProp("message.max.bytes", "4096");
        if (state.mode.equals("CAPACITY") || KafkaBookKeeperSelectedInputRealScenario.captureTasks(state.mode) == 2)
            builder.setConfigProp("nereus.kafka.storage.compaction.max.concurrent.tasks", "2");
        if (state.mode.startsWith("TASK_OUTPUT_PUBLISH") && state.mode.contains("_COLD"))
            builder.setConfigProp("nereus.kafka.storage.recovery.executor.threads", "1");
        return builder.build();
    }

    private static void exercise(KafkaClusterTestKit cluster, State state, String mode) throws Exception {
        var client = new Properties();
        client.put("bootstrap.servers", cluster.bootstrapServers());
        try (var admin = Admin.create(client)) {
            var partition = new TopicPartition("native-cold", 0);
            admin.createTopics(List.of(topic(mode, partition.topic()))).all().get(30, TimeUnit.SECONDS);
            var opened = state.zero.get(30, TimeUnit.SECONDS);
            int produced = 0;
            while (opened.storage().nativeReadSources().sealedRoots().stream()
                    .filter(root -> root.kafkaEndOffsetExclusive().orElseThrow() > root.kafkaStartOffset()).count() < 2 && produced < 32) {
                KafkaBookKeeperRootVerifierRealScenario.produce(client, "input-" + produced, produced);
                produced++;
            }
            var budget = budget(opened);
            if (mode.equals("LIFETIME") || mode.equals("CLEANUP")) {
                verifyLifetime(opened, budget, state);
                return;
            }
            var captured = captureWhenReady(opened, budget);
            verifyCheckpoint(captured);
            KafkaBookKeeperM5PolicyRealScenario.verifyInitialPolicy(cluster, opened, captured);
            if (mode.equals("POLICY")) {
                verifyPolicy(cluster, admin, opened, budget, captured, state);
                return;
            }
            assertTrue(captured.extents().size() >= 2, "native Produce did not form a multi-run input cut");
            assertEquals(captured.nativeSources().sources().selectedViewSha256(), captured.predecessorSelector().selectedViewSha256());
            var actualFrontiers = captured.nativeSources().protocol().root().frontiers();
            assertEquals(actualFrontiers.durableEndOffset(), captured.frontiers().durableOffset());
            assertEquals(actualFrontiers.highWatermark(), captured.frontiers().highWatermark());
            assertEquals(actualFrontiers.lastStableOffset(), captured.frontiers().lastStableOffset());
            var workerProposal = withCurrentMetadataImage(captured::proposeWorker);
            assertEquals(captured.predecessorSelector().binding(), workerProposal.identity().binding());
            assertEquals(captured.predecessorSelector().ownerEpoch(), workerProposal.identity().ownerEpoch());
            assertEquals(captured.predecessorSelector().capability(), workerProposal.identity().capability());
            assertEquals(opened.nativeM5ProtocolCell(), workerProposal.identity().protocolCellSha256());
            assertEquals(captured.nativeSources().sources().activeRoot().providerScopeId().digest(),
                    workerProposal.identity().providerScopeSha256());
            assertEquals(opened.nativeM5StorageFence(), workerProposal.identity().storageFence());
            assertEquals(1, workerProposal.identity().workerEpoch(), "a proposal must not itself admit or advance a worker");
            var offsets = new ArrayList<Long>();
            var values = new ArrayList<String>();
            for (var batch : captured.batches()) {
                for (var record : MemoryRecords.readableRecords(ByteBuffer.wrap(batch.canonicalBody().toByteArray())).records()) {
                    offsets.add(record.offset());
                    values.add(StandardCharsets.UTF_8.decode(record.value()).toString());
                }
            }
            var end = captured.frontiers().candidateEndOffsetExclusive();
            assertEquals(java.util.stream.LongStream.range(0, end).boxed().toList(), offsets);
            assertEquals(offsets.stream().map(offset -> "input-" + offset).toList(), values);
            var membership = verifyWhenReady(captured);
            assertEquals(captured.extents().stream().map(extent -> extent.sourceIdentitySha256()).collect(java.util.stream.Collectors.toSet()),
                    membership.keySet());
            assertTrue(membership.values().stream().allMatch(members -> members.size() == 1));
            assertEquals(0, budget.usage().owners());
            var callbacks = new AtomicInteger();
            var omitted = captured.batches().subList(1, captured.batches().size());
            reject(captured.verifyInputs(captured.extents(), omitted).thenRun(callbacks::incrementAndGet));
            var reversed = new ArrayList<>(captured.batches());
            Collections.reverse(reversed);
            reject(captured.verifyInputs(captured.extents(), reversed).thenRun(callbacks::incrementAndGet));
            reject(captured.verifyInputs(captured.extents().subList(1, captured.extents().size()), captured.batches())
                    .thenRun(callbacks::incrementAndGet));
            admin.createTopics(List.of(new NewTopic("native-foreign", Map.of(0, List.of(0)))))
                    .all().get(30, TimeUnit.SECONDS);
            for (int offset = 0; offset < 5; offset++) produceForeign(client, offset);
            var foreignOwner = state.foreign.get(30, TimeUnit.SECONDS);
            var foreign = captureWhenReady(foreignOwner, budget(foreignOwner));
            reject(captured.verifyInputs(foreign.extents(), foreign.batches()).thenRun(callbacks::incrementAndGet));
            assertEquals(captured.extents().size(), verifyWhenReady(captured).size(),
                    "unrelated topic metadata invalidated the policy cut");
            int rolloverLimit = produced + 32;
            while (opened.storage().nativeReadSources().selectedViewSha256().equals(captured.nativeSources().sources().selectedViewSha256())
                    && produced < rolloverLimit) {
                KafkaBookKeeperRootVerifierRealScenario.produce(client, "input-" + produced, produced);
                produced++;
            }
            reject(captured.verifyInputs(captured.extents(), captured.batches()).thenRun(callbacks::incrementAndGet));
            var newer = captureWhenReady(opened, budget);
            verifyCheckpoint(newer);
            assertEquals(end, captured.nativeSources().recoveryCheckpoint().orElseThrow().vector().recoveryCoveredThrough(),
                    "rollover changed the previously captured recovery state");
            assertFalse(newer.predecessorSelector().equals(captured.predecessorSelector()));
            reject(newer.verifyInputs(captured.extents(), captured.batches()).thenRun(callbacks::incrementAndGet));
            KafkaBookKeeperRootVerifierRealScenario.reassign(admin, partition, 1, 0);
            var next = state.one.get(30, TimeUnit.SECONDS);
            assertEquals((long) produced, admin.listOffsets(Map.of(partition, OffsetSpec.latest()))
                    .all().get(30, TimeUnit.SECONDS).get(partition).offset());
            reject(newer.verifyInputs(newer.extents(), newer.batches()).thenRun(callbacks::incrementAndGet));
            var afterMove = captureWhenReady(next, budget(next));
            verifyCheckpoint(afterMove);
            assertTrue(afterMove.predecessorSelector().ownerEpoch() > newer.predecessorSelector().ownerEpoch());
            assertEquals(newer.batches(), afterMove.batches().subList(0, newer.batches().size()));
            assertTrue(afterMove.extents().size() > newer.extents().size(), "takeover omitted the recovered former active run");
            assertEquals((long) produced, afterMove.frontiers().candidateEndOffsetExclusive());
            assertEquals(afterMove.extents().size(), verifyWhenReady(afterMove).size());
            assertEquals(0, callbacks.get(), "invalid native cut reached the next publication step");
        }
    }

    private static void verifyCheckpoint(NereusKafkaNativeInputV2.Captured captured) {
        var cut = captured.nativeSources();
        var checkpoint = cut.recoveryCheckpoint().orElseThrow();
        assertTrue(checkpoint.vector().isAlignedCompoundCheckpoint());
        assertEquals(captured.frontiers().candidateEndOffsetExclusive(), checkpoint.vector().recoveryCoveredThrough());
        assertEquals(cut.sources().activeRoot().kafkaStartOffset(), checkpoint.vector().recoveryCoveredThrough());
        assertEquals(checkpoint, KafkaProtocolCheckpointStateV1.fromNbke2(checkpoint.toNbke2()),
                "actual checkpoint lost protocol or historical read-index state when encoded");
        var transferred = KafkaCompactionRecoveryStateCodecV1.decode(captured.recoveryState().encode());
        assertEquals(checkpoint.components(), transferred.components(),
                "independent recovery state lost actual producer, transaction or epoch components");
        assertEquals(cut.protocol().root().references().checkpointVector().contentDigest(), transferred.sourceCheckpointSha256());
        assertEquals(cut.protocol().root().fence(), transferred.sourceFence());
        assertEquals(checkpoint.vector().recoveryCoveredThrough(), transferred.coveredThrough());
        var active = cut.sources().activeRoot();
        var outputCheckpoint = transferred.checkpointFor(new Nbke2RunBindingV1(
                active.bindingId(), active.topicIncarnation(), active.partitionId(), active.storageEpochId(),
                active.creatorOwnerEpoch(), active.kafkaLeaderEpoch(), active.providerScopeId(), active.runId()));
        assertTrue(outputCheckpoint.readIndexes().isEmpty(), "independent state retained obsolete source locators");
        assertEquals(checkpoint.components(), outputCheckpoint.components());
        var latest = new java.util.TreeMap<Long, org.apache.kafka.common.record.RecordBatch>();
        for (var input : captured.batches()) {
            for (var batch : MemoryRecords.readableRecords(ByteBuffer.wrap(input.canonicalBody().toByteArray())).batches()) {
                if (batch.producerId() >= 0 && !batch.isControlBatch()) latest.put(batch.producerId(), batch);
            }
        }
        for (var batch : latest.values()) {
            var duplicate = checkpoint.producerState().findDuplicate(new KafkaBatchDuplicateIdentityV1(
                    batch.producerId(), batch.producerEpoch(), batch.baseSequence(), batch.lastSequence())).orElseThrow();
            assertEquals(batch.baseOffset(), duplicate.startOffset());
            assertEquals(batch.lastOffset() + 1, duplicate.endOffsetExclusive());
            assertEquals(batch.maxTimestamp(), duplicate.maxTimestamp());
        }
        assertFalse(latest.isEmpty(), "real Produce did not exercise the native producer checkpoint");
    }

    private static void verifyTransactionCheckpoint(KafkaClusterTestKit cluster, Admin admin, Properties client,
            NereusKafkaOwnedProviderRuntime.Opened opened, State state, TopicPartition partition) throws Exception {
        admin.createTopics(List.of(
                new NewTopic("__transaction_state", Map.of(0, List.of(0))).configs(Map.of("cleanup.policy", "compact")),
                new NewTopic("__consumer_offsets", Map.of(0, List.of(0))).configs(Map.of("cleanup.policy", "compact"))))
                .all().get(30, TimeUnit.SECONDS);
        var config = new Properties();
        config.putAll(client);
        config.put("key.serializer", StringSerializer.class.getName());
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("acks", "all");
        config.put("enable.idempotence", "true");
        config.put("transactional.id", "native-input-checkpoint");
        var expected = new ArrayList<String>();
        String transactionKey = Plan.transactionKey(state.mode);
        int offset;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            producer.initTransactions();
            producer.beginTransaction();
            for (offset = 0; offset < 12; offset++) {
                String value = "committed-" + offset;
                expected.add(value);
                assertEquals((long) offset, producer.send(new ProducerRecord<>(partition.topic(), 0, transactionKey, value))
                        .get(30, TimeUnit.SECONDS).offset());
            }
            producer.commitTransaction();
            producer.beginTransaction();
            for (offset = 13; offset < 19; offset++) {
                assertEquals((long) offset, producer.send(new ProducerRecord<>(partition.topic(), 0, transactionKey, "aborted-" + offset))
                        .get(30, TimeUnit.SECONDS).offset());
            }
            producer.abortTransaction();
            producer.beginTransaction();
            assertEquals(20L, producer.send(new ProducerRecord<>(partition.topic(), 0, transactionKey, "open"))
                    .get(30, TimeUnit.SECONDS).offset());
            offset = 21;
            while (opened.storage().nativeReadSources().activeRoot().kafkaStartOffset() <= 20 && offset < 53) {
                expected.add("filler-" + offset);
                KafkaBookKeeperRootVerifierRealScenario.produce(client, "filler-" + offset, offset++);
            }
            var openCut = opened.storage().nativeInputSources();
            var openCheckpoint = openCut.recoveryCheckpoint().orElseThrow();
            assertTrue(openCheckpoint.vector().recoveryCoveredThrough() > 20, "open transaction never crossed a sealed run");
            assertEquals(20, openCut.protocol().root().frontiers().lastStableOffset());
            assertTrue(openCheckpoint.transactionState().ongoingTransactions().values().stream()
                    .anyMatch(transaction -> transaction.firstOffset() == 20));
            int scans = state.sdkCloses.get();
            reject(opened.captureM5Inputs(budget(opened), BOUNDS));
            assertEquals(scans, state.sdkCloses.get(), "candidate beyond LSO opened a native input SDK session");
            int markerOffset = offset++;
            producer.abortTransaction();
            while (opened.storage().nativeReadSources().activeRoot().kafkaStartOffset() <= markerOffset && offset < markerOffset + 33) {
                expected.add("filler-" + offset);
                KafkaBookKeeperRootVerifierRealScenario.produce(client, "filler-" + offset, offset++);
            }
            offset = Plan.sealIfSelected(state.mode, opened, client, offset, expected);
            var captured = captureWhenReady(opened, budget(opened));
            verifyCheckpoint(captured);
            var checkpoint = captured.nativeSources().recoveryCheckpoint().orElseThrow();
            assertTrue(checkpoint.transactionState().ongoingTransactions().isEmpty());
            assertEquals(3, checkpoint.transactionState().completedTransactions().size());
            assertEquals(2, checkpoint.transactionState().abortedTransactions().size());
            assertEquals(List.of(13L, 20L), checkpoint.transactionState().abortedTransactions().stream()
                    .map(transaction -> transaction.firstOffset()).toList());
            assertEquals(List.of(20L, markerOffset + 1L), checkpoint.transactionState().abortedTransactions().stream()
                    .map(transaction -> transaction.markerEndOffsetExclusive()).toList());
            assertEquals(expected, KafkaBookKeeperOwnerRelocationRealTest.consume(client, expected.size()));
            Plan.verifyIfSelected(state.mode, opened);
        }
        KafkaBookKeeperRootVerifierRealScenario.reassign(admin, partition, 1, 0);
        var next = state.one.get(30, TimeUnit.SECONDS);
        assertEquals((long) offset, admin.listOffsets(Map.of(partition, OffsetSpec.latest()))
                .all().get(30, TimeUnit.SECONDS).get(partition).offset());
        var recovered = captureWhenReady(next, budget(next));
        verifyCheckpoint(recovered);
        var checkpoint = recovered.nativeSources().recoveryCheckpoint().orElseThrow();
        assertTrue(recovered.nativeSources().sources().sealedReadRuns().stream().anyMatch(run -> run.activeIndex().isPresent()),
                "takeover native cut lost the recovered former active run's locators");
        assertEquals(checkpoint.producerState(), recovered.nativeSources().protocol().committedProducerState());
        assertEquals(checkpoint.transactionState(), recovered.nativeSources().protocol().transactionState());
        assertEquals(checkpoint.leaderEpochIndex(), recovered.nativeSources().protocol().leaderEpochIndex());
        assertEquals(expected, KafkaBookKeeperOwnerRelocationRealTest.consume(client, expected.size()));
    }

    private static void exerciseCheckpoint(KafkaClusterTestKit cluster, State state) throws Exception {
        var client = new Properties();
        client.put("bootstrap.servers", cluster.bootstrapServers());
        try (var admin = Admin.create(client)) {
            var partition = new TopicPartition("native-cold", 0);
            admin.createTopics(List.of(topic(state.mode, partition.topic()))).all().get(30, TimeUnit.SECONDS);
            verifyTransactionCheckpoint(cluster, admin, client, state.zero.get(30, TimeUnit.SECONDS), state, partition);
        }
    }

    private static NereusKafkaNativeInputV2.Captured captureWhenReady(
            NereusKafkaOwnedProviderRuntime.Opened opened, KafkaBookKeeperReadCellBudgetV2 budget) throws Exception {
        return withCurrentMetadataImage(() -> opened.captureM5Inputs(budget, BOUNDS));
    }

    private static Map<Sha256Digest, List<PhysicalResourceIdV2>> verifyWhenReady(
            NereusKafkaNativeInputV2.Captured captured) throws Exception {
        return withCurrentMetadataImage(() -> captured.verifyInputs(captured.extents(), captured.batches()));
    }

    static <T> T withCurrentMetadataImage(java.util.function.Supplier<CompletionStage<T>> read) throws Exception {
        return withCurrentMetadataImage(read, false);
    }

    static <T> T withCurrentMetadataImage(java.util.function.Supplier<CompletionStage<T>> read, boolean retained) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        var result = new AtomicReference<T>();
        var terminalFailure = new AtomicReference<Throwable>();
        TestUtils.waitForCondition(() -> {
            try {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) throw new java.util.concurrent.TimeoutException("native metadata image wait exhausted its total budget");
                var future = read.get().toCompletableFuture();
                result.set(future.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS));
                return true;
            } catch (Exception | AssertionError failure) {
                if (failure instanceof java.util.concurrent.ExecutionException
                        && failure.getCause() instanceof IllegalStateException
                        && failure.getCause().getSuppressed().length == 0
                        && (!retained || failure.getCause() instanceof NereusKafkaNativeInputV2.CaptureReleasedException)
                        && "native M5 metadata image changed during capture".equals(failure.getCause().getMessage()))
                    return false;
                // TestUtils retries thrown exceptions; finish this wait and rethrow every other failure once.
                terminalFailure.set(failure);
                return true;
            }
        }, 30_000, "actual native metadata image did not remain current through capture");
        if (terminalFailure.get() instanceof Exception failure) throw failure;
        if (terminalFailure.get() instanceof AssertionError failure) throw failure;
        return result.get();
    }

    private static NewTopic topic(String mode, String name) {
        var topic = new NewTopic(name, Map.of(0, List.of(0)));
        if (mode.equals("POLICY")) topic.configs(Map.of(
                TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete",
                TopicConfig.DELETE_RETENTION_MS_CONFIG, "3333",
                TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.25",
                TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "123",
                TopicConfig.COMPRESSION_TYPE_CONFIG, "gzip"));
        if (mode.equals("PLAN") || mode.startsWith("TASK")) topic.configs(Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete",
                TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.123456789", TopicConfig.COMPRESSION_TYPE_CONFIG, "producer"));
        if (mode.equals("TASK_RATIO")) topic.configs(Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete",
                TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "1.0"));
        if (mode.equals("TASK_RATIO_NEAR_ONE")) topic.configs(Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete",
                TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.999999999"));
        if (mode.startsWith("TASK_MAX_LAG")) topic.configs(Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete",
                TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "1.0", TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "1"));
        if (mode.equals("TASK_MIN_LAG")) topic.configs(Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete",
                TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "3600000", TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "3600000"));
        return KafkaBookKeeperReadScopeRealScenario.configureEmptyTail(topic, mode);
    }

    private static final class Plan {
        private static String transactionKey(String mode) {
            return mode.equals("PLAN") ? "transaction-key" : "key";
        }

        private static int sealIfSelected(String mode, NereusKafkaOwnedProviderRuntime.Opened opened,
                Properties client, int offset, List<String> expected) throws Exception {
            if (!mode.equals("PLAN")) return offset;
            int tombstoneOffset = offset++;
            produceTombstone(client, tombstoneOffset);
            expected.add(null);
            while (opened.storage().nativeReadSources().activeRoot().kafkaStartOffset() <= tombstoneOffset && offset < tombstoneOffset + 33) {
                expected.add("filler-" + offset);
                KafkaBookKeeperRootVerifierRealScenario.produce(client, "filler-" + offset, offset++);
            }
            return offset;
        }

        private static void verifyIfSelected(String mode, NereusKafkaOwnedProviderRuntime.Opened opened) throws Exception {
            if (mode.equals("PLAN")) verify(opened);
        }

        private static void verify(NereusKafkaOwnedProviderRuntime.Opened opened) throws Exception {
            var limits = new Caps(BOUNDS.payloadBytes(), BOUNDS.batches(), BOUNDS.records(), BOUNDS.records(),
                    BOUNDS.decodedBytes(), BOUNDS.decodedBytes(), BOUNDS.decodedBytes(), BOUNDS.records(), BOUNDS.records());
            var caps = KafkaBookKeeperSelectedInputRealScenario.caps(limits, 1);
            var allowance = new NereusKafkaNativeInputV2.CaptureAllowance(limits, 0, 0, 4096, 0, 4096, 64);
            var captured = withCurrentMetadataImage(() -> opened.captureM5Inputs(budget(opened), BOUNDS, caps, allowance), true);
            var occupied = opened.nativeM5CompactionAdmission().read().orElseThrow();
            var plan = withCurrentMetadataImage(captured::preparePlan);
            var references = captured.nativeSources().protocol().root().references();
            assertEquals(references.committedProducerState().contentDigest(), plan.protocolRoots().committedProducerStateSha256());
            assertEquals(references.speculativeProducerQueue().contentDigest(), plan.protocolRoots().speculativeQueueSha256());
            assertEquals(references.transactionIndex().contentDigest(), plan.protocolRoots().transactionIndexSha256());
            assertEquals(references.leaderEpochIndex().contentDigest(), plan.protocolRoots().leaderEpochIndexSha256());
            assertEquals(references.checkpointVector().contentDigest(), plan.sourceCut().recoveryCheckpointRootSha256());
            assertEquals(references.activeTail().contentDigest(), plan.protocolRoots().activeTailSha256());
            assertEquals(captured.extents(), plan.sourceCut().sources());
            assertEquals(captured.batches(), plan.inputBatches());
            assertEquals(List.of(TransactionOutcome.COMMITTED, TransactionOutcome.ABORTED, TransactionOutcome.ABORTED),
                    plan.transactions().stream().map(tx -> tx.outcome()).toList());
            var nativeTransactions = captured.nativeSources().protocol().transactionState().completedTransactions();
            assertEquals(nativeTransactions.stream().map(tx -> tx.coordinatorEpoch()).toList(),
                    plan.transactions().stream().map(tx -> tx.coordinatorEpoch()).toList());
            assertEquals(11, plan.keyProofs().stream().filter(key -> key.key().equals(
                    CanonicalBytes.copyOf("transaction-key".getBytes(StandardCharsets.UTF_8)))).findFirst().orElseThrow().latestEligibleOffset(),
                    "aborted DATA replaced the latest committed key proof");
            var tombstone = plan.keyProofs().stream().filter(key -> key.key().equals(CanonicalBytes.copyOf("gone".getBytes(StandardCharsets.UTF_8))))
                    .findFirst().orElseThrow();
            assertTrue(tombstone.olderValueMayReappear(), "raw tombstone received expiry authority before its first cleaning");
            assertEquals(0, tombstone.tombstoneDeadlineMs());
            assertEquals(occupied, opened.nativeM5CompactionAdmission().read().orElseThrow(), "building a plan replaced or released capacity");
            var output = new KafkaSemanticCompactorV1().compileSemantic(plan);
            new KafkaCompactionSemanticValidatorV1().validateSemantic(plan, output);
            assertTrue(output.dispositions().stream().anyMatch(row -> row.disposition() == Disposition.DROP_SUPERSEDED_VALUE));
            assertTrue(output.dispositions().stream().filter(row -> row.offset() < 21)
                    .allMatch(row -> row.disposition() == Disposition.KEEP_TRANSACTION_OR_CONTROL));
            assertTrue(output.dispositions().stream().filter(row -> row.offset() == tombstone.latestEligibleOffset())
                    .allMatch(row -> row.disposition() == Disposition.KEEP_TOMBSTONE_WITHIN_RETENTION));
            var tombstoneOutput = output.batchOutputs().stream().flatMap(batch -> batch.outputBody().stream())
                    .map(body -> MemoryRecords.readableRecords(ByteBuffer.wrap(body.toByteArray())).batches().iterator().next())
                    .filter(batch -> batch.baseOffset() <= tombstone.latestEligibleOffset() && batch.lastOffset() >= tombstone.latestEligibleOffset())
                    .findFirst().orElseThrow();
            assertEquals(plan.policy().capturedNowMs() + plan.policy().deleteRetentionMs(), tombstoneOutput.deleteHorizonMs().orElseThrow());
            verifyOracle(plan, output);
            captured.discard().toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertTrue(opened.nativeM5CompactionAdmission().read().orElseThrow().reservations().isEmpty());
            reject(captured.preparePlan());
        }

        private static void verifyOracle(CompactionPlan plan, KafkaCompactionSemanticOutputV2 output) {
            var oracleInput = ByteBuffer.allocate(plan.inputBatches().stream().mapToInt(batch -> batch.canonicalBody().length()).sum());
            plan.inputBatches().forEach(batch -> oracleInput.put(batch.canonicalBody().toByteArray()));
            oracleInput.flip();
            var retained = output.dispositions().stream().filter(row -> row.retained()).map(row -> row.offset()).collect(java.util.stream.Collectors.toSet());
            var filter = new MemoryRecords.RecordFilter(plan.policy().capturedNowMs(), plan.policy().deleteRetentionMs()) {
                @Override
                protected BatchRetentionResult checkBatchRetention(org.apache.kafka.common.record.RecordBatch batch) {
                    return new BatchRetentionResult(BatchRetention.DELETE_EMPTY, false);
                }

                @Override
                protected boolean shouldRetainRecord(org.apache.kafka.common.record.RecordBatch batch, org.apache.kafka.common.record.Record record) {
                    return retained.contains(record.offset());
                }
            };
            var oracle = MemoryRecords.readableRecords(oracleInput).filterTo(filter, ByteBuffer.allocate(Math.toIntExact(BOUNDS.payloadBytes())),
                    org.apache.kafka.common.utils.BufferSupplier.NO_CACHING).outputBuffer();
            oracle.flip();
            var actual = new java.io.ByteArrayOutputStream();
            for (var batch : output.batchOutputs()) if (batch.outputBody().isPresent()) actual.writeBytes(batch.outputBody().orElseThrow().toByteArray());
            var expectedBytes = new byte[oracle.remaining()];
            oracle.get(expectedBytes);
            org.junit.jupiter.api.Assertions.assertArrayEquals(expectedBytes, actual.toByteArray(), "native Kafka 4.3 RecordBatch filtering differs");
        }

        private static void produceTombstone(Properties client, int offset) throws Exception {
            var config = new Properties();
            config.putAll(client);
            config.put("key.serializer", StringSerializer.class.getName());
            config.put("value.serializer", StringSerializer.class.getName());
            config.put("acks", "all");
            config.put("enable.idempotence", "false");
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
                assertEquals((long) offset, producer.send(new ProducerRecord<>("native-cold", 0, "gone", null)).get(30, TimeUnit.SECONDS).offset());
            }
        }
    }

    private static void verifyPolicy(KafkaClusterTestKit cluster, Admin admin,
            NereusKafkaOwnedProviderRuntime.Opened opened, KafkaBookKeeperReadCellBudgetV2 budget,
            NereusKafkaNativeInputV2.Captured captured, State state) throws Exception {
        var policy = captured.topicPolicy();
        var image = cluster.brokers().get(0).metadataCache().getImage();
        assertTrue(policy.metadataOffset() >= 0 && policy.metadataOffset() <= image.provenance().lastContainedOffset());
        assertEquals(policy.metadataOffset() + 1, policy.generation());
        assertEquals(image.features().metadataVersionOrThrow().featureLevel(), policy.kafkaFeatureLevel());
        assertTrue(policy.config().compact);
        assertTrue(policy.config().delete);
        assertEquals(3333, policy.config().deleteRetentionMs);
        assertEquals(0.25, policy.config().minCleanableRatio);
        assertEquals(123, policy.config().compactionLagMs);
        assertEquals(BrokerCompressionType.GZIP, policy.config().compressionType);
        assertEquals(OutputCompressionPolicy.GZIP, policy.compressionPolicy());
        KafkaBookKeeperM5PolicyRealScenario.withConfigScanDrain(opened, admin);
        TestUtils.waitForCondition(() -> {
            try {
                var current = opened.nativeM5InputCut().policy();
                return current.config().deleteRetentionMs == 7777 && current.config().compressionType == BrokerCompressionType.PRODUCER;
            } catch (RuntimeException updating) {
                return false;
            }
        }, 30_000, "actual native log did not apply the KRaft compaction policy");
        int scans = state.sdkCloses.get();
        reject(captured.verifyInputs(captured.extents(), captured.batches()));
        assertEquals(scans, state.sdkCloses.get(), "stale topic policy opened a native input session");
        var next = captureWhenReady(opened, budget);
        assertTrue(next.topicPolicy().generation() > policy.generation());
        assertEquals(7777, next.topicPolicy().config().deleteRetentionMs);
        assertEquals(BrokerCompressionType.PRODUCER, next.topicPolicy().config().compressionType);
        assertEquals(OutputCompressionPolicy.PRODUCER, next.topicPolicy().compressionPolicy());
        assertEquals(captured.nativeSources(), next.nativeSources(), "policy change unexpectedly rewrote the physical input cut");
        assertEquals(captured.batches(), next.batches());
        assertEquals(next.extents().size(), verifyWhenReady(next).size());
        verifyBrokerDefaultPolicy(cluster, admin, opened, budget, next, state);
    }

    private static void verifyBrokerDefaultPolicy(KafkaClusterTestKit cluster, Admin admin,
            NereusKafkaOwnedProviderRuntime.Opened opened, KafkaBookKeeperReadCellBudgetV2 budget,
            NereusKafkaNativeInputV2.Captured before, State state) throws Exception {
        var publisher = cluster.brokers().get(0).brokerMetadataPublisher();
        var original = publisher.dynamicConfigPublisher();
        var entered = new CompletableFuture<Void>();
        var release = new CompletableFuture<Void>();
        publisher.dynamicConfigPublisher_$eq(new DynamicConfigPublisher(cluster.brokers().get(0).config(),
                (message, failure) -> {
                    throw new AssertionError(message, failure);
                },
                scala.collection.immutable.Map$.MODULE$.empty(), "Broker") {
            @Override
            public void onMetadataUpdate(MetadataDelta delta, MetadataImage image) {
                var props = image.configs().configProperties(new ConfigResource(ConfigResource.Type.BROKER, "0"));
                if ("60000".equals(props.getProperty(CleanerConfig.LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP))) {
                    entered.complete(null);
                    release.join();
                }
                original.onMetadataUpdate(delta, image);
            }
        });
        try {
            alterBrokerDefaultWhileHeld(admin, opened, budget, before, state, entered);
        } finally {
            release.complete(null);
            publisher.dynamicConfigPublisher_$eq(original);
        }
        TestUtils.waitForCondition(() -> {
            try {
                return opened.nativeM5InputCut().policy().config().maxCompactionLagMs == 60000;
            } catch (RuntimeException updating) {
                return false;
            }
        }, 30_000, "actual native log did not apply the Broker default policy");
        int scans = state.sdkCloses.get();
        reject(before.verifyInputs(before.extents(), before.batches()));
        assertEquals(scans, state.sdkCloses.get(), "stale Broker default opened a native input session");
        var next = captureWhenReady(opened, budget);
        assertTrue(next.topicPolicy().generation() > before.topicPolicy().generation());
        assertEquals(60000, next.topicPolicy().config().maxCompactionLagMs);
        assertEquals(7777, next.topicPolicy().config().deleteRetentionMs, "Broker default lost the topic override");
        assertEquals(before.nativeSources(), next.nativeSources());
        assertEquals(before.batches(), next.batches());
        assertEquals(next.extents().size(), verifyWhenReady(next).size());
    }

    private static void alterBrokerDefaultWhileHeld(Admin admin, NereusKafkaOwnedProviderRuntime.Opened opened,
            KafkaBookKeeperReadCellBudgetV2 budget, NereusKafkaNativeInputV2.Captured before, State state,
            CompletableFuture<Void> entered) throws Exception {
        var resource = new ConfigResource(ConfigResource.Type.BROKER, "0");
        admin.incrementalAlterConfigs(Map.of(resource, List.of(new AlterConfigOp(
                new ConfigEntry(CleanerConfig.LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP, "60000"), AlterConfigOp.OpType.SET))))
                .all().get(30, TimeUnit.SECONDS);
        entered.get(30, TimeUnit.SECONDS);
        int scans = state.sdkCloses.get();
        reject(opened.captureM5Inputs(budget, BOUNDS));
        reject(before.verifyInputs(before.extents(), before.batches()));
        assertEquals(scans, state.sdkCloses.get(), "unapplied metadata policy opened a native input session");
    }

    static KafkaBookKeeperReadCellBudgetV2 budget(NereusKafkaOwnedProviderRuntime.Opened opened) {
        var active = opened.storage().nativeReadSources().activeRoot();
        var binding = new BindingIdentity(active.bindingId(),
                Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(active.topicIncarnation())), active.storageEpochId().digest());
        var share = KafkaBookKeeperReadCellBudgetV2.Usage.forRunSource(BOUNDS);
        return new KafkaBookKeeperReadCellBudgetV2(active.providerScopeId(), share, Map.of(binding, share));
    }

    private static void produceForeign(Properties client, long offset) throws Exception {
        var config = new Properties();
        config.putAll(client);
        config.put("key.serializer", StringSerializer.class.getName());
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("acks", "all");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            assertEquals(offset, producer.send(new ProducerRecord<>("native-foreign", 0, "key", "foreign-" + offset))
                    .get(30, TimeUnit.SECONDS).offset());
        }
    }

    private static void reject(CompletionStage<?> operation) {
        assertThrows(java.util.concurrent.ExecutionException.class, () -> operation.toCompletableFuture().get(30, TimeUnit.SECONDS));
    }

    private static void verifyLifetime(NereusKafkaOwnedProviderRuntime.Opened opened,
            KafkaBookKeeperReadCellBudgetV2 budget, State state) throws Exception {
        state.hold.set(true);
        var capture = opened.captureM5Inputs(budget, BOUNDS).toCompletableFuture();
        var root = state.held.get(30, TimeUnit.SECONDS);
        state.cleanupLedger = opened.storage().nativeReadSources().sealedRoots().stream()
                .filter(candidate -> candidate.kafkaEndOffsetExclusive().orElseThrow() > candidate.kafkaStartOffset())
                .toList().get(1).ledgerIdentity().ledgerId();
        assertEquals(1, budget.usage().owners());
        assertTrue(capture.cancel(true));
        reject(opened.captureM5Inputs(budget, BOUNDS));
        var authority = state.route.read(root.resource().authorityKey()).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
        assertEquals(1, M5TargetDeleteAuthorityCodecV1.decodeAuthority(authority.canonicalStoredBytes()).activeWriterTickets().size());
        var close = opened.closeAsync();
        assertFalse(close.isDone(), "owner close abandoned a cancelled input scan's actual SDK close");
        assertEquals(1, budget.usage().readSlots());
        state.releaseClose.complete(null);
        if (state.mode.equals("CLEANUP")) {
            reject(close);
            var field = NereusKafkaOwnedProviderRuntime.Opened.class.getDeclaredField("inputTicketCleanup");
            field.setAccessible(true);
            var retained = (java.util.Set<?>) field.get(opened);
            assertEquals(1, retained.size(), "no-session invocation was not retained by its actual owner");
            var cleanup = (KafkaBookKeeperRunSourceV2.TicketCleanupException) retained.iterator().next();
            assertNotNull(cleanup.getCause());
            assertEquals("native run reader owner is draining", cleanup.getCause().getMessage());
            assertEquals(state.cleanupOperation.get(), cleanup.operationId());
            assertEquals(1, state.sdkCloses.get());
            state.blockCleanup.set(false);
            close = opened.closeAsync();
        }
        close.get(30, TimeUnit.SECONDS);
        if (state.mode.equals("CLEANUP")) assertTrue(state.cleanupFailures.get() > 0);
        else assertEquals(0, state.cleanupFailures.get());
        assertEquals(1, state.sdkCloses.get(), "close retry redispatched an input SDK session");
        if (state.mode.equals("CLEANUP")) {
            var cleanupResource = new PhysicalResourceIdV2.BookKeeperLedger(root.resource().namespace(), state.cleanupLedger);
            var cleaned = state.route.read(cleanupResource.authorityKey()).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
            assertTrue(M5TargetDeleteAuthorityCodecV1.decodeAuthority(cleaned.canonicalStoredBytes()).activeWriterTickets().isEmpty());
        }
        assertEquals(0, budget.usage().owners());
        assertEquals(0, budget.usage().readSlots());
        var terminal = state.route.read(root.resource().authorityKey()).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
        assertTrue(M5TargetDeleteAuthorityCodecV1.decodeAuthority(terminal.canonicalStoredBytes()).activeWriterTickets().isEmpty());
    }

    private static final class Capacity {
        private static void exercise(KafkaClusterTestKit cluster, State state) throws Exception {
            var client = new Properties();
            client.put("bootstrap.servers", cluster.bootstrapServers());
            try (var admin = Admin.create(client)) {
                admin.createTopics(List.of(topic("CAPACITY", "native-cold"))).all().get(30, TimeUnit.SECONDS);
                var opened = state.zero.get(30, TimeUnit.SECONDS);
                int produced = 0;
                while (opened.storage().nativeReadSources().sealedRoots().stream()
                        .filter(root -> root.kafkaEndOffsetExclusive().orElseThrow() > root.kafkaStartOffset()).count() < 2 && produced < 32) {
                    KafkaBookKeeperRootVerifierRealScenario.produce(client, "input-" + produced, produced);
                    produced++;
                }
                verifyCellCapacity(admin, client, opened, budget(opened), state);
            }
        }

        private static void verifyCellCapacity(Admin admin, Properties client, NereusKafkaOwnedProviderRuntime.Opened opened,
                KafkaBookKeeperReadCellBudgetV2 budget, State state) throws Exception {
            var limits = new Caps(BOUNDS.payloadBytes(), BOUNDS.batches(), BOUNDS.records(), BOUNDS.records(),
                    BOUNDS.decodedBytes(), BOUNDS.decodedBytes(), BOUNDS.decodedBytes(), BOUNDS.records(), BOUNDS.records());
            var caps = new KafkaCompactionAdmissionV1.CellCaps(1, limits.maximumDirtyBytes(), limits.maximumInputBatches(),
                    limits.maximumRecords(), limits.maximumDistinctKeys(), limits.maximumKeyBytes(), limits.maximumOutputBytes(),
                    limits.maximumIndexBytes(), limits.maximumTransactions(), limits.maximumTombstones(), 60000, 1,
                    4096, 1, 4096, 64);
            var allowance = new NereusKafkaNativeInputV2.CaptureAllowance(limits, 0, 0, 4096, 0, 4096, 64);
            var admission = opened.nativeM5CompactionAdmission();
            state.failInputRead.set(true);
            var failed = assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> captureWithCapacity(opened, budget, caps, allowance));
            assertInstanceOf(NereusKafkaNativeInputV2.CaptureReleasedException.class, failed.getCause());
            Throwable original = failed;
            while (original.getCause() != null) original = original.getCause();
            assertEquals("injected native M5 input read failure", original.getMessage());
            assertEquals(0, budget.usage().owners());
            assertTrue(admission.read().orElseThrow().reservations().isEmpty(), "terminated failed scan leaked persisted capacity");
            var captured = captureWithCapacity(opened, budget, caps, allowance);
            var occupied = admission.read().orElseThrow();
            assertEquals(1, occupied.reservations().size());
            assertEquals(0, budget.usage().owners(), "retained inputs must outlive the scan's local read slot");
            int scans = state.sdkCloses.get();
            rejectCellCap(opened.captureM5Inputs(budget, BOUNDS, caps, allowance));
            assertEquals(scans, state.sdkCloses.get(), "Cell exhaustion dispatched a native input reader");
            assertEquals(occupied, admission.read().orElseThrow());
            assertEquals(KafkaCompactionAdmissionV1.Outcome.EXISTING_EXACT, admission.install(caps));
            assertEquals(occupied, admission.read().orElseThrow(), "reinstallation reset occupied capacity");

            admin.createTopics(List.of(new NewTopic("native-foreign", Map.of(0, List.of(0)))))
                    .all().get(30, TimeUnit.SECONDS);
            for (int offset = 0; offset < 12; offset++) produceForeign(client, offset);
            var foreignOwner = state.foreign.get(30, TimeUnit.SECONDS);
            assertTrue(foreignOwner.storage().nativeReadSources().sealedRoots().stream()
                    .anyMatch(root -> root.kafkaEndOffsetExclusive().orElseThrow() > root.kafkaStartOffset()));
            captureWhenReady(foreignOwner, budget(foreignOwner));
            assertEquals(opened.nativeM5ProtocolCell(), foreignOwner.nativeM5ProtocolCell());
            assertEquals(occupied, foreignOwner.nativeM5CompactionAdmission().read().orElseThrow());
            scans = state.sdkCloses.get();
            rejectCellCap(foreignOwner.captureM5Inputs(budget(foreignOwner), BOUNDS, caps, allowance));
            assertEquals(scans, state.sdkCloses.get(), "another Binding bypassed the same Cell quota");

            var extents = captured.extents();
            var batches = captured.batches();
            state.hold.set(true);
            var verification = captured.verifyInputs(extents, batches).toCompletableFuture();
            state.held.get(30, TimeUnit.SECONDS);
            var discarded = captured.discard().toCompletableFuture();
            assertFalse(discarded.isDone(), "discard released capacity before accepted native verification drained");
            assertEquals(occupied, admission.read().orElseThrow());
            assertTrue(verification.cancel(true));
            assertEquals(occupied, admission.read().orElseThrow(), "cancelled observer released its actual operation");
            state.releaseClose.complete(null);
            discarded.get(30, TimeUnit.SECONDS);
            assertTrue(admission.read().orElseThrow().reservations().isEmpty());
            assertThrows(IllegalStateException.class, captured::batches);
            reject(captured.verifyInputs(extents, batches));

            var foreignCapture = captureWithCapacity(foreignOwner, budget(foreignOwner), caps, allowance);
            assertEquals(1, admission.read().orElseThrow().reservations().size());
            state.cleanupLedger = foreignCapture.nativeSources().sources().sealedRoots().stream()
                    .filter(root -> root.kafkaEndOffsetExclusive().orElseThrow() > root.kafkaStartOffset())
                    .findFirst().orElseThrow().ledgerIdentity().ledgerId();
            var unknown = assertThrows(java.util.concurrent.ExecutionException.class, () -> verifyWhenReady(foreignCapture));
            assertInstanceOf(KafkaBookKeeperRunSourceV2.TicketCleanupException.class, unknown.getCause());
            assertTrue(state.cleanupFailures.get() > 0);
            reject(foreignCapture.discard());
            assertEquals(1, admission.read().orElseThrow().reservations().size(), "unresolved ticket cleanup released capacity");
            state.blockCleanup.set(false);
            foreignOwner.closeAsync().get(30, TimeUnit.SECONDS);
            assertTrue(admission.read().orElseThrow().reservations().isEmpty(), "Owner drain leaked its retained capture");
            verifyNoDispatchOwner(opened, budget, caps, allowance, admission, state);
        }

        private static void verifyNoDispatchOwner(NereusKafkaOwnedProviderRuntime.Opened opened,
                KafkaBookKeeperReadCellBudgetV2 budget, KafkaCompactionAdmissionV1.CellCaps caps,
                NereusKafkaNativeInputV2.CaptureAllowance allowance, KafkaCompactionAdmissionV1 admission, State state) throws Exception {
            state.cleanupLedger = opened.storage().nativeReadSources().sealedRoots().stream()
                    .filter(root -> root.kafkaEndOffsetExclusive().orElseThrow() > root.kafkaStartOffset())
                    .findFirst().orElseThrow().ledgerIdentity().ledgerId();
            state.withholdInputAdmission.set(true);
            int scans = state.sdkCloses.get();
            var failure = assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> captureWithCapacity(opened, budget, caps, allowance));
            assertInstanceOf(KafkaBookKeeperRunSourceV2.TicketCleanupException.class, failure.getCause());
            assertEquals(scans, state.sdkCloses.get(), "uncertain ticket acquisition dispatched an input SDK session");
            assertEquals(1, admission.read().orElseThrow().reservations().size());
            assertEquals(0, budget.usage().owners());
            var closeFailure = opened.closeAsync().handle((ignored, error) -> error).get(30, TimeUnit.SECONDS);
            if (closeFailure != null) {
                while ((closeFailure instanceof java.util.concurrent.CompletionException
                        || closeFailure instanceof java.util.concurrent.ExecutionException) && closeFailure.getCause() != null)
                    closeFailure = closeFailure.getCause();
                assertInstanceOf(KafkaBookKeeperRunSourceV2.TicketCleanupException.class, closeFailure);
                assertEquals(1, admission.read().orElseThrow().reservations().size(), "unresolved no-dispatch cleanup released capacity");
                assertEquals(com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome.APPLIED_EXACT,
                        state.lateInputAdmission.get().get().toCompletableFuture().get(30, TimeUnit.SECONDS));
                opened.closeAsync().get(30, TimeUnit.SECONDS);
            } else {
                // A real intervening authority revision can also exclude the old conditional acquisition.
                assertEquals(com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome.DEFINITIVE_CONFLICT,
                        state.lateInputAdmission.get().get().toCompletableFuture().get(30, TimeUnit.SECONDS));
            }
            assertTrue(admission.read().orElseThrow().reservations().isEmpty());
            assertEquals(scans, state.sdkCloses.get(), "no-dispatch cleanup retry reopened an input SDK session");
        }

        private static NereusKafkaNativeInputV2.Captured captureWithCapacity(NereusKafkaOwnedProviderRuntime.Opened opened,
                KafkaBookKeeperReadCellBudgetV2 budget, KafkaCompactionAdmissionV1.CellCaps caps,
                NereusKafkaNativeInputV2.CaptureAllowance allowance) throws Exception {
            return withCurrentMetadataImage(() -> opened.captureM5Inputs(budget, BOUNDS, caps, allowance), true);
        }

        private static void rejectCellCap(CompletionStage<?> operation) {
            var failure = assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> operation.toCompletableFuture().get(30, TimeUnit.SECONDS));
            assertTrue(failure.getCause().getMessage().contains("REJECTED_CAP"), "rejection did not come from persisted Cell capacity");
        }

    }

    /** Pre-publication byte/Fetch validation only; the candidate is never installed as a native selector. */
    private static final class PreferredRead {
        static void verify(NereusKafkaPreparedTaskV2 prepared, NereusKafkaNativeInputV2.Captured captured,
                List<VerifiedPart> parts, com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2 source,
                NereusKafkaOwnedProviderRuntime.Opened opened) throws Exception {
            var descriptor = KafkaSealedBookKeeperDescriptorV2.create(prepared.plan(), prepared.semantic(), prepared.layout(), parts, captured.recoveryState());
            var sdk = source.newSession();
            try {
                int encoded = Math.toIntExact(prepared.task().parts().stream().mapToLong(part -> part.length()).sum());
                var view = new KafkaSealedBookKeeperReaderV2(sdk, source::captureExactTarget, encoded,
                        new KafkaSealedBookKeeperReaderV2.DecodingBounds(BOUNDS.records(), BOUNDS.decodedBytes()), Runnable::run)
                        .recover(descriptor).toCompletableFuture().get(30, TimeUnit.SECONDS);
                var snapshot = snapshot(opened);
                long end = descriptor.sourceCut().coverage().exclusiveEnd();
                assertTrue(snapshot.runTable().runs().stream().filter(run -> run.endOffsetExclusive() > end).allMatch(KafkaBookKeeperReadRunV1::active),
                        "fixture raw tail must use the actual active writer handle, as ordinary native Fetch does");
                var sessionField = NereusKafkaOwnedProviderRuntime.Opened.class.getDeclaredField("session");
                sessionField.setAccessible(true);
                var live = (BookKeeperCellSession) sessionField.get(opened);
                var reader = new KafkaBookKeeperTargetedReaderV1(live, 2);
                verifyReads(prepared, view, snapshot, reader, live, opened);
                verifyIsolation(view, snapshot, reader);
            } finally {
                sdk.closeAsync().toCompletableFuture().get(30, TimeUnit.SECONDS);
            }
        }

        private static KafkaBookKeeperReadSnapshotV1 snapshot(NereusKafkaOwnedProviderRuntime.Opened opened) {
            var nativeCut = opened.storage().nativeInputSources();
            var protocol = nativeCut.protocol();
            long generation = protocol.root().references().activeTail().generation();
            var runs = new ArrayList<KafkaBookKeeperReadRunV1>();
            for (var run : nativeCut.sources().sealedReadRuns())
                runs.add(new KafkaBookKeeperReadRunV1(run.runBinding(), run.handle(), run.startOffset(), run.endOffsetExclusive(),
                        generation, run.activeIndex(), run.sealedDirectory()));
            runs.addAll(KafkaBookKeeperReadSnapshotV1.fromActive(protocol).runTable().runs());
            return new KafkaBookKeeperReadSnapshotV1(protocol.root(), new KafkaBookKeeperRunTableV1(runs), protocol.transactionState());
        }

        private static KafkaBookKeeperSequentialReadRequestV1 request(long offset, long budget, KafkaReadIsolationV1 isolation) {
            return new KafkaBookKeeperSequentialReadRequestV1(offset, isolation, budget, Optional.empty());
        }

        private static void verifyReads(NereusKafkaPreparedTaskV2 prepared, KafkaBookKeeperReadViewV2 view,
                KafkaBookKeeperReadSnapshotV1 snapshot, KafkaBookKeeperTargetedReaderV1 reader,
                BookKeeperCellSession sdk, NereusKafkaOwnedProviderRuntime.Opened opened) throws Exception {
            long start = snapshot.root().frontiers().trimStartOffset();
            long cutEnd = view.descriptor().sourceCut().coverage().exclusiveEnd();
            var tail = opened.storage().read(request(cutEnd, Long.MAX_VALUE, KafkaReadIsolationV1.REPLICA))
                    .toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertEquals(KafkaBookKeeperReadOutcomeV1.FOUND, tail.outcome(), "fixture lacks actual post-cut Produce data");
            var prefix = prepared.semantic().batchOutputs().stream().flatMap(batch -> batch.outputBody().stream()).toList();
            assertFalse(prefix.isEmpty());
            assertFalse(view.gaps().isEmpty(), "fixture lacks a deleted prefix offset");
            var full = reader.readSequential(snapshot, request(start, Long.MAX_VALUE, KafkaReadIsolationV1.REPLICA), view)
                    .toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertEquals(KafkaBookKeeperReadOutcomeV1.FOUND, full.outcome(), full.detail());
            var expected = new ArrayList<>(prefix);
            tail.batches().forEach(batch -> expected.add(batch.rawAssignedRecordBatch()));
            assertEquals(expected, full.batches().stream().map(batch -> batch.rawAssignedRecordBatch()).toList());
            assertEquals(0, full.batches().get(0).entryId(), "preferred Fetch fabricated a nonzero physical locator");
            assertEquals(view.selectBatches(start, cutEnd, Long.MAX_VALUE).get(0).startOffset(), full.batches().get(0).startOffset());
            var rawCalls = new AtomicInteger();
            var first = reader.readSequential(snapshot, request(start, 1, KafkaReadIsolationV1.REPLICA), view, (handle, entry) -> {
                rawCalls.incrementAndGet();
                return sdk.readExactEntry(handle, entry);
            }).toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertEquals(List.of(prefix.get(0)), first.batches().stream().map(batch -> batch.rawAssignedRecordBatch()).toList());
            assertEquals(0, rawCalls.get(), "oversized preferred batch dispatched an obsolete predecessor/tail read");
            long prefixBytes = prefix.stream().mapToLong(CanonicalBytes::length).sum();
            long justShort = prefixBytes + tail.batches().get(0).rawAssignedRecordBatch().length() - 1;
            var limited = reader.readSequential(snapshot, request(start, justShort, KafkaReadIsolationV1.REPLICA), view)
                    .toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertEquals(prefix, limited.batches().stream().map(batch -> batch.rawAssignedRecordBatch()).toList(),
                    "raw tail took a second first-oversized-batch exception after the preferred prefix");
            var failure = reader.readSequential(snapshot, request(start, Long.MAX_VALUE, KafkaReadIsolationV1.REPLICA), view,
                    (handle, entry) -> sdk.readExactEntry(handle, entry).thenCompose(actual -> CompletableFuture.failedFuture(
                            new java.util.concurrent.TimeoutException("completed raw tail read response lost"))))
                    .toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertEquals(KafkaBookKeeperReadOutcomeV1.PROVIDER_FAILURE, failure.outcome(), "preferred bytes masked a failed actual raw SDK read");
            assertTrue(failure.batches().isEmpty());
        }

        private static void verifyIsolation(KafkaBookKeeperReadViewV2 view, KafkaBookKeeperReadSnapshotV1 snapshot,
                KafkaBookKeeperTargetedReaderV1 reader) throws Exception {
            long end = view.descriptor().sourceCut().coverage().exclusiveEnd();
            var root = snapshot.root();
            var frontiers = root.frontiers();
            var isolated = new com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionProtocolStateV1(root.fence(), root.stateVersion(),
                    new com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFrontiersV1(frontiers.trimStartOffset(), frontiers.allocatedEndOffset(),
                            frontiers.durableEndOffset(), frontiers.readableEndOffset(), end, end), root.references());
            var captured = new KafkaBookKeeperReadSnapshotV1(isolated, snapshot.runTable(), snapshot.transactionState());
            var committed = reader.readSequential(captured, request(frontiers.trimStartOffset(), Long.MAX_VALUE, KafkaReadIsolationV1.READ_COMMITTED), view)
                    .toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertEquals(view.selectBatches(frontiers.trimStartOffset(), end, Long.MAX_VALUE).stream().map(batch -> batch.rawRecordBatch()).toList(),
                    committed.batches().stream().map(batch -> batch.rawAssignedRecordBatch()).toList());
            assertEquals(KafkaBookKeeperReadOutcomeV1.END_OF_SNAPSHOT, reader.readSequential(captured,
                    request(end, 1, KafkaReadIsolationV1.READ_COMMITTED), view).toCompletableFuture().get(30, TimeUnit.SECONDS).outcome());
            assertEquals(KafkaBookKeeperReadOutcomeV1.OFFSET_OUT_OF_RANGE, reader.readSequential(snapshot,
                    request(frontiers.readableEndOffset() + 1, 1, KafkaReadIsolationV1.REPLICA), view)
                    .toCompletableFuture().get(30, TimeUnit.SECONDS).outcome());
            var fence = root.fence();
            var foreign = new com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionProtocolStateV1(
                    new com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1(fence.bindingId(), fence.topicIncarnation(),
                            fence.partitionId() + 1, fence.bindingGeneration(), fence.storageEpochId(), fence.ownerEpoch(), fence.kafkaLeaderEpoch()),
                    root.stateVersion(), frontiers, root.references());
            var foreignSnapshot = new KafkaBookKeeperReadSnapshotV1(foreign, new KafkaBookKeeperRunTableV1(List.of()), snapshot.transactionState());
            var attempted = new AtomicInteger();
            assertThrows(IllegalStateException.class, () -> reader.readSequential(foreignSnapshot,
                    request(frontiers.trimStartOffset(), Long.MAX_VALUE, KafkaReadIsolationV1.REPLICA), view, (handle, entry) -> {
                        attempted.incrementAndGet();
                        throw new AssertionError("foreign preferred prefix dispatched native IO");
                    }));
            assertEquals(0, attempted.get());
        }
    }

    /** Actual selected native Task, ordinary Broker Fetch, and its retained cache/SDK lifetime. */
    private static final class Published {
        static void verify(Task task, NereusKafkaPreparedTaskV2 prepared, NereusKafkaNativeInputV2.Captured captured,
                KafkaCompactionAdmissionV1.State formal, KafkaCompactionAdmissionV1 admission,
                NereusKafkaOwnedProviderRuntime.Opened opened, Properties client) throws Exception {
            var descriptor = KafkaSealedBookKeeperDescriptorV2.create(prepared.plan(), prepared.semantic(), prepared.layout(),
                    task.outputParts, captured.recoveryState());
            long end = descriptor.sourceCut().coverage().exclusiveEnd();
            task.selectedCutEnd = end;
            var tail = opened.storage().read(PreferredRead.request(end, Long.MAX_VALUE, KafkaReadIsolationV1.REPLICA))
                    .toCompletableFuture().get(30, TimeUnit.SECONDS);
            KafkaBookKeeperReadScopeRealScenario.verifyPublicationTail(task.mode, opened, end, tail);
            var initialExpected = new ArrayList<>(prepared.semantic().batchOutputs().stream().flatMap(batch -> batch.outputBody().stream()).toList());
            tail.batches().forEach(batch -> initialExpected.add(batch.rawAssignedRecordBatch()));
            List<CanonicalBytes> expected = List.copyOf(initialExpected);
            assertTrue(prepared.semantic().dispositions().stream().anyMatch(row -> row.disposition() == Disposition.DROP_SUPERSEDED_VALUE),
                    "fixture lacks an actually deleted prefix record");
            NativeFallbackRelease.holdRawRead(task, opened);
            var publication = prepared.publish().toCompletableFuture();
            if (task.mode.equals("TASK_OUTPUT_PUBLISH_CACHE_BUDGET_UNKNOWN")) {
                reject(publication);
                assertFalse(task.faultOnce.get(), "actual cache budget persistence fault was not exercised");
                assertTrue(task.blockCacheBudgetReads.get());
                assertEquals(formal, admission.read().orElseThrow(), "cache budget UNKNOWN released actual Task capacity");
                assertEquals(captured.predecessorSelector(), SelectedRollover.authority(task).selectorProjection(),
                        "unreadable cache budget allowed the selected descriptor CAS");
                var rawRead = opened.storage().read(PreferredRead.request(0, Long.MAX_VALUE, KafkaReadIsolationV1.REPLICA))
                        .toCompletableFuture().get(30, TimeUnit.SECONDS);
                assertEquals(KafkaBookKeeperReadOutcomeV1.FOUND, rawRead.outcome(),
                        "cache budget uncertainty stopped the still selected raw reader");
                task.verifyWriterTickets(true);
                task.blockCacheBudgetReads.set(false);
                publication = prepared.publish().toCompletableFuture();
            }
            if (task.mode.equals("TASK_OUTPUT_PUBLISH_UNKNOWN")) {
                var uncertain = publication.handle((outcome, failure) -> failure == null ? outcome : failure).get(30, TimeUnit.SECONDS);
                assertTrue(uncertain instanceof Throwable || uncertain == com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome.OUTCOME_UNKNOWN,
                        "lost applied selector response unexpectedly became a known publication");
                assertFalse(task.faultOnce.get(), "actual selector publication fault was not exercised");
                assertEquals(formal, admission.read().orElseThrow(), "publication UNKNOWN released its original Task capacity");
                task.verifyWriterTicketsRetained();
                task.blockSelectorReads.set(false);
                publication = prepared.publish().toCompletableFuture();
            }
            if (task.mode.equals("TASK_OUTPUT_PUBLISH_CLOSE_HELD")) {
                publication = withCurrentMetadataImage(() ->
                        KafkaBookKeeperRootVerifierRealScenario.publicationUntilOutputClose(prepared, task.outputCloseSeen));
                assertTrue(task.outputCloseSeen.isDone(), "selected Task did not close its actual SDK");
                assertFalse(publication.isDone(), "withheld native close completed Task publication");
                task.verifyWriterTicketsRetained();
            } else {
                var outcome = withCurrentMetadataImage(() -> KafkaBookKeeperRootVerifierRealScenario.publicationUntilKnown(prepared));
                assertTrue(outcome == com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome.APPLIED_EXACT
                        || outcome == com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome.EXISTING_EXACT,
                        "actual native Task was not selected: " + outcome);
                task.verifyWriterTickets(false);
            }
            String recordRoot = task.nativeTaskKey.substring(0, task.nativeTaskKey.indexOf("/v2/"));
            KafkaBookKeeperRootVerifierRealScenario.verifyReplacementSemantics(task.actual, recordRoot, descriptor);
            KafkaBookKeeperReadScopeRealScenario.verifyFetch(descriptor, expected, opened, client);
            SelectedRollover.verify(task, prepared, descriptor, formal, admission, opened, client);
            NativeFallbackRelease.verify(task, prepared, descriptor, expected, formal, admission, opened, client);
            formal = KafkaBookKeeperSelectedInputRealScenario.afterCapture(task.mode, formal, admission);
            assertEquals(formal, admission.read().orElseThrow(), "selected read cache lost its Task charge");
            var persistedBudget = KafkaBookKeeperRootVerifierRealScenario.selectedCacheBudget(task.actual, recordRoot, descriptor);
            assertEquals(formal.reservations().get(0), persistedBudget.taskReservation(),
                    "selected output did not preserve its actual Task resource envelope");
            assertEquals(captured.decodingBounds(), persistedBudget.decodingBounds());
            var discarded = captured.discard().toCompletableFuture();
            assertFalse(discarded.isDone(), "discard released a cache still selected by the actual Broker");
            assertEquals(formal, admission.read().orElseThrow());
            var recompaction = KafkaBookKeeperSelectedInputRealScenario.recompactIfSelected(task.mode, descriptor.sourceCut().identity().binding(),
                    descriptor, formal, admission, opened, client, KafkaBookKeeperReadScopeRealScenario::verifyFetch,
                    new KafkaBookKeeperSelectedInputRealScenario.ReadGate(task.holdFallbackRead, task.fallbackReadEntered, task.releaseFallbackRead),
                    () -> SelectedRollover.authority(task).selectorProjection(), discarded, task.recompactionFault);
            if (recompaction.isPresent()) {
                descriptor = recompaction.get().descriptor();
                expected = recompaction.get().expected();
                task.outputParts = recompaction.get().parts();
                task.selectedCutEnd = descriptor.sourceCut().coverage().exclusiveEnd();
                KafkaBookKeeperReadScopeRealScenario.verify(task.actual, task.nativeTaskKey, task.ownerKey, descriptor,
                        recompaction.get().before(), opened, recompaction.get().retirement(), task.recompactionFault);
            }
            task.gcInterruption.reclaim(task.mode, opened);
            if (task.mode.equals("TASK_OUTPUT_PUBLISH_CLOSE_HELD")) {
                assertTrue(publication.cancel(true));
                var ownerClose = opened.closeAsync();
                assertFalse(ownerClose.isDone(), "Owner closed before its actual selected Task SDK drained");
                assertFalse(discarded.isDone());
                task.releaseOutputClose.complete(null);
                ownerClose.get(30, TimeUnit.SECONDS);
            } else opened.closeAsync().get(30, TimeUnit.SECONDS);
            discarded.get(30, TimeUnit.SECONDS);
            task.registered = descriptor.task();
            SelectedRollover.verifyClosedTickets(task);
            assertTrue(admission.read().orElseThrow().reservations().isEmpty(), "actual read Owner drain leaked selected cache capacity");
            if (task.mode.contains("_COLD")) verifyColdOwner(task, descriptor, expected, admission, client);
        }

        private static void verifyColdOwner(Task task, KafkaSealedBookKeeperDescriptorV2 descriptor,
                List<CanonicalBytes> expected, KafkaCompactionAdmissionV1 admission, Properties client) throws Exception {
            KafkaBookKeeperReadScopeRealScenario.verifyColdOwner(task.actual, task.nativeTaskKey, task.selectorKey,
                    descriptor, expected, admission, task.coldOwner, task.registered.taskIdSha256(), client,
                    task.coldScope, offset -> task.produce(client, offset), KafkaBookKeeperReadScopeRealScenario::verifyFetch);
            SelectedRollover.verifyClosedTickets(task);
        }

        static void verifyDecision(Task task) throws Exception {
            String root = task.nativeTaskKey.substring(0, task.nativeTaskKey.indexOf("/v2/"));
            var cut = com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.decodeSourceCut(task.registered.sourceCut());
            var archived = task.actual.get(root + "/" + M5TaskSelectionDecisionV2.key(cut.identity().binding(), task.registered.taskIdSha256()))
                    .get(30, TimeUnit.SECONDS);
            var decision = M5TaskSelectionDecisionV2.decode(CanonicalBytes.copyOf(archived.value()));
            assertEquals(M5TaskSelectionDecisionV2.Outcome.SELECTED, decision.outcome());
            assertEquals(task.registered.taskIdSha256(), decision.taskId());
            assertNotNull(decision.selectedOutput().orElseThrow());
            assertEquals(null, task.actual.get(root + "/" + KafkaBookKeeperTaskTerminalV2.key(task.registered.taskIdSha256()))
                    .get(30, TimeUnit.SECONDS), "selected Task fabricated an unpublished terminal");
            task.verifyWriterTickets(false);
        }
    }

    private static final class NativeFallbackRelease {
        static Object readFault(Task task, String method, String key) {
            if (readUnavailable(task, method, key))
                return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("native M4 release read unknown"));
            return null;
        }

        static boolean readUnavailable(Task task, String method, String key) {
            if (method.equals("get") && key.endsWith("/owner-admission-v1")) task.ownerKey = key;
            boolean unavailable = key.contains("/protections/") && task.blockReleaseReads.get()
                    || key.endsWith("/owner-admission-v1") && task.blockOwnerReads.get()
                    || key.contains("/native-read-scope/") && task.blockScopeReads.get();
            return method.equals("get") && unavailable;
        }

        static Object put(Task task, AsyncOxiaClient actual, java.lang.reflect.Method method, Object[] args,
                String key, boolean selector) throws Throwable {
            if (key.contains("/native-read-scope/") && task.mode.equals("TASK_OUTPUT_PUBLISH_RELEASE_SCOPE_UNKNOWN")
                    && task.faultOnce.compareAndSet(true, false)) {
                task.scopeKey = key;
                return ((CompletionStage<?>) Task.invoke(actual, method, args)).thenCompose(value -> {
                    task.blockScopeReads.set(true);
                    return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("applied native read scope response lost"));
                }).toCompletableFuture();
            }
            if (key.contains("/protections/") && task.mode.equals("TASK_OUTPUT_PUBLISH_RELEASE_OWNER_UNKNOWN")
                    && M4ReadControlCodecV1.decodeProtection(CanonicalBytes.copyOf((byte[]) args[1])).state() == ProtectionState.RELEASED
                    && task.faultOnce.compareAndSet(true, false))
                return ((CompletionStage<?>) Task.invoke(actual, method, args)).thenApply(value -> {
                    task.blockOwnerReads.set(true);
                    return value;
                }).toCompletableFuture();
            if (key.contains("/protections/") && task.mode.equals("TASK_OUTPUT_PUBLISH_RELEASE_UNKNOWN")
                    && M4ReadControlCodecV1.decodeProtection(CanonicalBytes.copyOf((byte[]) args[1])).state() == ProtectionState.RELEASED
                    && task.faultOnce.compareAndSet(true, false))
                return ((CompletionStage<?>) Task.invoke(actual, method, args)).thenCompose(value -> {
                    task.blockReleaseReads.set(true);
                    return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("applied M4 protection release response lost"));
                }).toCompletableFuture();
            if (selector && task.mode.startsWith("TASK_OUTPUT_PUBLISH_RELEASE")) {
                var next = M5BindingAuthorityCodecV1.decodeAuthority(CanonicalBytes.copyOf((byte[]) args[1])).selectorProjection();
                if (next.mode() == com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode.PREFERRED_ONLY
                        && !next.pendingAnchors().isEmpty())
                    return ((CompletionStage<?>) Task.invoke(actual, method, args)).thenApply(value -> {
                        task.fallbackClosure.complete(next);
                        return value;
                    }).toCompletableFuture();
            }
            return task.putTask(actual, method, args, key, selector);
        }

        static void holdRawRead(Task task, NereusKafkaOwnedProviderRuntime.Opened opened) throws Exception {
            if (!task.mode.equals("TASK_OUTPUT_PUBLISH_RELEASE_DRAIN_HELD")
                    && !task.mode.equals("TASK_OUTPUT_PUBLISH_RELEASE_OWNER_CLOSED")
                    && !task.mode.equals("TASK_OUTPUT_PUBLISH_RELEASE_OWNER_REFRESHED")) return;
            task.holdFallbackRead.set(true);
            task.fallbackRead = opened.storage().read(PreferredRead.request(0, Long.MAX_VALUE, KafkaReadIsolationV1.REPLICA))
                    .toCompletableFuture();
            CompletableFuture.anyOf(task.fallbackRead, task.fallbackReadEntered).get(30, TimeUnit.SECONDS);
            assertTrue(task.fallbackReadEntered.isDone(), "actual raw fallback SDK read was not withheld");
            assertFalse(task.fallbackRead.isDone(), "withheld raw SDK read completed");
        }

        static void verify(Task task, NereusKafkaPreparedTaskV2 prepared,
                KafkaSealedBookKeeperDescriptorV2 descriptor, List<CanonicalBytes> expected,
                KafkaCompactionAdmissionV1.State formal, KafkaCompactionAdmissionV1 admission,
                NereusKafkaOwnedProviderRuntime.Opened opened, Properties client) throws Exception {
            if (!task.mode.startsWith("TASK_OUTPUT_PUBLISH_RELEASE")) return;
            var predecessor = SelectedRollover.authority(task).selectorProjection();
            var before = opened.storage().nativeInputSources();
            if (KafkaBookKeeperSelectedInputRealScenario.releaseRollover(task.mode))
                expected = KafkaBookKeeperRootVerifierRealScenario.selectedBytes(prepared, descriptor, opened);
            var retirement = opened.retireNativeM5Fallback().toCompletableFuture();
            if (task.mode.equals("TASK_OUTPUT_PUBLISH_RELEASE_DRAIN_HELD")
                    || task.mode.equals("TASK_OUTPUT_PUBLISH_RELEASE_OWNER_CLOSED")
                    || task.mode.equals("TASK_OUTPUT_PUBLISH_RELEASE_OWNER_REFRESHED")) {
                CompletableFuture.anyOf(retirement, task.fallbackClosure).get(30, TimeUnit.SECONDS);
                var closed = task.fallbackClosure.get(30, TimeUnit.SECONDS);
                assertFalse(retirement.isDone(), "fallback was released while its actual raw SDK was still held");
                assertEquals(formal, admission.read().orElseThrow(), "held SDK drain released selected cache capacity");
                KafkaBookKeeperRootVerifierRealScenario.verifyProtected(task.actual, task.nativeTaskKey, closed);
                assertEquals(before.sources(), opened.storage().nativeReadSources(), "held SDK drain removed its raw read sources");
                var keys = new M4ReadControlKeysV1(0, predecessor.binding());
                assertEquals(null, task.actual.get(key(task, keys.terminal(predecessor.readAdmissionEpoch())))
                        .get(30, TimeUnit.SECONDS), "held SDK drain produced an M4 terminal");
                assertEquals(null, task.actual.get(key(task, keys.proof(predecessor.readAdmissionEpoch())))
                        .get(30, TimeUnit.SECONDS), "held SDK drain produced an M4 proof");
                if (task.mode.contains("_OWNER_"))
                    KafkaBookKeeperRootVerifierRealScenario.changeNativeOwner(task.actual, task.ownerKey,
                            task.mode.equals("TASK_OUTPUT_PUBLISH_RELEASE_OWNER_CLOSED"));
                else assertTrue(retirement.cancel(true));
                assertFalse(task.fallbackRead.isDone(), "Owner fence or observer cancellation drained the actual raw read");
                reject(opened.storage().read(PreferredRead.request(0, Long.MAX_VALUE, KafkaReadIsolationV1.REPLICA)));
                KafkaBookKeeperRootVerifierRealScenario.verifyProduceFenced(opened, client);
                task.releaseFallbackRead.complete(null);
                assertEquals(KafkaBookKeeperReadOutcomeV1.FOUND, task.fallbackRead.get(30, TimeUnit.SECONDS).outcome());
                if (task.mode.contains("_OWNER_")) {
                    KafkaBookKeeperReadScopeRealScenario.verifyOwnerRejected(retirement, opened, before, formal, admission);
                    return;
                }
                retirement = opened.retireNativeM5Fallback().toCompletableFuture();
            }
            if (task.mode.equals("TASK_OUTPUT_PUBLISH_RELEASE_UNKNOWN")
                    || task.mode.equals("TASK_OUTPUT_PUBLISH_RELEASE_OWNER_UNKNOWN")) {
                reject(retirement);
                assertFalse(task.faultOnce.get(), "actual protection release or Owner read fault was not exercised");
                assertTrue(task.blockReleaseReads.get() || task.blockOwnerReads.get());
                assertEquals(before, opened.storage().nativeInputSources(), "release or Owner UNKNOWN changed its hot sources or checkpoint");
                assertEquals(formal, admission.read().orElseThrow(), "release UNKNOWN freed selected cache capacity");
                reject(opened.storage().read(PreferredRead.request(0, Long.MAX_VALUE, KafkaReadIsolationV1.REPLICA)));
                var cut = SelectedRollover.authority(task).selectorProjection();
                var keys = new M4ReadControlKeysV1(0, predecessor.binding());
                var terminal = stored(task, keys.terminal(predecessor.readAdmissionEpoch()));
                var proof = stored(task, keys.proof(predecessor.readAdmissionEpoch()));
                task.blockReleaseReads.set(false);
                task.blockOwnerReads.set(false);
                retirement = opened.retireNativeM5Fallback().toCompletableFuture();
                retirement.get(30, TimeUnit.SECONDS);
                assertEquals(cut, SelectedRollover.authority(task).selectorProjection(), "release retry advanced another read epoch");
                assertEquals(terminal, stored(task, keys.terminal(predecessor.readAdmissionEpoch())), "retry replaced the actual drain terminal");
                assertEquals(proof, stored(task, keys.proof(predecessor.readAdmissionEpoch())), "retry replaced the actual drain proof");
            }
            if (task.mode.equals("TASK_OUTPUT_PUBLISH_RELEASE_SCOPE_UNKNOWN")) {
                reject(retirement);
                assertFalse(task.faultOnce.get(), "actual scope create response loss was not exercised");
                assertTrue(task.blockScopeReads.get());
                KafkaBookKeeperReadScopeRealScenario.verifyFetch(descriptor, expected, opened, client);
                retirement = KafkaBookKeeperReadScopeRealScenario.reconcileUnknown(task.actual, task.scopeKey, task.blockScopeReads,
                        descriptor, before, formal, admission, opened, client);
            }
            var result = retirement.get(30, TimeUnit.SECONDS);
            task.rejectCoveredRaw = true;
            KafkaBookKeeperRootVerifierRealScenario.verifyReleased(task.actual, task.nativeTaskKey, predecessor, result, SelectedRollover.authority(task).selectorProjection());
            KafkaBookKeeperRootVerifierRealScenario.verifyNativeSourceTransfer(opened.storage(), descriptor, before);
            KafkaBookKeeperReadScopeRealScenario.verify(task.actual, task.nativeTaskKey, task.ownerKey, descriptor, before, opened, result, task.recompactionFault);
            assertEquals(formal, admission.read().orElseThrow(), "M4 release freed the still selected cache charge");
            KafkaBookKeeperReadScopeRealScenario.verifyFetch(descriptor, expected, opened, client);
            SelectedRollover.verify(task, prepared, descriptor, formal, admission, opened, client);
            KafkaBookKeeperSelectedInputRealScenario.verifyIfSelected(task.mode, predecessor.binding(), descriptor, formal, admission, opened,
                    task.actual, task.nativeTaskKey.substring(0, task.nativeTaskKey.indexOf("/v2/")));
        }

        private static CanonicalBytes stored(Task task, String key) throws Exception {
            return CanonicalBytes.copyOf(task.actual.get(key(task, key)).get(30, TimeUnit.SECONDS).value());
        }

        private static String key(Task task, String key) {
            return task.nativeTaskKey.substring(0, task.nativeTaskKey.indexOf("/v2/")) + "/" + key;
        }
    }

    private static final class SelectedRollover {
        static void verify(Task task, NereusKafkaPreparedTaskV2 prepared, KafkaSealedBookKeeperDescriptorV2 descriptor,
                KafkaCompactionAdmissionV1.State formal, KafkaCompactionAdmissionV1 admission,
                NereusKafkaOwnedProviderRuntime.Opened opened, Properties client) throws Exception {
            if (!task.mode.startsWith("TASK_OUTPUT_PUBLISH_ROLLOVER")
                    && !KafkaBookKeeperSelectedInputRealScenario.releaseRollover(task.mode)) return;
            var predecessor = authority(task).selectorProjection();
            var decision = authority(task).taskSelectionDecision();
            int next = Math.toIntExact(opened.storage().nativeInputSources().protocol().root().frontiers().durableEndOffset());
            for (int rollover = 0; rollover < 2; rollover++) {
                var before = opened.storage().nativeReadSources();
                int limit = next + 33;
                while (opened.storage().nativeReadSources().activeRoot().runId().equals(before.activeRoot().runId()) && next < limit)
                    task.produce(client, next++);
                var after = opened.storage().nativeReadSources();
                assertFalse(after.activeRoot().runId().equals(before.activeRoot().runId()), "selected owner did not roll its actual raw tail");
                assertEquals(before.sealedRoots().size() + 1, after.sealedRoots().size());
                var selected = authority(task).selectorProjection();
                assertEquals(predecessor.sourceGeneration() + rollover + 1, selected.sourceGeneration());
                verifySelector(predecessor, selected, descriptor);
                assertEquals(decision, authority(task).taskSelectionDecision(), "rollover changed the immutable selected Task decision");
                assertEquals(formal, admission.read().orElseThrow(), "rollover released its selected cache charge");
            }
            KafkaBookKeeperReadScopeRealScenario.verifyFetch(descriptor, KafkaBookKeeperRootVerifierRealScenario.selectedBytes(prepared, descriptor, opened), opened, client);
            if (task.mode.equals("TASK_OUTPUT_PUBLISH_ROLLOVER_UNKNOWN")) verifyUnknown(task, descriptor, formal, admission, opened, client, next);
        }

        private static void verifyUnknown(Task task, KafkaSealedBookKeeperDescriptorV2 descriptor,
                KafkaCompactionAdmissionV1.State formal, KafkaCompactionAdmissionV1 admission,
                NereusKafkaOwnedProviderRuntime.Opened opened, Properties client, int next) throws Exception {
            task.rolloverPredecessor = authority(task).selectorProjection();
            task.rolloverArmed.set(true);
            var failedClient = new Properties();
            failedClient.putAll(client);
            failedClient.put("retries", "0");
            failedClient.put("delivery.timeout.ms", "5000");
            failedClient.put("request.timeout.ms", "1000");
            var produce = CompletableFuture.runAsync(() -> produceUntilUnknown(task, failedClient, next));
            task.rolloverLost.get(30, TimeUnit.SECONDS);
            var closeUnknown = opened.closeAsync();
            assertFalse(closeUnknown.isDone(), "Owner close overtook its actual pending source publication");
            assertEquals(formal, admission.read().orElseThrow(), "pending source publication lost its selected cache charge");
            task.rolloverReply.complete(null);
            produce.get(30, TimeUnit.SECONDS);
            assertThrows(java.util.concurrent.ExecutionException.class, () -> closeUnknown.get(30, TimeUnit.SECONDS),
                    "Owner close concealed its unresolved source publication");
            var selected = authority(task).selectorProjection();
            assertEquals(task.rolloverPredecessor.sourceGeneration() + 1, selected.sourceGeneration());
            verifySelector(task.rolloverPredecessor, selected, descriptor);
            assertEquals(formal, admission.read().orElseThrow(), "unknown rollover released its actual Task cache");
            var sources = task.rolloverSources;
            assertNotNull(sources, "actual source qualification was not observed");
            var roots = new ArrayList<>(sources.sealedRoots());
            roots.add(sources.activeRoot());
            for (var root : roots) {
                var resource = new PhysicalResourceIdV2.BookKeeperLedger(task.registered.namespace(),
                        root.ledgerIdentity().ledgerId());
                var tickets = M5TargetDeleteAuthorityCodecV1.decodeAuthority(task.outputRoute.read(resource.authorityKey())
                        .toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow().canonicalStoredBytes()).activeWriterTickets();
                var ticket = tickets.stream().filter(value -> value.externalFactsRootSha256().equals(sources.selectedViewSha256()))
                        .findFirst().orElseThrow(() -> new AssertionError("unknown source CAS lost its original physical member ticket"));
                if (task.rolloverOperation == null) task.rolloverOperation = ticket.operationIdSha256();
                assertEquals(task.rolloverOperation, ticket.operationIdSha256());
            }
            assertEquals(formal, admission.read().orElseThrow(), "unresolved source cleanup released selected Task capacity");
            task.blockSelectorReads.set(false);
        }

        private static void produceUntilUnknown(Task task, Properties client, int next) {
            for (int offset = next; offset < next + 33; offset++) {
                try {
                    task.produce(client, offset);
                } catch (Exception expected) {
                    try {
                        assertTrue(task.rolloverLost.isDone() || expected instanceof java.util.concurrent.ExecutionException
                                && expected.getCause() instanceof org.apache.kafka.common.errors.TimeoutException,
                                "unexpected Produce failure before source CAS response loss");
                        task.rolloverLost.get(30, TimeUnit.SECONDS);
                    } catch (Exception | AssertionError failure) {
                        expected.addSuppressed(failure);
                        throw new AssertionError("Produce failed without the actual source CAS response loss", expected);
                    }
                    return;
                }
            }
            throw new AssertionError("unknown selected rollover acknowledged Produce");
        }

        private static com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingRetirementAuthorityV1 authority(Task task) throws Exception {
            return M5BindingAuthorityCodecV1.decodeAuthority(CanonicalBytes.copyOf(task.actual.get(task.selectorKey).get(30, TimeUnit.SECONDS).value()));
        }

        private static void verifySelector(BindingReadSelector predecessor, BindingReadSelector selected, KafkaSealedBookKeeperDescriptorV2 descriptor) {
            assertEquals(descriptor.descriptorSha256(), selected.selectedViewSha256());
            assertEquals(predecessor.ownerEpoch(), selected.ownerEpoch());
            assertEquals(predecessor.readAdmissionEpoch(), selected.readAdmissionEpoch());
            assertEquals(predecessor.mode(), selected.mode());
            assertEquals(predecessor.fallbackSetSha256(), selected.fallbackSetSha256());
        }

        static void verifyClosedTickets(Task task) throws Exception {
            if (task.rolloverOperation == null) return;
            var roots = new ArrayList<>(task.rolloverSources.sealedRoots());
            roots.add(task.rolloverSources.activeRoot());
            for (var root : roots) {
                var resource = new PhysicalResourceIdV2.BookKeeperLedger(task.registered.namespace(),
                        root.ledgerIdentity().ledgerId());
                var tickets = M5TargetDeleteAuthorityCodecV1.decodeAuthority(task.outputRoute.read(resource.authorityKey())
                        .toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow().canonicalStoredBytes()).activeWriterTickets();
                assertFalse(tickets.stream().anyMatch(ticket -> ticket.operationIdSha256().equals(task.rolloverOperation)),
                        "confirmed Owner drain leaked the original source publication nonce");
            }
        }
    }

    private static final class Task {
        private final KafkaBookKeeperM5GcInterruptionRealScenario gcInterruption = new KafkaBookKeeperM5GcInterruptionRealScenario();
        private final KafkaBookKeeperSelectedInputRealScenario.RecompactionFault recompactionFault;
        private final CompletableFuture<NereusKafkaOwnedProviderRuntime.Opened> coldOwner;
        private final KafkaBookKeeperReadScopeRealScenario.ColdScopeFault coldScope;
        private volatile long selectedCutEnd;
        private volatile boolean rejectCoveredRaw;
        private final String mode;
        private AsyncOxiaClient actual;
        private String nativeTaskKey;
        private String selectorKey;
        private KafkaBookKeeperInventoryV2.Task registered;
        private BindingReadSelector cancelPredecessor;
        private String nativeCapacityKey;
        private KafkaCompactionAdmissionV1.State formalCandidate;
        private BoundPhysicalDeleteAuthorityRouteV2 outputRoute;
        private List<VerifiedPart> outputParts;
        private final AtomicInteger outputSessions = new AtomicInteger();
        private final CompletableFuture<Void> outputCloseSeen = new CompletableFuture<>();
        private final CompletableFuture<Void> releaseOutputClose = new CompletableFuture<>();
        private final AtomicBoolean blockOutputTicketReads = new AtomicBoolean();
        private String outputTicketKey;
        private java.util.function.Supplier<CompletionStage<?>> delayed;
        private final AtomicBoolean faultOnce = new AtomicBoolean(true);
        private final AtomicBoolean cancelArmed = new AtomicBoolean();
        private final AtomicBoolean blockSelectorReads = new AtomicBoolean();
        private final AtomicBoolean blockCapacityReads = new AtomicBoolean();
        private final AtomicBoolean blockCacheBudgetReads = new AtomicBoolean();
        private final AtomicBoolean blockReleaseReads = new AtomicBoolean();
        private final AtomicBoolean blockOwnerReads = new AtomicBoolean();
        private final AtomicBoolean blockScopeReads = new AtomicBoolean();
        private String scopeKey;
        private volatile String ownerKey;
        private final AtomicBoolean holdFallbackRead = new AtomicBoolean();
        private final CompletableFuture<Void> fallbackReadEntered = new CompletableFuture<>();
        private final CompletableFuture<Void> releaseFallbackRead = new CompletableFuture<>();
        private final CompletableFuture<BindingReadSelector> fallbackClosure = new CompletableFuture<>();
        private CompletableFuture<com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadResultV1> fallbackRead;
        private final AtomicBoolean rolloverArmed = new AtomicBoolean();
        private final CompletableFuture<Void> rolloverLost = new CompletableFuture<>();
        private final CompletableFuture<Void> rolloverReply = new CompletableFuture<>();
        private BindingReadSelector rolloverPredecessor;
        private com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1.NativeReadSources rolloverSources;
        private Sha256Digest rolloverOperation;

        private Task(String mode, CompletableFuture<NereusKafkaOwnedProviderRuntime.Opened> coldOwner) {
            this.mode = mode;
            this.coldOwner = coldOwner;
            coldScope = new KafkaBookKeeperReadScopeRealScenario.ColdScopeFault(mode);
            recompactionFault = new KafkaBookKeeperSelectedInputRealScenario.RecompactionFault(mode, parts -> outputParts = parts, this::verifyWriterTicketsRetained);
        }

        private static void exercise(KafkaClusterTestKit cluster, State state) throws Exception {
            var client = new Properties();
            client.put("bootstrap.servers", cluster.bootstrapServers());
            try (var admin = Admin.create(client)) {
                admin.createTopics(List.of(topic(state.mode, "native-cold"))).all().get(30, TimeUnit.SECONDS);
                var opened = state.zero.get(30, TimeUnit.SECONDS);
                int offset = KafkaBookKeeperReadScopeRealScenario.prepareTransactionHistory(state.mode, admin, opened, client,
                        next -> state.task.produce(client, next));
                while (opened.storage().nativeReadSources().sealedRoots().stream()
                        .filter(root -> root.kafkaEndOffsetExclusive().orElseThrow() > root.kafkaStartOffset()).count() < 2 && offset < 32)
                    state.task.produce(client, offset++);
                offset = KafkaBookKeeperReadScopeRealScenario.prepareEmptyTail(state.mode, opened, client, offset,
                        next -> state.task.produce(client, next));
                state.task.verify(opened, client, offset);
            } finally {
                state.task.releaseOutputClose.complete(null);
                state.task.rolloverReply.complete(null);
                state.task.blockSelectorReads.set(false);
                state.task.blockOutputTicketReads.set(false);
                state.task.releaseFallbackRead.complete(null);
                state.task.blockReleaseReads.set(false);
                state.task.blockOwnerReads.set(false);
                state.task.blockScopeReads.set(false);
                state.task.recompactionFault.release();
            }
        }

        private void produce(Properties client, int offset) throws Exception {
            var config = new Properties();
            config.putAll(client);
            config.put("key.serializer", StringSerializer.class.getName());
            config.put("value.serializer", StringSerializer.class.getName());
            config.put("acks", "all");
            long timestamp = mode.equals("TASK_MAX_LAG") || (mode.equals("TASK_MIN_LAG") && offset == 0)
                    ? 1000
                    : mode.equals("TASK_MAX_LAG_ZERO") ? 0 : System.currentTimeMillis();
            boolean emptyKey = (mode.equals("TASK_EMPTY_KEY") || mode.equals("TASK_OUTPUT_EMPTY_KEY")) && offset < 2;
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
                assertEquals((long) offset, producer.send(new ProducerRecord<>("native-cold", 0, timestamp,
                        emptyKey ? "" : "key", emptyKey && offset == 1 ? null : "task-" + offset))
                        .get(30, TimeUnit.SECONDS).offset());
            }
        }

        private void verify(NereusKafkaOwnedProviderRuntime.Opened opened, Properties client, int nextOffset) throws Exception {
            for (int attempt = 0; attempt < 3; attempt++) {
                if (attempt > 0) nextOffset = Math.toIntExact(opened.storage().nativeInputSources().protocol().root().frontiers().durableEndOffset());
                if (verifyAttempt(opened, client, nextOffset)) return;
            }
            throw new IllegalStateException("native metadata image changed through three confirmed preparation attempts");
        }

        private boolean verifyAttempt(NereusKafkaOwnedProviderRuntime.Opened opened, Properties client, int nextOffset) throws Exception {
            var limits = new Caps(BOUNDS.payloadBytes(), BOUNDS.batches(), BOUNDS.records(), BOUNDS.records(),
                    BOUNDS.decodedBytes(), BOUNDS.decodedBytes(), BOUNDS.decodedBytes(), BOUNDS.records(), BOUNDS.records());
            var caps = KafkaBookKeeperSelectedInputRealScenario.caps(limits, KafkaBookKeeperSelectedInputRealScenario.captureTasks(mode));
            var allowance = new NereusKafkaNativeInputV2.CaptureAllowance(limits, 0, 0, 4096, 0, 4096, 64);
            var captured = withCurrentMetadataImage(() -> opened.captureM5Inputs(budget(opened), BOUNDS, caps, allowance), true);
            var admission = opened.nativeM5CompactionAdmission();
            var preparation = admission.read().orElseThrow();
            verifyPolicy(captured);
            if (mode.equals("TASK_MIN_LAG") || mode.equals("TASK_RATIO") || mode.equals("TASK_MAX_LAG_ZERO")) {
                return KafkaBookKeeperSelectedInputRealScenario.verifyIneligible(mode, registered != null, actual, selectorKey,
                        captured, admission, preparation);
            }
            if (mode.equals("TASK_REGISTER_UNKNOWN") || mode.startsWith("TASK_CONVERT")) {
                if (!verifyUnknown(opened, captured, preparation)) return false;
            } else {
                var prepared = prepareConfirmed(captured, admission);
                if (prepared == null) return false;
                if (mode.equals("TASK_EMPTY_KEY") || mode.equals("TASK_OUTPUT_EMPTY_KEY")) verifyEmptyKey(prepared);
                if (mode.equals("TASK_RATIO_NEAR_ONE")) assertEquals(1_000_000, prepared.plan().policy().minimumCleanableRatioPartsPerMillion());
                assertEquals(registered, prepared.task());
                assertEquals(prepared.semantic().compactionPlanRootSha256(), registered.compactionPlanRootSha256());
                assertEquals(com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.encodeSourceCut(prepared.plan().sourceCut()),
                        registered.sourceCut());
                assertEquals(prepared.semantic().outputIdentitySha256(), registered.semanticOutputSha256());
                assertEquals(prepared.layout().parts().stream().map(part -> part.plan()).toList(), registered.parts());
                var formal = admission.read().orElseThrow();
                assertEquals(preparation.generation() + 1, formal.generation());
                assertEquals(List.of(registered.taskIdSha256()), formal.reservations().stream().map(row -> row.compactionTaskIdSha256()).toList());
                assertEquals(KafkaCompactionAdmissionV1.totals(preparation.reservations()), KafkaCompactionAdmissionV1.totals(formal.reservations()),
                        "conversion reduced the retained input/output resource envelope");
                reject(captured.prepareTask(4096, 65536));
                final boolean ownerDrained;
                try {
                    ownerDrained = materializeIfSelected(prepared, formal, admission, opened, captured, client, nextOffset);
                } catch (java.util.concurrent.ExecutionException failure) {
                    return retryConfirmedNoOutput(captured, admission, failure);
                }
                finishTask(prepared, captured, formal, admission, opened, client, ownerDrained);
            }
            verifyTerminal();
            KafkaBookKeeperM5UnpublishedRealScenario.verifyClosedCaptureAndGc(captured, mode, opened,
                    registered, client, caps, allowance);
            return true;
        }

        private boolean retryConfirmedNoOutput(NereusKafkaNativeInputV2.Captured captured, KafkaCompactionAdmissionV1 admission,
                java.util.concurrent.ExecutionException failure) throws Exception {
            var cause = failure.getCause();
            while (cause instanceof java.util.concurrent.CompletionException && cause.getCause() != null) cause = cause.getCause();
            if (outputSessions.get() != 0 || !(cause instanceof IllegalStateException)
                    || !"native M5 metadata image changed during capture".equals(cause.getMessage())) throw failure;
            captured.discard().toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertTrue(admission.read().orElseThrow().reservations().isEmpty(), "known no-output cancellation retained Task capacity");
            verifyCancelled();
            registered = null;
            nativeTaskKey = null;
            return false;
        }

        private NereusKafkaPreparedTaskV2 prepareConfirmed(NereusKafkaNativeInputV2.Captured captured,
                KafkaCompactionAdmissionV1 admission) throws Exception {
            try {
                return captured.prepareTask(4096, 65536).toCompletableFuture().get(30, TimeUnit.SECONDS);
            } catch (java.util.concurrent.ExecutionException failure) {
                if (!KafkaBookKeeperRootVerifierRealScenario.releaseKnownNoTask(registered != null, captured, admission, failure)) throw failure;
                return null;
            }
        }

        private boolean materializeIfSelected(NereusKafkaPreparedTaskV2 prepared, KafkaCompactionAdmissionV1.State formal,
                KafkaCompactionAdmissionV1 admission, NereusKafkaOwnedProviderRuntime.Opened opened,
                NereusKafkaNativeInputV2.Captured captured, Properties client, int nextOffset) throws Exception {
            boolean ownerDrained = false;
            if (mode.equals("TASK_OUTPUT_WORKER_STALE")) rolloverIfSelected(opened, client, nextOffset);
            if (mode.startsWith("TASK_OUTPUT")) ownerDrained = verifyOutput(prepared, formal, admission, opened, captured);
            if (!ownerDrained && !mode.equals("TASK_OUTPUT_WORKER_STALE")) rolloverIfSelected(opened, client, nextOffset);
            return ownerDrained;
        }

        private void finishFormalTask(NereusKafkaNativeInputV2.Captured captured, KafkaCompactionAdmissionV1.State formal,
                KafkaCompactionAdmissionV1 admission, NereusKafkaOwnedProviderRuntime.Opened opened, boolean ownerDrained) throws Exception {
            cancelPredecessor = M5BindingAuthorityCodecV1.decodeAuthority(
                    CanonicalBytes.copyOf(actual.get(selectorKey).get(30, TimeUnit.SECONDS).value())).selectorProjection();
            if (ownerDrained) {
                assertTrue(admission.read().orElseThrow().reservations().isEmpty());
            } else if (mode.equals("TASK_OUTPUT_TICKET_UNKNOWN")) {
                reject(captured.discard());
                assertEquals(formal, admission.read().orElseThrow(), "unresolved exact writer cleanup released Task capacity");
                assertTrue(blockOutputTicketReads.get(), "native ticket release fault was not exercised");
                blockOutputTicketReads.set(false);
                captured.discard().toCompletableFuture().get(30, TimeUnit.SECONDS);
            } else if (mode.equals("TASK_CANCEL_UNKNOWN")) {
                cancelArmed.set(true);
                reject(captured.discard());
                assertEquals(formal, admission.read().orElseThrow(), "unknown cancellation released formal task capacity");
                reject(opened.closeAsync());
                assertEquals(formal, admission.read().orElseThrow(), "unknown Owner cancellation released formal task capacity");
                blockSelectorReads.set(false);
                opened.closeAsync().get(30, TimeUnit.SECONDS);
            } else captured.discard().toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertTrue(admission.read().orElseThrow().reservations().isEmpty());
        }

        private void finishTask(NereusKafkaPreparedTaskV2 prepared, NereusKafkaNativeInputV2.Captured captured,
                KafkaCompactionAdmissionV1.State formal, KafkaCompactionAdmissionV1 admission,
                NereusKafkaOwnedProviderRuntime.Opened opened, Properties client, boolean ownerDrained) throws Exception {
            if (mode.startsWith("TASK_OUTPUT_PUBLISH")) Published.verify(this, prepared, captured, formal, admission, opened, client);
            else {
                if (mode.equals("TASK_OUTPUT")) reject(opened.captureNativeM5UnpublishedEvidence(registered.taskIdSha256()));
                finishFormalTask(captured, formal, admission, opened, ownerDrained);
                if (!ownerDrained && outputParts != null && !outputParts.isEmpty())
                    KafkaBookKeeperReadScopeRealScenario.verifyUnpublished(actual, mode, recompactionFault.replacementFacts, opened, registered, outputParts);
            }
        }

        private void verifyTerminal() throws Exception {
            if (mode.startsWith("TASK_OUTPUT_PUBLISH")) Published.verifyDecision(this);
            else verifyCancelled();
        }

        private boolean verifyOutput(NereusKafkaPreparedTaskV2 prepared, KafkaCompactionAdmissionV1.State formal,
                KafkaCompactionAdmissionV1 admission, NereusKafkaOwnedProviderRuntime.Opened opened,
                NereusKafkaNativeInputV2.Captured captured) throws Exception {
            var sourceBefore = opened.storage().nativeReadSources();
            if (mode.equals("TASK_OUTPUT_APPEND_UNKNOWN"))
                assertEquals(1, prepared.layout().parts().get(0).entries().size(), "lost reply must concern the complete first Part");
            var output = prepared.materialize();
            if (mode.equals("TASK_OUTPUT_WORKER_UNKNOWN") || mode.equals("TASK_OUTPUT_WORKER_STALE")) {
                var failure = assertThrows(java.util.concurrent.ExecutionException.class,
                        () -> output.toCompletableFuture().get(30, TimeUnit.SECONDS));
                if (mode.equals("TASK_OUTPUT_WORKER_UNKNOWN") && faultOnce.get()) throw failure;
                assertEquals(0, outputSessions.get(), "invalid or unknown worker dispatched a native output session");
                assertEquals(formal, admission.read().orElseThrow(), "worker failure released Task capacity");
                blockSelectorReads.set(false);
                return false;
            }
            if (mode.equals("TASK_OUTPUT_CLOSE_UNKNOWN")) {
                var failure = assertThrows(java.util.concurrent.ExecutionException.class,
                        () -> output.toCompletableFuture().get(30, TimeUnit.SECONDS));
                if (!outputCloseSeen.isDone()) throw failure;
                var cause = failure.getCause();
                while (cause instanceof java.util.concurrent.CompletionException && cause.getCause() != null) cause = cause.getCause();
                assertInstanceOf(java.util.concurrent.TimeoutException.class, cause, "actual SDK close fault masked an earlier output failure");
                assertEquals("completed native output SDK close response lost", cause.getMessage());
                outputParts = readOutputParts(prepared, opened);
            } else if (mode.equals("TASK_OUTPUT_CLOSE_HELD")) {
                CompletableFuture.anyOf(output.toCompletableFuture(), outputCloseSeen).get(30, TimeUnit.SECONDS);
                assertTrue(outputCloseSeen.isDone(), "materialization finished before the actual SDK close was withheld");
                assertFalse(output.toCompletableFuture().isDone(), "withheld actual SDK close completed materialization");
                outputParts = readOutputParts(prepared, opened);
            } else outputParts = output.toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertEquals(sourceBefore, opened.storage().nativeReadSources(), "output client changed the ordinary native source");
            assertEquals(formal, admission.read().orElseThrow(), "output completion released Task capacity before native termination");
            assertEquals(1, outputSessions.get());
            assertEquals(registered.parts().size(), outputParts.size());
            var authority = M5BindingAuthorityCodecV1.decodeAuthority(
                    CanonicalBytes.copyOf(actual.get(selectorKey).get(30, TimeUnit.SECONDS).value()));
            var worker = authority.materializationWorker().orElseThrow();
            assertEquals(prepared.plan().sourceCut().identity(), worker.identity());
            assertEquals(registered.taskIdSha256(), worker.taskIdSha256());
            assertEquals(registered.compactionPlanRootSha256(), worker.planRootSha256());
            verifyWriterTickets(true);
            verifyOutputBytes(prepared, opened, captured);
            if (mode.equals("TASK_OUTPUT_APPEND_UNKNOWN") || mode.startsWith("TASK_OUTPUT_CLOSE"))
                assertFalse(faultOnce.get(), "real native output reply fault was not exercised");
            if (!mode.equals("TASK_OUTPUT_CLOSE_HELD")) return false;
            assertTrue(output.toCompletableFuture().cancel(true));
            var release = captured.discard().toCompletableFuture();
            var ownerClose = opened.closeAsync();
            assertFalse(release.isDone(), "observer cancellation released the actual Task operation");
            assertFalse(ownerClose.isDone(), "Owner closed before its actual Task SDK operation drained");
            assertEquals(formal, admission.read().orElseThrow());
            verifyWriterTickets(true);
            releaseOutputClose.complete(null);
            release.get(30, TimeUnit.SECONDS);
            ownerClose.get(30, TimeUnit.SECONDS);
            return true;
        }

        private List<VerifiedPart> readOutputParts(NereusKafkaPreparedTaskV2 prepared,
                NereusKafkaOwnedProviderRuntime.Opened opened) throws Exception {
            String cellRoot = nativeTaskKey.substring(0, nativeTaskKey.indexOf("/v2/"));
            var source = nativeSource(opened);
            var parts = new ArrayList<VerifiedPart>();
            for (int ordinal = 0; ordinal < registered.parts().size(); ordinal++) {
                var stored = actual.get(cellRoot + "/" + KafkaBookKeeperInventoryV2.partKey(registered.taskIdSha256(), ordinal))
                        .get(30, TimeUnit.SECONDS);
                assertNotNull(stored, "actual output SDK close occurred without a complete Part inventory");
                var part = KafkaBookKeeperInventoryCodecV2.decodePart(CanonicalBytes.copyOf(stored.value()));
                assertEquals(prepared.layout().task().parts().get(ordinal), registered.parts().get(ordinal));
                parts.add(new VerifiedPart(part, source.captureExactTarget(part.handle()).toCompletableFuture()
                        .get(30, TimeUnit.SECONDS).exactTarget().orElseThrow()));
            }
            return List.copyOf(parts);
        }

        private com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2 nativeSource(
                NereusKafkaOwnedProviderRuntime.Opened opened) throws Exception {
            var sourceField = NereusKafkaOwnedProviderRuntime.Opened.class.getDeclaredField("source");
            sourceField.setAccessible(true);
            return (com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2) sourceField.get(opened);
        }

        private void verifyOutputBytes(NereusKafkaPreparedTaskV2 prepared, NereusKafkaOwnedProviderRuntime.Opened opened,
                NereusKafkaNativeInputV2.Captured captured) throws Exception {
            var reader = nativeSource(opened).newSession();
            try {
                for (var output : outputParts) {
                    assertEquals(Optional.of(output.part().handle()), reader.openRunLedger(output.part().handle())
                            .toCompletableFuture().get(30, TimeUnit.SECONDS).exactHandle());
                    var entries = prepared.layout().parts().get(output.part().ordinal()).entries();
                    for (int entry = 0; entry < entries.size(); entry++) {
                        var actualEntry = reader.readExactEntry(output.part().handle(), entry).toCompletableFuture()
                                .get(30, TimeUnit.SECONDS).exactEntry().orElseThrow();
                        assertEquals(entries.get(entry), actualEntry.payload(), "fresh native SDK read differs from the complete planned output bytes");
                    }
                }
            } finally {
                reader.closeAsync().toCompletableFuture().get(30, TimeUnit.SECONDS);
            }
            if (mode.equals("TASK_OUTPUT_READ")) PreferredRead.verify(prepared, captured, outputParts, nativeSource(opened), opened);
        }

        private void verifyWriterTickets(boolean retained) throws Exception {
            for (var output : outputParts) {
                var observed = outputRoute.read(output.part().resource().authorityKey()).toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow();
                var value = M5TargetDeleteStoredValueV2.decode(observed).fullAuthority().orElseThrow();
                assertEquals(retained ? 1 : 0, value.activeWriterTickets().size(), "Task writer ticket lifetime differs from native termination");
                assertTrue(value.eligibilitySnapshot().isEmpty(), "output enrollment fabricated physical delete eligibility");
            }
        }

        private void verifyWriterTicketsRetained() throws Exception {
            for (var output : outputParts) {
                var observed = outputRoute.read(output.part().resource().authorityKey()).toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow();
                var authority = M5TargetDeleteStoredValueV2.decode(observed).fullAuthority().orElseThrow();
                assertFalse(authority.activeWriterTickets().isEmpty(), "uncertain actual SDK/publication lost its output tickets");
                assertTrue(authority.eligibilitySnapshot().isEmpty());
            }
        }

        private void verifyPolicy(NereusKafkaNativeInputV2.Captured captured) {
            var config = captured.topicPolicy().config();
            var batches = captured.batches().stream().map(batch -> MemoryRecords.readableRecords(
                    ByteBuffer.wrap(batch.canonicalBody().toByteArray())).batches().iterator().next()).toList();
            if (mode.equals("TASK_MIN_LAG")) {
                assertEquals(3600000, config.compactionLagMs);
                assertEquals(3600000, config.maxCompactionLagMs);
                assertEquals(1000, batches.get(0).maxTimestamp());
                assertTrue(batches.stream().anyMatch(batch -> batch.maxTimestamp() > System.currentTimeMillis() - config.compactionLagMs));
                assertTrue(batches.get(0).maxTimestamp() < System.currentTimeMillis() - config.maxCompactionLagMs,
                        "oldest dirty first batch did not force max lag; minimum lag precedence was not exercised");
            }
            if (mode.equals("TASK_MAX_LAG")) {
                assertEquals(1.0, config.minCleanableRatio);
                assertEquals(1, config.maxCompactionLagMs);
                assertTrue(batches.stream().allMatch(batch -> batch.maxTimestamp() == 1000));
            }
            if (mode.equals("TASK_MAX_LAG_ZERO")) {
                assertEquals(1.0, config.minCleanableRatio);
                assertEquals(1, config.maxCompactionLagMs);
                assertTrue(batches.stream().allMatch(batch -> batch.maxTimestamp() == 0));
            }
            if (mode.equals("TASK_RATIO")) assertEquals(1.0, config.minCleanableRatio);
            if (mode.equals("TASK_RATIO_NEAR_ONE")) assertEquals(0.999999999, config.minCleanableRatio);
        }

        private static void verifyEmptyKey(NereusKafkaPreparedTaskV2 prepared) {
            var proof = prepared.plan().keyProofs().stream().filter(key -> key.key().isEmpty()).findFirst().orElseThrow();
            assertEquals(1, proof.latestEligibleOffset());
            assertTrue(proof.olderValueMayReappear());
            assertEquals(0, proof.tombstoneDeadlineMs());
            assertEquals(Disposition.DROP_SUPERSEDED_VALUE, prepared.semantic().dispositions().stream()
                    .filter(row -> row.offset() == 0).findFirst().orElseThrow().disposition());
            assertEquals(Disposition.KEEP_TOMBSTONE_WITHIN_RETENTION, prepared.semantic().dispositions().stream()
                    .filter(row -> row.offset() == 1).findFirst().orElseThrow().disposition());
            Plan.verifyOracle(prepared.plan(), prepared.semantic());
        }

        private boolean verifyUnknown(NereusKafkaOwnedProviderRuntime.Opened opened, NereusKafkaNativeInputV2.Captured captured,
                KafkaCompactionAdmissionV1.State preparation) throws Exception {
            var preparationFailure = assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> captured.prepareTask(4096, 65536).toCompletableFuture().get(30, TimeUnit.SECONDS));
            var admission = opened.nativeM5CompactionAdmission();
            if (KafkaBookKeeperRootVerifierRealScenario.releaseKnownNoTask(registered != null, captured, admission, preparationFailure)) return false;
            assertNotNull(registered);
            boolean applied = mode.equals("TASK_CONVERT_APPLIED_UNKNOWN");
            if (applied) {
                assertNotNull(formalCandidate);
                assertEquals(List.of(registered.taskIdSha256()), formalCandidate.reservations().stream().map(row -> row.compactionTaskIdSha256()).toList());
                assertEquals(formalCandidate, actualCapacity());
            } else {
                assertEquals(preparation, admission.read().orElseThrow(), "unknown preparation released or substituted its quota");
                assertNotNull(delayed);
            }
            reject(captured.discard());
            if (applied) {
                reject(opened.closeAsync());
                assertEquals(formalCandidate, actualCapacity(), "unreadable applied conversion released formal capacity");
                blockCapacityReads.set(false);
            }
            opened.closeAsync().get(30, TimeUnit.SECONDS);
            assertTrue(admission.read().orElseThrow().reservations().isEmpty());
            if (delayed != null) {
                var late = delayed.get().handle((value, failure) -> failure).toCompletableFuture().get(30, TimeUnit.SECONDS);
                if (mode.equals("TASK_REGISTER_UNKNOWN")) assertEquals(null, late, "late immutable registration did not execute");
                else assertNotNull(late, "old conditional conversion resurrected the task quota after Owner cancellation");
            }
            assertTrue(admission.read().orElseThrow().reservations().isEmpty());
            return true;
        }

        private KafkaCompactionAdmissionV1.State actualCapacity() throws Exception {
            return KafkaCompactionAdmissionV1.decode(CanonicalBytes.copyOf(actual.get(nativeCapacityKey).get(30, TimeUnit.SECONDS).value()));
        }

        private void rolloverIfSelected(NereusKafkaOwnedProviderRuntime.Opened opened, Properties client, int nextOffset) throws Exception {
            if (!mode.equals("TASK_ROLLOVER") && !mode.equals("TASK_OUTPUT_ROLLOVER")
                    && !mode.equals("TASK_OUTPUT_WORKER_STALE")) return;
            var old = opened.storage().nativeReadSources().activeRoot().runId();
            int offset = nextOffset;
            while (opened.storage().nativeReadSources().activeRoot().runId().equals(old) && offset < nextOffset + 33)
                KafkaBookKeeperRootVerifierRealScenario.produce(client, "rollover-" + offset, offset++);
            assertFalse(opened.storage().nativeReadSources().activeRoot().runId().equals(old));
        }

        private void verifyCancelled() throws Exception {
            assertEquals(KafkaBookKeeperInventoryCodecV2.encodeTask(registered),
                    CanonicalBytes.copyOf(actual.get(nativeTaskKey).get(30, TimeUnit.SECONDS).value()));
            String cellRoot = nativeTaskKey.substring(0, nativeTaskKey.indexOf("/v2/"));
            var cut = com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.decodeSourceCut(registered.sourceCut());
            String decisionKey = cellRoot + "/" + M5TaskSelectionDecisionV2.key(cut.identity().binding(), registered.taskIdSha256());
            var decision = M5TaskSelectionDecisionV2.decode(CanonicalBytes.copyOf(actual.get(decisionKey).get(30, TimeUnit.SECONDS).value()));
            assertEquals(M5TaskSelectionDecisionV2.Outcome.SELECTION_CANCELLED, decision.outcome());
            var authority = M5BindingAuthorityCodecV1.decodeAuthority(CanonicalBytes.copyOf(actual.get(selectorKey).get(30, TimeUnit.SECONDS).value()));
            assertEquals(cut.identity().binding(), authority.binding());
            if (cancelPredecessor != null) {
                assertEquals(cancelPredecessor, authority.selectorProjection(), "prepared cancellation changed the current native selector");
                assertEquals(Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(cancelPredecessor)), decision.predecessorSelector());
            }
            if (outputParts != null) {
                assertEquals(registered.taskIdSha256(), authority.materializationWorker().orElseThrow().taskIdSha256());
                var terminal = KafkaBookKeeperTaskTerminalV2.decode(CanonicalBytes.copyOf(actual.get(
                        cellRoot + "/" + KafkaBookKeeperTaskTerminalV2.key(registered.taskIdSha256())).get(30, TimeUnit.SECONDS).value()));
                assertEquals(registered, terminal.task());
                assertEquals(registered.taskIdSha256(), terminal.nativeCreateScope().taskId());
                assertEquals(outputParts.stream().map(part -> part.sealedMetadata()).toList(),
                        terminal.physicalCut().stream().map(part -> part.sealed().orElseThrow()).toList());
                verifyWriterTickets(false);
                return;
            }
            if (mode.equals("TASK_OUTPUT_WORKER_UNKNOWN") && !faultOnce.get())
                assertEquals(registered.taskIdSha256(), authority.materializationWorker().orElseThrow().taskIdSha256());
            else assertTrue(authority.materializationWorker().isEmpty(), "Task registration admitted an output worker");
            for (int ordinal = 0; ordinal < registered.parts().size(); ordinal++)
                assertEquals(null, actual.get(cellRoot + "/" + KafkaBookKeeperInventoryV2.partKey(registered.taskIdSha256(), ordinal))
                        .get(30, TimeUnit.SECONDS), "prepared task dispatched a physical output part");
        }

        private AsyncOxiaClient client(AsyncOxiaClient actual) {
            this.actual = actual;
            return recompactionFault.client(actual, (AsyncOxiaClient) java.lang.reflect.Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[] {AsyncOxiaClient.class},
                    (proxy, method, args) -> {
                        String key = args != null && args.length > 0 && args[0] instanceof String value ? value : "";
                        boolean selector = key.contains("/read-m4/") && key.endsWith("/selector");
                        if (selector) selectorKey = key;
                        if (method.getName().equals("get") && key.endsWith("/cache-budget") && blockCacheBudgetReads.get())
                            return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("native cache budget read unknown"));
                        var readFault = NativeFallbackRelease.readFault(this, method.getName(), key);
                        if (readFault != null) return readFault;
                        if (method.getName().equals("get") && (selector && blockSelectorReads.get()
                                || key.endsWith("/kafka-compaction-admission") && blockCapacityReads.get()))
                            return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("native prepared task authority read unknown"));
                        return KafkaBookKeeperM5PolicyRealScenario.interceptPut(method.getName(), selector, args)
                                ? put(actual, method, args, key, selector) : invoke(actual, method, args);
                    }));
        }

        private Object put(AsyncOxiaClient actual, java.lang.reflect.Method method, Object[] args, String key, boolean selector) throws Throwable {
            if (mode.startsWith("TASK_OUTPUT_PUBLISH_RELEASE"))
                return NativeFallbackRelease.put(this, actual, method, args, key, selector);
            if (key.endsWith("/cache-budget") && mode.equals("TASK_OUTPUT_PUBLISH_CACHE_BUDGET_UNKNOWN")
                    && faultOnce.compareAndSet(true, false))
                return ((CompletionStage<?>) invoke(actual, method, args)).thenCompose(value -> {
                    blockCacheBudgetReads.set(true);
                    return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("applied cache budget persistence response lost"));
                }).toCompletableFuture();
            if (selector && rolloverArmed.get() && M5BindingAuthorityCodecV1.decodeAuthority(CanonicalBytes.copyOf((byte[]) args[1]))
                    .selectorProjection().sourceGeneration() > rolloverPredecessor.sourceGeneration() && rolloverArmed.compareAndSet(true, false)) {
                return ((CompletionStage<?>) invoke(actual, method, args)).thenCompose(value -> {
                    blockSelectorReads.set(true);
                    rolloverLost.complete(null);
                    return rolloverReply.thenCompose(ignored -> CompletableFuture.failedFuture(
                            new java.util.concurrent.TimeoutException("applied selected raw-tail rollover response lost")));
                }).toCompletableFuture();
            }
            if (selector && mode.equals("TASK_OUTPUT_PUBLISH_UNKNOWN")
                    && M5BindingAuthorityCodecV1.decodeAuthority(CanonicalBytes.copyOf((byte[]) args[1])).taskSelectionDecision()
                    .filter(value -> value.outcome() == M5TaskSelectionDecisionV2.Outcome.SELECTED).isPresent()
                    && faultOnce.compareAndSet(true, false))
                return ((CompletionStage<?>) invoke(actual, method, args)).thenCompose(value -> {
                    blockSelectorReads.set(true);
                    return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("applied native Task publication response lost"));
                }).toCompletableFuture();
            return putTask(actual, method, args, key, selector);
        }

        private Object putTask(AsyncOxiaClient actual, java.lang.reflect.Method method, Object[] args, String key, boolean selector) throws Throwable {
            if (selector && mode.equals("TASK_OUTPUT_WORKER_UNKNOWN") && registered != null
                    && M5BindingAuthorityCodecV1.decodeAuthority(CanonicalBytes.copyOf((byte[]) args[1])).materializationWorker()
                    .filter(worker -> worker.taskIdSha256().equals(registered.taskIdSha256())).isPresent()
                    && faultOnce.compareAndSet(true, false))
                return ((CompletionStage<?>) invoke(actual, method, args)).thenCompose(value -> {
                    blockSelectorReads.set(true);
                    return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("applied native output worker response lost"));
                }).toCompletableFuture();
            if (key.contains("/kafka-bk-compaction-v2/") && key.endsWith("/task")) {
                nativeTaskKey = key;
                registered = KafkaBookKeeperInventoryCodecV2.decodeTask(CanonicalBytes.copyOf((byte[]) args[1]));
                if (mode.equals("TASK_REGISTER_UNKNOWN") && faultOnce.compareAndSet(true, false))
                    return withhold(actual, method, args);
            }
            if (withholdConversion(key, args)) {
                if (mode.equals("TASK_CONVERT_UNKNOWN")) return withhold(actual, method, args);
                return ((CompletionStage<?>) invoke(actual, method, args)).thenCompose(value -> {
                    blockCapacityReads.set(true);
                    return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("applied native task conversion response lost"));
                }).toCompletableFuture();
            }
            if (selector && cancelArmed.compareAndSet(true, false))
                return ((CompletionStage<?>) invoke(actual, method, args)).thenCompose(value -> {
                    blockSelectorReads.set(true);
                    return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("applied prepared task cancellation response lost"));
                }).toCompletableFuture();
            return invoke(actual, method, args);
        }

        private boolean coveredRawReadForbidden(KafkaRunRootRecordV2 record, int broker) {
            return (rejectCoveredRaw || broker == 1 && mode.startsWith("TASK_OUTPUT_PUBLISH") && mode.contains("_COLD"))
                    && record.root().kafkaEndOffsetExclusive().isPresent()
                    && record.root().kafkaEndOffsetExclusive().orElseThrow() <= selectedCutEnd;
        }

        private BookKeeperCellSession outputSession(BookKeeperCellSession actual) {
            int ordinal = outputSessions.incrementAndGet();
            return (BookKeeperCellSession) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] {BookKeeperCellSession.class}, (proxy, method, args) -> {
                        var response = invoke(actual, method, args);
                        if (method.getName().equals("appendExplicitEntry") && mode.equals("TASK_OUTPUT_APPEND_UNKNOWN")
                                && faultOnce.compareAndSet(true, false))
                            return ((CompletionStage<?>) response).thenApply(value -> {
                                assertEquals(ProviderMutationOutcomeV1.APPLIED_EXACT, ((ProviderMutationResultV1<?>) value).outcome());
                                return ProviderMutationResultV1.outcomeUnknown();
                            });
                        if (method.getName().equals("closeAsync") && (mode.startsWith("TASK_OUTPUT_CLOSE")
                                || mode.equals("TASK_OUTPUT_PUBLISH_CLOSE_HELD") && ordinal == 2)
                                && faultOnce.compareAndSet(true, false))
                            return ((CompletionStage<?>) response).thenCompose(value -> {
                                outputCloseSeen.complete(null);
                                return mode.endsWith("CLOSE_HELD") ? releaseOutputClose : CompletableFuture.failedFuture(
                                        new java.util.concurrent.TimeoutException("completed native output SDK close response lost"));
                            });
                        return response;
                    });
        }

        private BoundPhysicalDeleteAuthorityRouteV2 outputAuthorityRoute(BoundPhysicalDeleteAuthorityRouteV2 actual) {
            if (!mode.equals("TASK_OUTPUT_TICKET_UNKNOWN")) return actual;
            return (BoundPhysicalDeleteAuthorityRouteV2) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] {BoundPhysicalDeleteAuthorityRouteV2.class}, (proxy, method, args) -> {
                        if (method.getName().equals("read") && blockOutputTicketReads.get() && args[0].equals(outputTicketKey))
                            return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("native output ticket reread unknown"));
                        if (method.getName().equals("compareAndSet") && args[0] instanceof Optional<?> before && before.isPresent()) {
                            var next = M5TargetDeleteAuthorityCodecV1.decodeAuthority((CanonicalBytes) args[2]);
                            var previous = M5TargetDeleteAuthorityCodecV1.decodeAuthority(((VersionedValue) before.orElseThrow()).canonicalStoredBytes());
                            if (outputParts != null && outputParts.stream().anyMatch(part -> part.part().resource().authorityKey().equals(args[1]))
                                    && previous.activeWriterTickets().size() > next.activeWriterTickets().size()
                                    && faultOnce.compareAndSet(true, false)) {
                                outputTicketKey = (String) args[1];
                                return ((CompletionStage<?>) invoke(actual, method, args)).thenCompose(value -> {
                                    blockOutputTicketReads.set(true);
                                    return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("applied native output ticket release response lost"));
                                });
                            }
                        }
                        return invoke(actual, method, args);
                    });
        }

        private boolean withholdConversion(String key, Object[] args) {
            if (!key.endsWith("/kafka-compaction-admission") || registered == null || !mode.startsWith("TASK_CONVERT")) return false;
            var state = KafkaCompactionAdmissionV1.decode(CanonicalBytes.copyOf((byte[]) args[1]));
            if (state.reservations().size() != 1
                    || !state.reservations().get(0).compactionTaskIdSha256().equals(registered.taskIdSha256())
                    || !faultOnce.compareAndSet(true, false)) return false;
            nativeCapacityKey = key;
            formalCandidate = state;
            return true;
        }

        private Object withhold(AsyncOxiaClient client, java.lang.reflect.Method method, Object[] args) {
            delayed = () -> {
                try {
                    return (CompletionStage<?>) invoke(client, method, args);
                } catch (Throwable failure) {
                    return CompletableFuture.failedFuture(failure);
                }
            };
            return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("native prepared task mutation withheld"));
        }

        private BookKeeperCellSession fallbackReadSession(BookKeeperCellSession actual) {
            return (BookKeeperCellSession) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] {BookKeeperCellSession.class}, (proxy, method, args) -> {
                        Object response = invoke(actual, method, args);
                        if (method.getName().equals("readExactEntry") && holdFallbackRead.compareAndSet(true, false))
                            return ((CompletionStage<?>) response).thenCompose(value -> {
                                fallbackReadEntered.complete(null);
                                return releaseFallbackRead.thenApply(ignored -> value);
                            }).toCompletableFuture();
                        return response;
                    });
        }

        private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args) throws Throwable {
            try {
                return method.invoke(target, args);
            } catch (java.lang.reflect.InvocationTargetException failure) {
                throw failure.getCause();
            }
        }
    }

    private static final class State {
        private final String mode;
        private final Task task;
        private final AtomicInteger cleanupFailures = new AtomicInteger();
        private final AtomicBoolean blockCleanup = new AtomicBoolean(true);
        private final AtomicReference<Sha256Digest> cleanupOperation = new AtomicReference<>();
        private final AtomicInteger sdkCloses = new AtomicInteger();
        private final AtomicBoolean failInputRead = new AtomicBoolean();
        private final AtomicBoolean withholdInputAdmission = new AtomicBoolean();
        private final AtomicReference<java.util.function.Supplier<CompletionStage<com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome>>>
                lateInputAdmission = new AtomicReference<>();
        private volatile long cleanupLedger = -1;
        private final CompletableFuture<NereusKafkaOwnedProviderRuntime.Opened> zero = new CompletableFuture<>();
        private final CompletableFuture<NereusKafkaOwnedProviderRuntime.Opened> one = new CompletableFuture<>();
        private final CompletableFuture<NereusKafkaOwnedProviderRuntime.Opened> foreign = new CompletableFuture<>();
        private final AtomicBoolean hold = new AtomicBoolean();
        private final CompletableFuture<KafkaRunRootRecordV2> held = new CompletableFuture<>();
        private final CompletableFuture<Void> releaseClose = new CompletableFuture<>();
        private volatile BoundPhysicalDeleteAuthorityRouteV2 route;

        private State(String mode) {
            this.mode = mode;
            this.task = new Task(mode, one);
        }

        private boolean withholdAcquisition(BoundPhysicalDeleteAuthorityRouteV2 actual, Optional<?> before, Object[] args,
                com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1 next,
                com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1 previous) {
            if (next.target().resourceId() instanceof PhysicalResourceIdV2.BookKeeperLedger ledger
                    && ledger.ledgerId() == cleanupLedger && next.activeWriterTickets().size() > previous.activeWriterTickets().size()
                    && withholdInputAdmission.compareAndSet(true, false)) {
                lateInputAdmission.set(() -> actual.compareAndSet(before.map(value -> (VersionedValue) value),
                        (String) args[1], (CanonicalBytes) args[2]));
                return true;
            }
            return false;
        }

        private NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver observer(int broker) {
            return new NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver() {
                @Override
                public AsyncOxiaClient controlClient(AsyncOxiaClient actual) {
                    if (broker == 0 && mode.startsWith("TASK")) return task.gcInterruption.client(mode, task.client(actual));
                    return broker == 1 ? task.coldScope.client(actual) : actual;
                }
                @Override
                public BoundPhysicalDeleteAuthorityRouteV2 writerAuthorityRoute(BoundPhysicalDeleteAuthorityRouteV2 actual) {
                    if (broker == 0) {
                        task.outputRoute = actual;
                        route = actual;
                        if (mode.equals("TASK_OUTPUT_TICKET_UNKNOWN")) return task.outputAuthorityRoute(actual);
                    }
                    if (broker != 0 || (!mode.equals("CLEANUP") && !mode.equals("CAPACITY"))) return actual;
                    return (BoundPhysicalDeleteAuthorityRouteV2) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                            new Class<?>[] {BoundPhysicalDeleteAuthorityRouteV2.class}, (proxy, method, args) -> {
                                if (method.getName().equals("compareAndSet") && args[0] instanceof Optional<?> before && before.isPresent()) {
                                    var next = M5TargetDeleteAuthorityCodecV1.decodeAuthority((CanonicalBytes) args[2]);
                                    var previous = M5TargetDeleteAuthorityCodecV1.decodeAuthority(((VersionedValue) before.orElseThrow()).canonicalStoredBytes());
                                    if (withholdAcquisition(actual, before, args, next, previous)) {
                                        return CompletableFuture.completedFuture(
                                                com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome.RESPONSE_UNKNOWN);
                                    }
                                    if (next.target().resourceId() instanceof PhysicalResourceIdV2.BookKeeperLedger ledger
                                            && ledger.ledgerId() == cleanupLedger && previous.activeWriterTickets().size() > next.activeWriterTickets().size()
                                            && blockCleanup.get()) {
                                        cleanupFailures.incrementAndGet();
                                        cleanupOperation.compareAndSet(null, previous.activeWriterTickets().get(0).operationIdSha256());
                                        return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("no-session input ticket cleanup unknown"));
                                    }
                                }
                                try {
                                    return method.invoke(actual, args);
                                } catch (java.lang.reflect.InvocationTargetException failure) {
                                    throw failure.getCause();
                                }
                            });
                }

                @Override
                public BookKeeperCellSession compactionSession(BookKeeperCellSession actual) {
                    return broker == 0 && mode.startsWith("TASK_OUTPUT") ? task.outputSession(actual) : actual;
                }

                @Override
                public CompletionStage<ProviderMutationResultV1<AppendQuorumProofV1>> afterAppend(
                        RunLedgerAppendRequestV1 request, CompletionStage<ProviderMutationResultV1<AppendQuorumProofV1>> response) {
                    return broker == 1 ? task.coldScope.pendingAppend.observe(request, response) : response;
                }

                @Override
                public void beforeNativeSourcePublication(NereusKafkaOwnedProviderRuntime.Opened opened,
                        com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1.NativeReadSources sources) {
                    if (broker == 0 && task.rolloverArmed.get()) task.rolloverSources = sources;
                }

                @Override
                public void afterRecovery(org.apache.kafka.common.TopicIdPartition partition, Throwable failure) {
                    KafkaBookKeeperM5PolicyRealScenario.recoveryFailure(partition, failure, broker, zero, one);
                }

                @Override
                public void afterReady(org.apache.kafka.common.TopicIdPartition partition, NereusKafkaOwnedProviderRuntime.Opened opened) {
                    if (partition.topic().equals("native-foreign") && broker == 0) foreign.complete(opened);
                    if (!partition.topic().equals("native-cold")) return;
                    if (broker == 0) zero.complete(opened);
                    if (broker == 1) one.complete(opened);
                }

                @Override
                public com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession historyReaderSession(KafkaRunRootRecordV2 record,
                        KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase phase,
                        com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession actual) {
                    if (task.coveredRawReadForbidden(record, broker))
                        throw new AssertionError("selected Owner dispatched a reader for its covered raw predecessor");
                    if (broker == 0 && phase == KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase.FETCH && task.holdFallbackRead.get())
                        return task.fallbackReadSession(actual);
                    if (phase != KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase.M5_INPUT || !failInputRead.get()) return actual;
                    return (com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession) java.lang.reflect.Proxy.newProxyInstance(
                            getClass().getClassLoader(), new Class<?>[] {com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession.class},
                            (proxy, method, args) -> {
                                final Object response;
                                try {
                                    response = method.invoke(actual, args);
                                } catch (java.lang.reflect.InvocationTargetException failure) {
                                    throw failure.getCause();
                                }
                                if (method.getName().equals("readExactEntry") && failInputRead.compareAndSet(true, false))
                                    return ((CompletionStage<?>) response).thenCompose(ignored -> CompletableFuture.failedFuture(
                                            new IllegalStateException("injected native M5 input read failure")));
                                return response;
                            });
                }

                @Override
                public CompletionStage<Void> afterHistoryReaderClose(KafkaRunRootRecordV2 record,
                        KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase phase, CompletionStage<Void> actual) {
                    if (phase == KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase.M5_INPUT) sdkCloses.incrementAndGet();
                    if (phase != KafkaBookKeeperClosedHistoryRecoveryV1.ReadPhase.M5_INPUT || !hold.compareAndSet(true, false)) return actual;
                    return actual.thenCompose(ignored -> {
                        held.complete(record);
                        return releaseClose;
                    });
                }
            };
        }
    }
}
