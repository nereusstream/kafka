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
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperNativeFallbackRetirementV2;
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadResultV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadOutcomeV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperNativeReadScopeV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperNativeRecoveryScopeV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionAdmissionV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperDescriptorV2;
import com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper;
import com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceDispositionV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import io.oxia.client.api.AsyncOxiaClient;
import kafka.server.nsip.KafkaBookKeeperOwnerRelocationRealTest;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidTimestampException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Independent actual stored proof and native fact checks after the owned Broker transfer. */
final class KafkaBookKeeperReadScopeRealScenario {

    static void verifyUnpublished(AsyncOxiaClient actual, String mode, ReplacementFactFault fault,
            NereusKafkaOwnedProviderRuntime.Opened opened,
            com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.Task task,
            java.util.List<com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionWriterV2.VerifiedPart> output) throws Exception {
        var facts = opened.captureNativeM5UnpublishedEvidence(task.taskIdSha256()).toCompletableFuture().get(30, TimeUnit.SECONDS);
        assertEquals(task, facts.terminal().task());
        assertTrue(facts.createScope().fenced());
        assertEquals(task.parts().size(), facts.partInventory().size());
        assertEquals(output.stream().map(value -> value.part().resource()).collect(java.util.stream.Collectors.toSet()), facts.sealed().keySet());
        assertEquals(facts.sealed().keySet(), facts.physical().keySet());
        assertEquals(facts.sealed().keySet(), facts.members().keySet());
        for (var part : output) assertEquals(part.sealedMetadata(), facts.sealed().get(part.part().resource()).closedMetadata());
        var nativeStore = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(actual);
        for (var resource : facts.sealed().keySet()) {
            var taskEvidence = facts.unpublishedTaskEvidence(resource);
            assertEquals(task.taskIdSha256(), taskEvidence.taskAttemptId());
            for (var fact : List.of(taskEvidence.terminalTask(), taskEvidence.fencedTaskOwner(),
                    taskEvidence.closedAdoptionAndResponseLossPaths(), taskEvidence.neverAdmittedProtection().orElseThrow())) {
                var independent = nativeStore.read(fact.key()).toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow();
                assertEquals(independent.metadataVersion(), fact.metadataVersion());
                assertEquals(independent.canonicalStoredSha256(), fact.valueSha256());
            }
        }
        if (!facts.sealed().isEmpty()) {
            var original = facts.sealed().keySet().iterator().next();
            assertThrows(IllegalArgumentException.class, () -> facts.unpublishedTaskEvidence(
                    new com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger(original.namespace(), Long.MAX_VALUE)));
        }
        KafkaBookKeeperM5UnpublishedRealScenario.verifyInputs(opened, task.taskIdSha256());
        if (mode.equals("TASK_OUTPUT") || mode.equals("TASK_OUTPUT_READ"))
            fault.verifyUnpublished(actual, opened, facts, mode.equals("TASK_OUTPUT"));
    }
    private static final String EMPTY_TAIL = "TASK_OUTPUT_PUBLISH_RELEASE_COLD_NO_TAIL";

    private KafkaBookKeeperReadScopeRealScenario() { }

    static NewTopic configureEmptyTail(NewTopic topic, String mode) {
        if (mode.equals(EMPTY_TAIL)) {
            var config = new java.util.HashMap<>(topic.configs());
            config.put(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, "1000");
            topic.configs(config);
        }
        return topic;
    }

    static int prepareTransactionHistory(String mode, Admin admin, NereusKafkaOwnedProviderRuntime.Opened opened,
            Properties client, AppendOne append) throws Exception {
        if (!mode.contains("_TRANSACTION_")) return 0;
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
        config.put("transactional.id", "native-replacement-history");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            producer.initTransactions();
            producer.beginTransaction();
            for (int offset = 0; offset < 12; offset++) {
                assertEquals((long) offset, producer.send(new ProducerRecord<>("native-cold", 0, "transaction-key", "committed-" + offset))
                        .get(30, TimeUnit.SECONDS).offset());
            }
            producer.commitTransaction();
            producer.beginTransaction();
            for (int offset = 13; offset < 19; offset++) {
                assertEquals((long) offset, producer.send(new ProducerRecord<>("native-cold", 0, "aborted-key", "aborted-" + offset))
                        .get(30, TimeUnit.SECONDS).offset());
            }
            producer.abortTransaction();
        }
        int offset = 20;
        while (opened.storage().nativeReadSources().activeRoot().kafkaStartOffset() <= 22 && offset < 54) append.append(offset++);
        assertTrue(opened.storage().nativeReadSources().activeRoot().kafkaStartOffset() > 22,
                "transaction markers and superseded ordinary DATA did not reach sealed input");
        verifyTransactionHistory(opened.storage().nativeInputSources());
        return offset;
    }

    private static void verifyTransactionHistory(KafkaBookKeeperPartitionV1.NativeInputSources inputs) {
        var transactions = inputs.protocol().transactionState();
        assertTrue(transactions.ongoingTransactions().isEmpty());
        assertEquals(List.of(0L, 13L), transactions.completedTransactions().stream().map(row -> row.firstOffset()).toList());
        assertEquals(List.of(13L, 20L), transactions.completedTransactions().stream().map(row -> row.markerEndOffsetExclusive()).toList());
        assertEquals(List.of(false, true), transactions.completedTransactions().stream().map(row -> row.aborted()).toList());
        assertEquals(List.of(13L), transactions.abortedTransactions().stream().map(row -> row.firstOffset()).toList());
        assertEquals(transactions, inputs.recoveryCheckpoint().orElseThrow().transactionState(),
                "actual recovery checkpoint lost completed or aborted native transaction history");
    }

    /** Native pre-offset admission rolls first; rejected validation releases admission without appending DATA. */
    static int prepareEmptyTail(String mode, NereusKafkaOwnedProviderRuntime.Opened opened,
            Properties client, int offset, AppendOne append) throws Exception {
        if (!mode.equals(EMPTY_TAIL)) return offset;
        for (int attempt = 0; attempt < 8; attempt++) {
            var before = opened.storage().nativeInputSources();
            rejectFutureTimestamp(client);
            var after = opened.storage().nativeInputSources();
            assertEquals(before.protocol().root().frontiers(), after.protocol().root().frontiers(),
                    "rejected native Produce advanced logical frontiers");
            if (!before.sources().activeRoot().runId().equals(after.sources().activeRoot().runId())) {
                assertEquals((long) offset, after.sources().activeRoot().kafkaStartOffset());
                assertEquals((long) offset, after.protocol().root().frontiers().durableEndOffset());
                assertEquals(before.sources().sealedRoots().size() + 1, after.sources().sealedRoots().size());
                return offset;
            }
            append.append(offset++);
        }
        throw new AssertionError("rejected native Produce did not expose its actual empty rollover tail");
    }

    private static void rejectFutureTimestamp(Properties client) throws Exception {
        var config = new Properties();
        config.putAll(client);
        config.put("key.serializer", StringSerializer.class.getName());
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("acks", "all");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> producer.send(
                    new ProducerRecord<>("native-cold", 0, System.currentTimeMillis() + 60000, "key", "rejected"))
                    .get(30, TimeUnit.SECONDS));
            assertInstanceOf(InvalidTimestampException.class, failure.getCause());
        }
    }

    static void verifyPublicationTail(String mode, NereusKafkaOwnedProviderRuntime.Opened opened, long cutEnd,
            KafkaBookKeeperReadResultV1 tail) {
        if (mode.equals(EMPTY_TAIL)) {
            assertEquals(KafkaBookKeeperReadOutcomeV1.END_OF_SNAPSHOT, tail.outcome(), tail.detail());
            assertTrue(tail.batches().isEmpty());
            assertEquals(cutEnd, opened.storage().nativeInputSources().protocol().root().frontiers().durableEndOffset());
        } else assertEquals(KafkaBookKeeperReadOutcomeV1.FOUND, tail.outcome(), "fixture lacks actual post-cut Produce data");
    }

    @FunctionalInterface
    interface AppendOne {
        void append(int offset) throws Exception;
    }

    @FunctionalInterface
    interface VerifyFetch {
        void verify(KafkaSealedBookKeeperDescriptorV2 descriptor, List<CanonicalBytes> expected,
                NereusKafkaOwnedProviderRuntime.Opened opened, Properties client) throws Exception;
    }

    static void verifyFetch(KafkaSealedBookKeeperDescriptorV2 descriptor, List<CanonicalBytes> expected,
            NereusKafkaOwnedProviderRuntime.Opened opened, Properties client) throws Exception {
        var read = opened.storage().read(new com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperSequentialReadRequestV1(
                descriptor.sourceCut().coverage().inclusiveStart(), com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1.REPLICA,
                Long.MAX_VALUE, Optional.empty())).toCompletableFuture().get(30, TimeUnit.SECONDS);
        assertEquals(KafkaBookKeeperReadOutcomeV1.FOUND, read.outcome(), read.detail());
        assertEquals(expected, read.batches().stream().map(batch -> batch.rawAssignedRecordBatch()).toList(),
                "selected native Fetch resurrected the raw predecessor or lost its actual tail");
        assertEquals(0, read.batches().get(0).entryId());
        var inputs = opened.storage().nativeInputSources();
        var transactions = inputs.protocol().transactionState();
        if (!transactions.completedTransactions().isEmpty()) verifyTransactionHistory(inputs);
        var values = new ArrayList<String>();
        for (var body : expected) {
            for (var batch : MemoryRecords.readableRecords(ByteBuffer.wrap(body.toByteArray())).batches()) {
                if (batch.isControlBatch()) continue;
                boolean aborted = batch.isTransactional() && transactions.abortedTransactions().stream().anyMatch(transaction ->
                        transaction.producerId() == batch.producerId() && batch.baseOffset() >= transaction.firstOffset()
                                && batch.lastOffset() < transaction.markerEndOffsetExclusive());
                if (aborted) continue;
                for (var record : batch) values.add(StandardCharsets.UTF_8.decode(record.value().duplicate()).toString());
            }
        }
        if (!transactions.completedTransactions().isEmpty()) {
            assertEquals(java.util.stream.IntStream.range(0, 12).mapToObj(offset -> "committed-" + offset).toList(),
                    values.stream().filter(value -> value.startsWith("committed-")).toList());
            assertFalse(values.stream().anyMatch(value -> value.startsWith("aborted-")));
        }
        assertEquals(values, KafkaBookKeeperOwnerRelocationRealTest.consume(client, values.size()),
                "ordinary native Broker Fetch differs from the selected descriptor and actual live tail");
    }


    static final class ColdScopeFault {
        final PendingAppend pendingAppend;
        private final boolean active;
        private final AtomicBoolean unused = new AtomicBoolean(true);
        private final CompletableFuture<Void> readSeen = new CompletableFuture<>();
        private final CompletableFuture<Void> release = new CompletableFuture<>();
        private volatile String key;

        private final ReplacementFactFault replacementFacts;

        ColdScopeFault(String mode) {
            pendingAppend = new PendingAppend(mode);
            active = mode.endsWith("_COLD_SCOPE_HELD");
            replacementFacts = new ReplacementFactFault(mode);
        }

        AsyncOxiaClient client(AsyncOxiaClient actual) {
            if (!active) return replacementFacts.client(actual);
            return replacementFacts.client((AsyncOxiaClient) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] {AsyncOxiaClient.class}, (proxy, method, args) -> {
                        String next = args != null && args.length > 0 && args[0] instanceof String value ? value : "";
                        final Object response;
                        try {
                            response = method.invoke(actual, args);
                        } catch (java.lang.reflect.InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                        if (method.getName().equals("put") && shouldLose(next)) return lose((CompletionStage<?>) response);
                        if (method.getName().equals("get") && holds(next)) return hold((CompletionStage<?>) response);
                        return response;
                    }));
        }

        boolean shouldLose(String next) {
            if (!active || !next.contains("/native-recovery-scope/") || !unused.compareAndSet(true, false)) return false;
            key = next;
            return true;
        }

        boolean holds(String next) {
            return next.equals(key);
        }

        CompletionStage<?> lose(CompletionStage<?> actual) {
            return actual.thenCompose(ignored -> CompletableFuture.failedFuture(
                    new java.util.concurrent.TimeoutException("applied cold scope create response lost"))).toCompletableFuture();
        }

        CompletionStage<?> hold(CompletionStage<?> actual) {
            return actual.thenCompose(value -> {
                readSeen.complete(null);
                return release.thenApply(ignored -> value);
            }).toCompletableFuture();
        }
    }

    static void verifyColdOwner(AsyncOxiaClient actual, String nativeTaskKey, String selectorKey,
            KafkaSealedBookKeeperDescriptorV2 descriptor, List<CanonicalBytes> expected,
            KafkaCompactionAdmissionV1 admission, CompletableFuture<NereusKafkaOwnedProviderRuntime.Opened> coldOwner,
            Sha256Digest oldTask, Properties client, ColdScopeFault fault, AppendOne append, VerifyFetch fetch) throws Exception {
        try (var admin = Admin.create(client)) {
            var partition = new TopicPartition("native-cold", 0);
            if (fault.active) {
                admin.alterPartitionReassignments(Map.of(partition, Optional.of(new NewPartitionReassignment(List.of(1)))))
                        .all().get(30, TimeUnit.SECONDS);
                CompletableFuture.anyOf(fault.readSeen, coldOwner).get(30, TimeUnit.SECONDS);
                assertTrue(fault.readSeen.isDone(), "actual cold scope confirmation was not withheld");
                assertFalse(coldOwner.isDone(), "cold Owner became ready before its exact scope confirmation");
                assertFalse(fault.unused.get(), "actual applied cold scope response loss was not exercised");
                assertNotEmptyCache(admission, oldTask);
                assertTrue(actual.get(fault.key).get(30, TimeUnit.SECONDS) != null, "cold scope fault preceded actual persistence");
                fault.release.complete(null);
                assertEquals(1, KafkaBookKeeperOwnerRelocationRealTest.awaitLeader(admin, 0));
            } else KafkaBookKeeperRootVerifierRealScenario.reassign(admin, partition, 1, 0);
            var opened = coldOwner.get(30, TimeUnit.SECONDS);
            var cache = assertNotEmptyCache(admission, oldTask);
            verifyColdScope(actual, nativeTaskKey, selectorKey, descriptor, opened.storage());
            fetch.verify(descriptor, expected, opened, client);
            fault.pendingAppend.verify(opened, client);
            fault.replacementFacts.verify(actual, descriptor, opened, () -> {
                append.append(Math.toIntExact(opened.storage().nativeInputSources().protocol().root().frontiers().durableEndOffset()));
                return null;
            });
            verifyColdReplacementFacts(actual, selectorKey, descriptor, opened);
            KafkaBookKeeperM5ReplacementGcRealScenario.reclaimIfAdmitted(opened, false);
            int next = Math.toIntExact(opened.storage().nativeInputSources().protocol().root().frontiers().durableEndOffset());
            append.append(next);
            var after = opened.storage().read(new com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperSequentialReadRequestV1(
                    descriptor.sourceCut().coverage().inclusiveStart(), com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1.REPLICA,
                    Long.MAX_VALUE, Optional.empty())).toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertEquals(com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadOutcomeV1.FOUND, after.outcome(), after.detail());
            assertEquals(expected, after.batches().subList(0, expected.size()).stream().map(batch -> batch.rawAssignedRecordBatch()).toList());
            fetch.verify(descriptor, after.batches().stream().map(batch -> batch.rawAssignedRecordBatch()).toList(), opened, client);
            assertEquals(cache, admission.read().orElseThrow(), "live Produce released the new Owner's selected cache charge");
            opened.closeAsync().get(30, TimeUnit.SECONDS);
            assertTrue(admission.read().orElseThrow().reservations().isEmpty(), "cold Owner drain leaked its selected cache charge");
        } finally {
            fault.release.complete(null);
            fault.pendingAppend.release.complete(null);
        }
    }

    /** Withholds one completed actual DATA append response before ordered protocol publication. */
    static final class PendingAppend {
        private final boolean active;
        private final AtomicBoolean armed = new AtomicBoolean();
        private final CompletableFuture<Void> entered = new CompletableFuture<>();
        private final CompletableFuture<Void> release = new CompletableFuture<>();

        PendingAppend(String mode) {
            active = mode.endsWith("REPLACEMENT_SPECULATIVE_PENDING");
        }

        <T> CompletionStage<T> observe(com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1 request,
                CompletionStage<T> actual) {
            if (!armed.get()) return actual;
            byte[] body = new byte[request.payload().readableBytes()];
            request.payload().readOnlyBuffer().get(body);
            var frame = com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1.decode(body,
                    request.handle().ledgerIdentity().ledgerId(), request.expectedEntryId());
            if (!(frame instanceof com.nereusstream.kafka.bookkeeper.nbke2.Nbke2DataV1) || !armed.compareAndSet(true, false)) return actual;
            return actual.thenCompose(value -> {
                entered.complete(null);
                return release.thenApply(ignored -> value);
            });
        }

        void verify(NereusKafkaOwnedProviderRuntime.Opened opened, Properties client) throws Exception {
            if (!active) return;
            var before = opened.storage().nativeInputSources();
            var admission = opened.nativeM5CompactionAdmission();
            var cache = admission.read().orElseThrow();
            long offset = before.protocol().root().frontiers().durableEndOffset();
            int epoch = before.protocol().root().fence().kafkaLeaderEpoch();
            assertTrue(before.protocol().leaderEpochIndex().startOffsets().lastKey() < epoch,
                    "cold Owner already committed DATA in its new Kafka leader epoch");
            var config = new Properties();
            config.putAll(client);
            config.put("key.serializer", StringSerializer.class.getName());
            config.put("value.serializer", StringSerializer.class.getName());
            config.put("acks", "all");
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
                try {
                    armed.set(true);
                    var append = producer.send(new ProducerRecord<>("native-cold", 0, "pending", "pending-" + offset));
                    entered.get(30, TimeUnit.SECONDS);
                    assertFalse(append.isDone(), "completed DATA response hold acknowledged Produce");
                    var pending = opened.captureNativeM5ReplacementEvidence().toCompletableFuture().get(30, TimeUnit.SECONDS);
                    assertEquals(1, pending.recovery().protocol().speculativeQueue().commits().size());
                    var commit = pending.recovery().protocol().speculativeQueue().commits().get(0);
                    assertEquals(offset, commit.startOffset());
                    assertEquals(offset + 1, commit.endOffsetExclusive());
                    assertEquals(offset, pending.recovery().protocol().root().frontiers().durableEndOffset());
                    assertEquals(before.protocol().committedProducerState(), pending.recovery().protocol().committedProducerState());
                    assertEquals(before.protocol().leaderEpochIndex(), pending.recovery().protocol().leaderEpochIndex());
                    verifyProtocolFacts(pending);
                    requireProtocolReferences(pending, ReferenceDispositionV1.PRESENT, false);
                    assertEquals(cache, admission.read().orElseThrow(), "pending append observation freed the selected cache");
                    release.complete(null);
                    assertEquals(offset, append.get(30, TimeUnit.SECONDS).offset());
                    assertThrows(IllegalStateException.class, pending.selected()::recoveryInputs,
                            "ordered commit left a captured pending protocol qualification current");
                    var fresh = opened.captureNativeM5ReplacementEvidence().toCompletableFuture().get(30, TimeUnit.SECONDS);
                    assertTrue(fresh.recovery().protocol().speculativeQueue().commits().isEmpty());
                    assertEquals(offset + 1, fresh.recovery().protocol().root().frontiers().durableEndOffset());
                    assertEquals(Long.valueOf(offset), fresh.recovery().protocol().leaderEpochIndex().startOffsets().get(epoch));
                    assertEquals(before.protocol().leaderEpochIndex().startOffsets().size() + 1,
                            fresh.recovery().protocol().leaderEpochIndex().startOffsets().size());
                    verifyProtocolFacts(fresh);
                    requireProtocolReferences(fresh, ReferenceDispositionV1.ABSENT, true);
                    assertEquals(cache, admission.read().orElseThrow(), "ordered commit observation freed the selected cache");
                } finally {
                    release.complete(null);
                }
            }
        }

        private static void requireProtocolReferences(NereusKafkaReadScopeV2.ReplacementEvidence evidence,
                ReferenceDispositionV1 disposition, boolean complete) {
            var kinds = java.util.EnumSet.of(ReferenceKindV1.KAFKA_PRODUCER_RECOVERY, ReferenceKindV1.KAFKA_TRANSACTION_OR_ABORTED_RECOVERY,
                    ReferenceKindV1.KAFKA_REPLICA_OR_LEADER_EPOCH_RECOVERY);
            for (var rows : evidence.references().values()) {
                var observed = rows.stream().filter(row -> kinds.contains(row.kind())).toList();
                assertEquals(kinds, observed.stream().map(row -> row.kind()).collect(java.util.stream.Collectors.toSet()));
                assertTrue(observed.stream().allMatch(row -> row.disposition() == disposition && row.enumerationComplete() == complete));
            }
        }
    }

    private static KafkaCompactionAdmissionV1.State assertNotEmptyCache(KafkaCompactionAdmissionV1 admission, Sha256Digest oldTask) {
        var cache = admission.read().orElseThrow();
        assertEquals(1, cache.reservations().size(), "new Owner did not retain its actual selected cache charge");
        assertFalse(cache.reservations().get(0).compactionTaskIdSha256().equals(oldTask), "new Owner reused the fenced Task's capacity identity");
        return cache;
    }

    private static void verifyColdReplacementFacts(AsyncOxiaClient actual, String selectorKey,
            KafkaSealedBookKeeperDescriptorV2 descriptor, NereusKafkaOwnedProviderRuntime.Opened opened) throws Exception {
        var selector = M5BindingAuthorityCodecV1.decodeAuthority(CanonicalBytes.copyOf(
                actual.get(selectorKey).get(30, TimeUnit.SECONDS).value())).selectorProjection();
        if (selector.mode() == com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode.PREFERRED_ONLY) {
            verifyReplacementEvidence(actual, selectorKey.substring(0, selectorKey.indexOf("/v2/")), descriptor, opened, Optional.empty());
            return;
        }
        assertEquals(com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode.PREFERRED_WITH_FALLBACK, selector.mode());
        var admission = opened.nativeM5CompactionAdmission();
        var cache = admission.read().orElseThrow();
        var failure = assertThrows(java.util.concurrent.ExecutionException.class,
                () -> opened.captureNativeM5ReplacementEvidence().toCompletableFuture().get(30, TimeUnit.SECONDS));
        assertEquals("native selected input requires its released stable read actor", failure.getCause().getMessage());
        assertEquals(cache, admission.read().orElseThrow(), "protected cold-source rejection released selected cache capacity");
    }

    private static void verifyColdScope(AsyncOxiaClient actual, String nativeTaskKey, String selectorKey,
            KafkaSealedBookKeeperDescriptorV2 descriptor, KafkaBookKeeperPartitionV1 storage) throws Exception {
        String root = nativeTaskKey.substring(0, nativeTaskKey.indexOf("/v2/"));
        var selector = M5BindingAuthorityCodecV1.decodeAuthority(CanonicalBytes.copyOf(
                actual.get(selectorKey).get(30, TimeUnit.SECONDS).value())).selectorProjection();
        String key = root + "/v2/kafka-bk-compaction-v2/views/" + descriptor.descriptorSha256().toHex()
                + "/native-recovery-scope/" + selector.ownerEpoch() + "/" + selector.readAdmissionEpoch();
        var stored = actual.get(key).get(30, TimeUnit.SECONDS);
        var bytes = CanonicalBytes.copyOf(stored.value());
        var scope = KafkaBookKeeperNativeRecoveryScopeV2.decode(bytes);
        scope.requireDescriptor(descriptor, predecessor(actual, root, descriptor));
        assertEquals(key, root + "/" + scope.key());
        assertEquals(bytes, scope.encode());
        assertEquals(selector.mode(), scope.mode());
        assertEquals(selector.sourceGeneration(), scope.sourceGeneration());
        assertEquals(com.nereusstream.storage.object.read.control.M4ReadControlCodecV1.selectorCoreSha256(selector), scope.selectorCoreSha256());
        var inputs = storage.nativeInputSources();
        assertEquals(inputs.protocol().root().stateVersion(), scope.snapshot().stateVersion());
        assertEquals(inputs.sources().selectedViewSha256(), scope.snapshot().sourceMap());
        assertEquals(inputs.protocol().root().references().checkpointVector().contentDigest(), scope.snapshot().checkpointRoot());
        var raw = new ArrayList<Long>();
        inputs.sources().sealedRoots().forEach(member -> raw.add(member.ledgerIdentity().ledgerId()));
        raw.add(inputs.sources().activeRoot().ledgerIdentity().ledgerId());
        assertEquals(raw.stream().sorted().toList(), scope.rawLedgerIds());
        assertEquals(descriptor.sealedParts().stream().map(part -> part.handle().ledgerIdentity().ledgerId()).sorted().toList(), scope.selectedLedgerIds());
        verifyOwnerFact(actual, scope.ownerFact(), scope.owner(), false, scope.nativeScopeSha256());
        verifyOwnerFact(actual, scope.closedOwnerFact(), scope.predecessor(), true, scope.nativeScopeSha256());
        byte[] corrupt = bytes.toByteArray();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> KafkaBookKeeperNativeRecoveryScopeV2.decode(CanonicalBytes.copyOf(corrupt)));
        assertThrows(IllegalArgumentException.class, () -> KafkaBookKeeperNativeRecoveryScopeV2.decode(
                CanonicalBytes.copyOf(Arrays.copyOf(bytes.toByteArray(), bytes.length() - 1))));
        assertEquals(stored.version(), actual.get(key).get(30, TimeUnit.SECONDS).version());
    }

    private static void verifyOwnerFact(AsyncOxiaClient actual,
            com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1 fact,
            com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1 owner, boolean closed, Sha256Digest scope) throws Exception {
        var stored = actual.get(fact.key()).get(30, TimeUnit.SECONDS);
        var bytes = CanonicalBytes.copyOf(stored.value());
        var admission = KafkaOwnerAdmissionV1.decode(bytes);
        assertEquals(owner, admission.owner());
        assertEquals(closed, admission.closed());
        assertEquals(scope, admission.scopeSha256());
        assertEquals(MetadataVersionMapper.fromOxia(stored.version().versionId()), fact.metadataVersion());
        assertEquals(Sha256Digest.hash(bytes), fact.valueSha256());
    }

    private static Optional<KafkaSealedBookKeeperDescriptorV2> predecessor(AsyncOxiaClient actual, String root,
            KafkaSealedBookKeeperDescriptorV2 descriptor) throws Exception {
        var first = descriptor.sourceCut().sources().get(0);
        if (first.kind() != com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind.KAFKA_BK_COMPACTED_GENERATION_V2)
            return Optional.empty();
        String key = root + "/" + com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionPublicationV2
                .descriptorKey(first.formatRootSha256());
        return Optional.of(com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperDescriptorCodecV2.decode(
                CanonicalBytes.copyOf(actual.get(key).get(30, TimeUnit.SECONDS).value())));
    }

    static void verifyOwnerRejected(CompletableFuture<KafkaBookKeeperNativeFallbackRetirementV2.Result> retirement,
            NereusKafkaOwnedProviderRuntime.Opened opened, KafkaBookKeeperPartitionV1.NativeInputSources before,
            com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionAdmissionV1.State formal,
            com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionAdmissionV1 admission) {
        var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> retirement.get(30, TimeUnit.SECONDS));
        assertTrue(failure.getCause().toString().contains("native fallback Owner admission changed during drain"), failure.toString());
        assertEquals(before, opened.storage().nativeInputSources(), "changed Owner admission modified its hot sources or checkpoint");
        assertEquals(formal, admission.read().orElseThrow(), "changed Owner admission freed selected cache capacity");
    }

    static CompletableFuture<KafkaBookKeeperNativeFallbackRetirementV2.Result> reconcileUnknown(AsyncOxiaClient actual, String scopeKey,
            AtomicBoolean blocked, KafkaSealedBookKeeperDescriptorV2 descriptor, KafkaBookKeeperPartitionV1.NativeInputSources before,
            com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionAdmissionV1.State formal,
            com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionAdmissionV1 admission,
            NereusKafkaOwnedProviderRuntime.Opened opened, Properties client) throws Exception {
        assertEquals(formal, admission.read().orElseThrow(), "unconfirmed scope released selected cache capacity");
        var stored = actual.get(scopeKey).get(30, TimeUnit.SECONDS);
        org.junit.jupiter.api.Assertions.assertNotNull(stored, "scope fault did not follow the actual applied create");
        KafkaBookKeeperRootVerifierRealScenario.verifyNativeSourceTransfer(opened.storage(), descriptor, before);
        KafkaBookKeeperRootVerifierRealScenario.verifyProduceFenced(opened, client);
        blocked.set(false);
        var retry = opened.retireNativeM5Fallback().toCompletableFuture();
        retry.get(30, TimeUnit.SECONDS);
        var repeated = actual.get(scopeKey).get(30, TimeUnit.SECONDS);
        assertEquals(stored.version(), repeated.version(), "scope retry replaced the original immutable proof");
        assertEquals(CanonicalBytes.copyOf(stored.value()), CanonicalBytes.copyOf(repeated.value()));
        return retry;
    }

    static void verify(AsyncOxiaClient actual, String nativeTaskKey, String ownerKey,
            KafkaSealedBookKeeperDescriptorV2 descriptor, KafkaBookKeeperPartitionV1.NativeInputSources before,
            NereusKafkaOwnedProviderRuntime.Opened opened, KafkaBookKeeperNativeFallbackRetirementV2.Result result,
            KafkaBookKeeperSelectedInputRealScenario.RecompactionFault fault) throws Exception {
        var storage = opened.storage();
        String root = nativeTaskKey.substring(0, nativeTaskKey.indexOf("/v2/"));
        String key = root + "/" + KafkaBookKeeperNativeReadScopeV2.key(descriptor.descriptorSha256(),
                before.protocol().root().fence().ownerEpoch(), result.terminal().readAdmissionEpoch());
        var stored = actual.get(key).get(30, TimeUnit.SECONDS);
        var bytes = CanonicalBytes.copyOf(stored.value());
        var scope = KafkaBookKeeperNativeReadScopeV2.decode(bytes);
        var previous = predecessor(actual, root, descriptor);
        assertEquals(result.transfer().original().publicationPreferred(), previous);
        scope.requireDescriptor(descriptor, previous);
        assertEquals(key, root + "/" + scope.key());
        assertEquals(bytes, scope.encode());
        assertEquals(before.protocol().root().stateVersion(), scope.before().stateVersion());
        assertEquals(before.sources().selectedViewSha256(), scope.before().sourceMap());
        assertEquals(before.protocol().root().references().checkpointVector().contentDigest(), scope.before().checkpointRoot());
        var after = storage.nativeInputSources();
        assertEquals(after.protocol().root().stateVersion(), scope.after().stateVersion());
        assertEquals(after.sources().selectedViewSha256(), scope.after().sourceMap());
        assertEquals(after.protocol().root().references().checkpointVector().contentDigest(), scope.after().checkpointRoot());
        var nativeOwner = actual.get(ownerKey).get(30, TimeUnit.SECONDS);
        var admission = KafkaOwnerAdmissionV1.decode(CanonicalBytes.copyOf(nativeOwner.value()));
        assertFalse(admission.closed());
        assertEquals(admission.owner(), scope.owner());
        assertEquals(admission.scopeSha256(), scope.nativeScopeSha256());
        assertEquals(ownerKey, scope.ownerFact().key());
        assertEquals(MetadataVersionMapper.fromOxia(nativeOwner.version().versionId()), scope.ownerFact().metadataVersion());
        assertEquals(Sha256Digest.hash(CanonicalBytes.copyOf(nativeOwner.value())), scope.ownerFact().valueSha256());
        assertEquals(result.batch().batchIdSha256(), scope.batchId());
        assertTrue(scope.retiredLedgerIds().stream().noneMatch(id -> after.sources().sealedRoots().stream()
                .anyMatch(source -> source.ledgerIdentity().ledgerId() == id)));
        byte[] corrupt = bytes.toByteArray();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> KafkaBookKeeperNativeReadScopeV2.decode(CanonicalBytes.copyOf(corrupt)));
        assertThrows(IllegalArgumentException.class, () -> KafkaBookKeeperNativeReadScopeV2.decode(
                CanonicalBytes.copyOf(Arrays.copyOf(bytes.toByteArray(), bytes.length() - 1))));
        var repeated = actual.get(key).get(30, TimeUnit.SECONDS);
        assertEquals(stored.version(), repeated.version());
        assertTrue(Arrays.equals(stored.value(), repeated.value()));
        fault.verifyReplacementRefresh(descriptor, opened);
        verifyReplacementEvidence(actual, root, descriptor, opened, Optional.of(scope.retiredLedgerIds()));
    }

    static void verifyReplacementEvidence(AsyncOxiaClient actual, String root, KafkaSealedBookKeeperDescriptorV2 descriptor, NereusKafkaOwnedProviderRuntime.Opened opened,
            Optional<List<Long>> retired) throws Exception {
        var evidence = opened.captureNativeM5ReplacementEvidence().toCompletableFuture().get(30, TimeUnit.SECONDS);
        assertEquals(descriptor, evidence.selected().descriptor());
        var nativeStore = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(actual);
        for (var captured : List.of(evidence.nativeDescriptor(), evidence.nativeSemantics())) {
            var independent = nativeStore.read(captured.key()).toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow();
            assertEquals(captured, independent);
        }
        var selectedResource = new com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger(
                descriptor.task().namespace(), descriptor.sealedParts().get(0).handle().ledgerIdentity().ledgerId());
        assertThrows(IllegalArgumentException.class, () -> evidence.replacementTaskEvidence(selectedResource));
        assertEquals(descriptor.sourceCut().sources().stream().map(source -> source.sourceIdentitySha256()).toList(),
                evidence.releases().stream().map(release -> release.sourceIdentitySha256()).toList());
        retired.ifPresent(ids -> assertEquals(ids, evidence.sources().stream()
                .map(source -> ((com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger) source.resource()).ledgerId()).sorted().toList()));
        assertEquals(evidence.sources().stream().map(source -> source.resource()).collect(java.util.stream.Collectors.toSet()), evidence.references().keySet());
        assertEquals(evidence.references().keySet(), evidence.physical().keySet());
        for (var source : evidence.sources()) {
            var replacement = evidence.replacementTaskEvidence(source.resource());
            assertEquals(evidence.nativeDescriptor().key(), replacement.selectedGeneration().key());
            assertEquals(evidence.nativeDescriptor().metadataVersion(), replacement.selectedGeneration().metadataVersion());
            assertEquals(evidence.nativeDescriptor().canonicalStoredSha256(), replacement.selectedGeneration().valueSha256());
            assertEquals(descriptor.sealedParts().size(), replacement.replacementResources().size());
            assertFalse(replacement.replacementResources().contains(source.resource()));
            for (var transfer : replacement.transfers()) {
                assertEquals(evidence.nativeSemantics().key(), transfer.verifierAuthority().key());
                assertEquals(evidence.nativeSemantics().metadataVersion(), transfer.verifierAuthority().metadataVersion());
                assertEquals(evidence.nativeSemantics().canonicalStoredSha256(), transfer.verifierAuthority().valueSha256());
                assertEquals(source.coverage(), transfer.coverage());
            }
            var extent = descriptor.sourceCut().sources().stream().filter(value -> value.sourceIdentitySha256().equals(source.sourceIdentity())).findFirst().orElseThrow();
            assertEquals(extent.coverage(), source.coverage());
            assertEquals(List.of(com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.SemanticAspect.values()),
                    source.transfers().stream().map(transfer -> transfer.aspect()).toList());
            assertEquals(source.transfers().stream().map(transfer -> transfer.aspect()).toList(),
                    replacement.transfers().stream().map(transfer -> transfer.aspect()).toList());
            var references = evidence.references().get(source.resource());
            assertEquals(java.util.EnumSet.of(ReferenceKindV1.MANIFEST_SELECTED, ReferenceKindV1.MANIFEST_FALLBACK,
                    ReferenceKindV1.SOURCE_PROTECTION, ReferenceKindV1.MATERIALIZATION_OR_COMPACTION_TASK,
                    ReferenceKindV1.READ_GENERATION_PIN_OR_OPEN_HANDLE, ReferenceKindV1.RECOVERY_CHECKPOINT_OR_SNAPSHOT,
                    ReferenceKindV1.KAFKA_PRODUCER_RECOVERY, ReferenceKindV1.KAFKA_TRANSACTION_OR_ABORTED_RECOVERY,
                    ReferenceKindV1.KAFKA_REPLICA_OR_LEADER_EPOCH_RECOVERY),
                    references.stream().map(row -> row.kind()).collect(java.util.stream.Collectors.toSet()));
            var baseKinds = java.util.EnumSet.of(ReferenceKindV1.MANIFEST_SELECTED, ReferenceKindV1.MANIFEST_FALLBACK,
                    ReferenceKindV1.SOURCE_PROTECTION);
            assertEquals(List.of(ReferenceKindV1.MANIFEST_SELECTED, ReferenceKindV1.MANIFEST_FALLBACK, ReferenceKindV1.SOURCE_PROTECTION),
                    references.stream().filter(row -> baseKinds.contains(row.kind())).map(row -> row.kind()).toList());
            var release = evidence.releases().stream().filter(value -> value.sourceIdentitySha256().equals(source.sourceIdentity())).findFirst().orElseThrow();
            for (var row : references) {
                assertEquals(source.resource().sha256(), row.targetIdentitySha256());
                assertEquals(source.coverage(), row.coverage());
                if (!baseKinds.contains(row.kind())) continue;
                assertEquals(ReferenceDispositionV1.ABSENT, row.disposition());
                assertTrue(row.enumerationComplete());
                if (row.kind() == ReferenceKindV1.SOURCE_PROTECTION) assertEquals(release.protectionAuthority(), row.authority());
                else {
                    assertEquals(evidence.selector().stored().key(), row.authority().key());
                    assertEquals(evidence.selector().stored().metadataVersion(), row.authority().metadataVersion());
                    assertEquals(evidence.selector().stored().canonicalStoredSha256(), row.authority().valueSha256());
                }
            }
        }
        verifyQuiescenceFacts(actual, root, evidence);
        verifyPhysicalFacts(actual, evidence);
        verifySealedFacts(actual, evidence);
        verifyMemberFacts(actual, evidence);
        verifyTaskFacts(actual, root, evidence);
        verifyTaskReferences(evidence);
        verifyNativeTaskFacts(evidence, Optional.empty());
        verifyReaderFacts(actual, root, evidence);
        verifyRecoveryFacts(opened, evidence);
        verifyProtocolFacts(evidence);
    }

    private static void verifyProtocolFacts(NereusKafkaReadScopeV2.ReplacementEvidence evidence) {
        var protocol = evidence.recovery().protocol();
        assertFalse(protocol.committedProducerState().producers().isEmpty(), "actual idempotent Produce omitted recovery state");
        if (!protocol.transactionState().completedTransactions().isEmpty()) verifyTransactionHistory(evidence.recovery());
        var owner = evidence.readers().owner().admission().owner();
        assertEquals(owner.kafkaLeaderEpoch(), protocol.root().fence().kafkaLeaderEpoch());
        assertEquals(owner.ownerEpoch(), protocol.root().fence().ownerEpoch());
        var epochs = protocol.leaderEpochIndex().startOffsets();
        assertFalse(epochs.isEmpty(), "actual native Produce omitted the leader-epoch recovery index");
        assertEquals(Long.valueOf(0), epochs.firstEntry().getValue());
        assertTrue(epochs.lastKey() <= owner.kafkaLeaderEpoch());
        evidence.recovery().recoveryCheckpoint().orElseThrow().leaderEpochIndex().startOffsets()
                .forEach((epoch, offset) -> assertEquals(offset, epochs.get(epoch), "current protocol lost a checkpoint leader-epoch boundary"));
        var kinds = java.util.EnumSet.of(ReferenceKindV1.KAFKA_PRODUCER_RECOVERY, ReferenceKindV1.KAFKA_TRANSACTION_OR_ABORTED_RECOVERY,
                ReferenceKindV1.KAFKA_REPLICA_OR_LEADER_EPOCH_RECOVERY);
        boolean closed = evidence.readers().closed() && protocol.speculativeQueue().commits().isEmpty();
        for (var source : evidence.sources()) {
            var checkpointRows = evidence.references().get(source.resource()).stream()
                    .filter(row -> row.kind() == ReferenceKindV1.RECOVERY_CHECKPOINT_OR_SNAPSHOT).toList();
            for (var kind : kinds) {
                var rows = evidence.references().get(source.resource()).stream().filter(row -> row.kind() == kind).toList();
                assertEquals(checkpointRows.stream().map(row -> row.authority()).collect(java.util.stream.Collectors.toSet()),
                        rows.stream().map(row -> row.authority()).collect(java.util.stream.Collectors.toSet()));
                assertTrue(rows.stream().allMatch(row -> row.enumerationComplete() == closed
                        && row.disposition() == (closed ? ReferenceDispositionV1.ABSENT : ReferenceDispositionV1.PRESENT)));
            }
        }
    }

    private static void verifyRecoveryFacts(NereusKafkaOwnedProviderRuntime.Opened opened,
            NereusKafkaReadScopeV2.ReplacementEvidence evidence) {
        assertEquals(opened.storage().nativeInputSources(), evidence.recovery());
        assertEquals(evidence.recovery(), evidence.selected().recoveryInputs());
        var checkpoint = evidence.recovery().recoveryCheckpoint().orElseThrow();
        var carriers = new java.util.HashSet<com.nereusstream.storage.api.bookkeeper.StorageRunId>();
        carriers.add(evidence.recovery().sources().activeRoot().runId());
        evidence.recovery().sources().sealedRoots().forEach(root -> carriers.add(root.runId()));
        evidence.selected().descriptor().sealedParts().forEach(part -> carriers.add(part.handle().runId()));
        assertTrue(carriers.contains(checkpoint.vector().runBinding().runId()));
        assertTrue(checkpoint.readIndexes().stream().allMatch(index -> carriers.contains(index.runId())));
        for (var source : evidence.sources()) {
            var expected = new java.util.HashSet<com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1>();
            var selector = evidence.selector().stored();
            expected.add(new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1(
                    selector.key(), selector.metadataVersion(), selector.canonicalStoredSha256()));
            source.transfers().stream().filter(transfer -> transfer.aspect()
                    == com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.SemanticAspect.RECOVERY)
                    .forEach(transfer -> expected.add(transfer.verifierAuthority()));
            expected.add(evidence.readers().owner().authority());
            evidence.readers().closedOwner().ifPresent(owner -> expected.add(owner.authority()));
            evidence.readers().records().forEach(value -> expected.add(new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1(
                    value.key(), value.metadataVersion(), value.canonicalStoredSha256())));
            var rows = evidence.references().get(source.resource()).stream()
                    .filter(row -> row.kind() == ReferenceKindV1.RECOVERY_CHECKPOINT_OR_SNAPSHOT).toList();
            assertEquals(expected, rows.stream().map(row -> row.authority()).collect(java.util.stream.Collectors.toSet()));
            assertTrue(rows.stream().allMatch(row -> row.enumerationComplete() == evidence.readers().closed()
                    && row.disposition() == (evidence.readers().closed() ? ReferenceDispositionV1.ABSENT : ReferenceDispositionV1.PRESENT)));
        }
    }

    private static void verifyReaderFacts(AsyncOxiaClient actual, String root,
            NereusKafkaReadScopeV2.ReplacementEvidence evidence) throws Exception {
        var readers = evidence.readers();
        assertTrue(readers.closed(), "actual live/cold reader scope was not captured");
        assertEquals(2, readers.scopes().size());
        var owner = readers.owner();
        verifyOwnerFact(actual, owner.authority(), owner.admission().owner(), false, owner.admission().scopeSha256());
        var authorities = new java.util.HashSet<com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1>();
        authorities.add(owner.authority());
        var selector = evidence.selector().stored();
        authorities.add(new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1(
                selector.key(), selector.metadataVersion(), selector.canonicalStoredSha256()));
        for (var entry : readers.scopes().entrySet()) {
            var stored = actual.get(root + "/" + entry.getKey()).get(30, TimeUnit.SECONDS);
            assertEquals(stored == null, entry.getValue().isEmpty(), "reader observation changed a missing historical scope");
            if (stored == null) continue;
            var fact = entry.getValue().orElseThrow();
            assertEquals(CanonicalBytes.copyOf(stored.value()), fact.canonicalStoredBytes());
            byte[] nativeKey = (root + "/" + entry.getKey()).getBytes(StandardCharsets.UTF_8);
            var version = CanonicalBytes.copyOf(ByteBuffer.allocate(4 + nativeKey.length + 8)
                    .putInt(nativeKey.length).put(nativeKey).putLong(stored.version().versionId()).array());
            assertEquals(Sha256Digest.hash(version).bytes(), fact.metadataVersion().value());
            authorities.add(new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1(
                    fact.key(), fact.metadataVersion(), fact.canonicalStoredSha256()));
            if (entry.getKey().contains("/native-read-scope/")) {
                var scope = KafkaBookKeeperNativeReadScopeV2.decode(fact.canonicalStoredBytes());
                scope.requireDescriptor(evidence.selected().descriptor(), predecessor(actual, root, evidence.selected().descriptor()));
                assertEquals(owner.admission().owner(), scope.owner());
                assertEquals(scope.retiredLedgerIds(), evidence.sources().stream()
                        .map(source -> ((com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger) source.resource()).ledgerId())
                        .sorted().toList());
            } else {
                var scope = KafkaBookKeeperNativeRecoveryScopeV2.decode(fact.canonicalStoredBytes());
                scope.requireDescriptor(evidence.selected().descriptor(), predecessor(actual, root, evidence.selected().descriptor()));
                assertEquals(owner.admission().owner(), scope.owner());
                assertEquals(evidence.selector().selector().readAdmissionEpoch(), scope.readEpoch());
                assertEquals(readers.closedOwner().orElseThrow().authority().key(), scope.closedOwnerFact().key());
                assertEquals(readers.closedOwner().orElseThrow().authority().valueSha256(), scope.closedOwnerFact().valueSha256());
            }
        }
        if (readers.closedOwner().isPresent()) {
            var archive = readers.closedOwner().orElseThrow();
            verifyOwnerFact(actual, archive.authority(), archive.admission().owner(), true, owner.admission().scopeSha256());
            authorities.add(archive.authority());
            assertEquals(Optional.of(new KafkaOwnerAdmissionV1.Previous(archive.admission().owner().ownerEpoch(),
                    archive.authority().valueSha256(), archive.admission().tail())), owner.admission().previous());
        }
        for (var fact : evidence.quiescence()) authorities.add(new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1(
                fact.key(), fact.metadataVersion(), fact.canonicalStoredSha256()));
        for (var rows : evidence.references().values()) {
            var observed = rows.stream().filter(row -> row.kind() == ReferenceKindV1.READ_GENERATION_PIN_OR_OPEN_HANDLE).toList();
            assertEquals(authorities, observed.stream().map(row -> row.authority()).collect(java.util.stream.Collectors.toSet()));
            assertEquals(authorities.size(), observed.size());
            assertTrue(observed.stream().allMatch(row -> row.disposition() == ReferenceDispositionV1.ABSENT && row.enumerationComplete()));
        }
    }

    static void verifyTaskFacts(AsyncOxiaClient actual, String root, NereusKafkaReadScopeV2.ReplacementEvidence evidence) throws Exception {
        var facts = evidence.tasks();
        var persisted = actual.get(root + "/" + facts.admission().key()).get(30, TimeUnit.SECONDS);
        assertEquals(CanonicalBytes.copyOf(persisted.value()), facts.admission().canonicalStoredBytes());
        assertEquals(MetadataVersionMapper.fromOxia(persisted.version().versionId()), facts.admission().metadataVersion());
        var state = KafkaCompactionAdmissionV1.decode(facts.admission().canonicalStoredBytes());
        assertEquals(state.reservations(), facts.reservations().stream().map(NereusKafkaReadScopeV2.TaskFact::reservation).toList());
        for (var fact : facts.reservations()) {
            var key = com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.taskKey(fact.reservation().compactionTaskIdSha256());
            var inventory = actual.get(root + "/" + key).get(30, TimeUnit.SECONDS);
            assertEquals(inventory == null, fact.inventory().isEmpty(), "missing Task inventory was treated as a terminal Task");
            if (inventory == null) continue;
            var observed = fact.inventory().orElseThrow();
            assertEquals(key, observed.key());
            assertEquals(CanonicalBytes.copyOf(inventory.value()), observed.canonicalStoredBytes());
            byte[] nativeKey = (root + "/" + key).getBytes(StandardCharsets.UTF_8);
            var version = CanonicalBytes.copyOf(ByteBuffer.allocate(4 + nativeKey.length + 8)
                    .putInt(nativeKey.length).put(nativeKey).putLong(inventory.version().versionId()).array());
            assertEquals(Sha256Digest.hash(version).bytes(), observed.metadataVersion().value());
            var task = com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryCodecV2.decodeTask(observed.canonicalStoredBytes());
            assertEquals(fact.reservation().compactionTaskIdSha256(), task.taskIdSha256());
            var binding = com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.decodeSourceCut(task.sourceCut()).identity().binding();
            String decisionKey = com.nereusstream.storage.object.retention.M5TaskSelectionDecisionV2.key(binding, task.taskIdSha256());
            var decision = actual.get(root + "/" + decisionKey).get(30, TimeUnit.SECONDS);
            assertEquals(decision == null, fact.decision().isEmpty());
            if (decision != null) {
                assertEquals(CanonicalBytes.copyOf(decision.value()), fact.decision().orElseThrow().canonicalStoredBytes());
                String selector = new com.nereusstream.storage.object.read.control.M4ReadControlKeysV1(partition(evidence), binding).selector();
                var routeDigest = Sha256Digest.hash(CanonicalBytes.copyOf((root + "\0" + selector + "\0"
                        + binding.incarnationSha256().toHex() + binding.storageEpochSha256().toHex()).getBytes(StandardCharsets.US_ASCII)));
                assertEquals(CanonicalBytes.copyOf(ByteBuffer.allocate(44).putInt(0x4d354f32).put(routeDigest.bytes().toByteArray())
                        .putLong(decision.version().versionId()).array()), fact.decision().orElseThrow().metadataVersion().value());
            }
            var terminal = actual.get(root + "/" + com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperTaskTerminalV2.key(task.taskIdSha256()))
                    .get(30, TimeUnit.SECONDS);
            assertEquals(terminal == null, fact.terminal().isEmpty());
            for (var value : fact.records())
                assertEquals(CanonicalBytes.copyOf(actual.get(root + "/" + value.key()).get(30, TimeUnit.SECONDS).value()), value.canonicalStoredBytes());
        }
    }

    private static int partition(NereusKafkaReadScopeV2.ReplacementEvidence evidence) {
        String key = evidence.selector().stored().key();
        int start = key.indexOf("/shards/") + "/shards/".length();
        return Integer.parseInt(key.substring(start, start + 10));
    }

    private static void verifyTaskReferences(NereusKafkaReadScopeV2.ReplacementEvidence evidence) {
        boolean closed = evidence.tasks().reservations().stream().allMatch(NereusKafkaReadScopeV2.TaskFact::closed);
        var authorities = new ArrayList<com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue>();
        authorities.add(evidence.tasks().admission());
        evidence.tasks().reservations().forEach(task -> authorities.addAll(task.records()));
        for (var rows : evidence.references().values()) {
            var tasks = rows.stream().filter(row -> row.kind() == ReferenceKindV1.MATERIALIZATION_OR_COMPACTION_TASK).toList();
            assertEquals(authorities.size(), tasks.size());
            for (int i = 0; i < tasks.size(); i++) {
                assertEquals(authorities.get(i).key(), tasks.get(i).authority().key());
                assertEquals(authorities.get(i).metadataVersion(), tasks.get(i).authority().metadataVersion());
                assertEquals(authorities.get(i).canonicalStoredSha256(), tasks.get(i).authority().valueSha256());
                assertEquals(closed ? ReferenceDispositionV1.ABSENT : ReferenceDispositionV1.PRESENT, tasks.get(i).disposition());
                assertEquals(closed, tasks.get(i).enumerationComplete());
            }
        }
    }

    static void verifyNativeTaskFacts(NereusKafkaReadScopeV2.ReplacementEvidence evidence, Optional<Sha256Digest> missingTask) throws Exception {
        if (evidence.tasks().reservations().stream().noneMatch(task -> task.nativeScope().isPresent())) return;
        var uri = System.getProperty("nereus.nsip.bkUri");
        var configuration = com.nereusstream.storage.bookkeeper.RealBookKeeperClientConfigurationV1.from(uri,
                evidence.selected().descriptor().task().capability());
        try (var client = (org.apache.bookkeeper.client.BookKeeper) org.apache.bookkeeper.client.api.BookKeeper.newBuilder(configuration).build()) {
            var zk = ((org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver) client.getMetadataClientDriver()).getZk();
            for (var task : evidence.tasks().reservations()) {
                if (task.nativeScope().isEmpty()) continue;
                var nativeScope = task.nativeScope().orElseThrow();
                for (var fact : List.of(nativeScope.instanceIdentity(), nativeScope.taskFence())) {
                    var stat = zk.exists(fact.nativePath(), false);
                    var bytes = zk.getData(fact.nativePath(), false, stat);
                    assertEquals(0, stat.getEphemeralOwner());
                    assertEquals(fact.nativeVersion(), stat.getVersion());
                    assertEquals(fact.rawStoredBytes(), CanonicalBytes.copyOf(bytes));
                }
                assertTrue(nativeScope.fenced());
                assertEquals(1, nativeScope.taskFence().rawStoredBytes().toByteArray()[0]);
                assertTrue(task.closed());
            }
            if (missingTask.isPresent()) {
                String hash = missingTask.orElseThrow().toHex();
                String path = java.net.URI.create(uri).getPath() + "/nereus-m5-native-v2/tasks/" + hash.substring(0, 2) + "/" + hash;
                assertEquals(null, zk.exists(path, false), "observation recreated an undispatched Task's native scope");
            }
        }
    }

    private static void verifyMemberFacts(AsyncOxiaClient actual, NereusKafkaReadScopeV2.ReplacementEvidence evidence) throws Exception {
        assertEquals(evidence.physical().keySet(), evidence.members().keySet());
        var descriptor = evidence.selected().descriptor();
        var expected = new com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Member(
                com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain.KAFKA_OFFSET,
                descriptor.task().capability().providerScopeId().digest(), descriptor.sourceCut().identity().binding());
        for (var entry : evidence.members().entrySet()) {
            var fact = entry.getValue();
            var stored = actual.get(fact.authority().key()).get(30, TimeUnit.SECONDS);
            assertEquals(CanonicalBytes.copyOf(stored.value()), fact.canonicalStoredBytes());
            assertEquals(MetadataVersionMapper.fromOxia(stored.version().versionId()), fact.authority().metadataVersion());
            assertEquals(Sha256Digest.hash(CanonicalBytes.copyOf(stored.value())), fact.authority().valueSha256());
            var inventory = com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Inventory.decode(CanonicalBytes.copyOf(stored.value()));
            assertEquals(fact.inventory(), inventory);
            assertEquals(entry.getKey().sha256(), inventory.resource());
            assertEquals(descriptor.sourceCut().identity().protocolCellSha256(), inventory.protocolCell());
            assertEquals(List.of(expected), inventory.members());
            assertEquals(evidence.physical().get(entry.getKey()).binding().authorityRoot() + "/"
                    + com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.PREFIX
                    + entry.getKey().sha256().toHex() + "/inventory-v2", fact.authority().key());
        }
    }

    private static void verifyPhysicalFacts(AsyncOxiaClient actual, NereusKafkaReadScopeV2.ReplacementEvidence evidence) throws Exception {
        for (var fact : evidence.physical().values()) {
            assertEquals(fact.resource().namespace(), fact.binding().physicalNamespace());
            var root = fact.binding().authorityRoot();
            var authority = actual.get(root + "/" + fact.authority().key()).get(30, TimeUnit.SECONDS);
            assertEquals(CanonicalBytes.copyOf(authority.value()), fact.authority().canonicalStoredBytes());
            var decoded = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(fact.authority().canonicalStoredBytes());
            assertEquals(fact.resource(), decoded.target().resourceId());
            assertEquals(com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1.OPEN_V1, decoded.state());
            assertTrue(decoded.activeWriterTickets().isEmpty());
            var reservation = actual.get(root + "/" + fact.reservation().key()).get(30, TimeUnit.SECONDS);
            assertEquals(CanonicalBytes.copyOf(reservation.value()), fact.reservation().canonicalStoredBytes());
            assertEquals(MetadataVersionMapper.fromOxia(reservation.version().versionId()), fact.reservation().metadataVersion());
            var grant = com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Entry.decode(fact.reservation().canonicalStoredBytes());
            assertEquals(fact.resource().sha256(), grant.resource());
            assertFalse(grant.settled());
        }
    }

    private static void verifySealedFacts(AsyncOxiaClient actual, NereusKafkaReadScopeV2.ReplacementEvidence evidence) throws Exception {
        assertEquals(evidence.physical().keySet(), evidence.sealed().keySet());
        var configuration = com.nereusstream.storage.bookkeeper.RealBookKeeperClientConfigurationV1.from(
                System.getProperty("nereus.nsip.bkUri"), evidence.selected().descriptor().task().capability());
        try (var client = (org.apache.bookkeeper.client.BookKeeper) org.apache.bookkeeper.client.api.BookKeeper.newBuilder(configuration).build()) {
            var zk = ((org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver) client.getMetadataClientDriver()).getZk();
            for (var entry : evidence.sealed().entrySet()) {
                var scope = entry.getValue().live().orElseThrow();
                assertEquals(evidence.physical().get(entry.getKey()).binding(), scope.namespaceBinding());
                assertEquals(((com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger) entry.getKey()).ledgerId(), scope.handle().ledgerIdentity().ledgerId());
                assertEquals(scope.handle(), scope.closedMetadata().handle());
                for (var fact : List.of(scope.instanceIdentity(), scope.taskFence(), scope.reservation(), scope.ledgerMetadata())) {
                    var stat = new org.apache.zookeeper.data.Stat();
                    assertEquals(fact.rawStoredBytes(), CanonicalBytes.copyOf(zk.getData(fact.nativePath(), false, stat)));
                    assertEquals(0, stat.getEphemeralOwner());
                    assertEquals(fact.nativeVersion(), stat.getVersion());
                    assertEquals(fact.nativeCreateZxid(), stat.getCzxid());
                    assertEquals(fact.nativeModifyZxid(), stat.getMzxid());
                    assertEquals(fact.nativeCreatedAtMillis(), stat.getCtime());
                }
                var metadata = client.getLedgerMetadata(scope.handle().ledgerIdentity().ledgerId()).get(30, TimeUnit.SECONDS);
                assertTrue(metadata.isClosed());
                assertEquals(metadata.getLastEntryId(), scope.closedMetadata().sealedLastEntryId());
                assertEquals(metadata.getLength(), scope.closedMetadata().sealedLength());
                assertEquals(metadata.getCToken(), scope.closedMetadata().metadataCToken());
                assertEquals(metadata.getMetadataFormatVersion(), scope.closedMetadata().metadataFormatVersion());
                assertEquals(metadata.getEnsembleSize(), scope.closedMetadata().ensembleSize());
                assertEquals(metadata.getWriteQuorumSize(), scope.closedMetadata().writeQuorumSize());
                assertEquals(metadata.getAckQuorumSize(), scope.closedMetadata().ackQuorumSize());
                assertEquals(scope.createScope().sha256().bytes(), CanonicalBytes.copyOf(java.util.Arrays.copyOf(scope.reservation().rawStoredBytes().toByteArray(), 32)));
                assertEquals(scope.handle().ledgerIdentity().ledgerId(), java.nio.ByteBuffer.wrap(scope.reservation().rawStoredBytes().toByteArray(), 32, 8).getLong());
                if (entry.getValue().rawRoot().isPresent()) {
                    var raw = entry.getValue().rawRoot().orElseThrow();
                    var stored = actual.get(raw.authority().key()).get(30, TimeUnit.SECONDS);
                    assertEquals(raw.record().encode(), CanonicalBytes.copyOf(stored.value()));
                    assertEquals(MetadataVersionMapper.fromOxia(stored.version().versionId()), raw.authority().metadataVersion());
                    assertEquals(entry.getKey(), raw.record().resource());
                    assertEquals(raw.record().root().runId(), scope.handle().runId());
                    var admitted = actual.get(raw.owner().authority().key()).get(30, TimeUnit.SECONDS);
                    assertEquals(raw.owner().admission().encode(), CanonicalBytes.copyOf(admitted.value()));
                    assertEquals(MetadataVersionMapper.fromOxia(admitted.version().versionId()), raw.owner().authority().metadataVersion());
                    assertEquals(raw.record().root().creatorOwnerEpoch(), raw.owner().admission().owner().ownerEpoch());
                    assertTrue(raw.owner().admission().runs().contains(raw.record().initialLink()));
                }
            }
        }
    }

    private static void verifyQuiescenceFacts(AsyncOxiaClient actual, String root, NereusKafkaReadScopeV2.ReplacementEvidence evidence) throws Exception {
        var selector = evidence.selector();
        int start = selector.stored().key().indexOf("/shards/") + "/shards/".length();
        int partition = Integer.parseInt(selector.stored().key().substring(start, start + 10));
        var route = new com.nereusstream.metadata.oxia.v2.retention.OxiaBindingLifecycleMetadataStoreV2(actual, root, partition, selector.selector().binding());
        var facts = new java.util.TreeMap<String, com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue>();
        for (var fact : evidence.quiescence()) {
            assertEquals(Optional.of(fact), route.read(fact.key()).toCompletableFuture().get(30, TimeUnit.SECONDS), "actual quiescence fact revision differs");
            assertEquals(null, facts.put(fact.key(), fact), "quiescence capture contains duplicate authority rows");
        }
        assertFalse(facts.isEmpty());
        assertEquals(List.copyOf(facts.keySet()), evidence.quiescence().stream().map(fact -> fact.key()).toList());
        String prefix = selector.stored().key().substring(0, selector.stored().key().lastIndexOf('/'));
        var head = M4ReadControlCodecV1.decodeHead(facts.get(prefix + "/proof-head").canonicalStoredBytes());
        var epochs = new java.util.TreeSet<Long>();
        for (var release : evidence.releases()) {
            assertEquals(release.canonicalProtectionBytes(), facts.get(release.protectionAuthority().key()).canonicalStoredBytes());
            var protection = M4ReadControlCodecV1.decodeProtection(release.canonicalProtectionBytes());
            var batch = selector.selector().activeBatches().stream().filter(value -> value.batchIdSha256().equals(release.releasedByBatchSha256())).findFirst().orElseThrow();
            long first = protection.identity().firstFallbackCapableReadAdmissionEpoch();
            long last = batch.sharedLastFallbackCapableReadAdmissionEpoch();
            for (long offset = 0; offset <= last - first; offset++) epochs.add(first + offset);
            for (var fold : head.folds()) {
                if (fold.lastEpoch() >= first && fold.firstEpoch() <= last)
                    for (long offset = 0; offset <= fold.lastEpoch() - fold.firstEpoch(); offset++) epochs.add(fold.firstEpoch() + offset);
            }
        }
        assertEquals(epochs.size(), facts.keySet().stream().filter(key -> key.contains("/proofs/")).count());
        for (long epoch : epochs) {
            String ordinal = String.format(java.util.Locale.ROOT, "%020d", epoch);
            var proof = M4ReadControlCodecV1.decodeProof(facts.get(prefix + "/proofs/" + ordinal).canonicalStoredBytes());
            var terminal = facts.get(prefix + "/terminals/" + ordinal);
            assertEquals(epoch, proof.readAdmissionEpoch());
            assertEquals(terminal.canonicalStoredSha256(), proof.terminalCutSha256());
            var capabilityFact = facts.get(prefix + "/capabilities/" + String.format(java.util.Locale.ROOT, "%020d", proof.capability().generation()));
            var capability = M4ReadControlCodecV1.decodeCapability(capabilityFact.canonicalStoredBytes());
            assertEquals(proof.capability().generation(), capability.generation());
            assertEquals(proof.capability().evidenceSha256(), M4ReadControlCodecV1.capabilityEvidenceSha256(capability));
            assertEquals(com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityState.ADMITTED, capability.state());
            assertEquals(selector.selector().binding(), capability.binding());
        }
    }

    /** Observes real reads before version refresh or response loss; no synthetic qualification is supplied. */
    static final class ReplacementFactFault {
        private final boolean active;
        private final boolean proofUnknown;
        private final boolean proofRefresh;
        private final boolean recoveryRefresh;
        private final boolean physicalRefresh;
        private final boolean physicalUnknown;
        private final boolean quotaRefresh;
        private final boolean taskDirectoryRefresh;
        private final boolean taskDirectoryUnknown;
        private final boolean taskInventoryUnknown;
        private final boolean taskDecisionRefresh;
        private final boolean taskDecisionUnknown;
        private final boolean taskTerminalUnknown;
        private final boolean readerScopeRefresh;
        private final boolean readerScopeUnknown;
        private final boolean readerColdScopeRefresh;
        private final boolean readerColdScopeUnknown;
        private final boolean readerScopeMissing;
        private final boolean readerOwnerRefresh;
        private final boolean readerOwnerUnknown;
        private final AtomicBoolean armed = new AtomicBoolean();
        private final java.util.concurrent.atomic.AtomicInteger factReads = new java.util.concurrent.atomic.AtomicInteger();
        private final CompletableFuture<Void> entered = new CompletableFuture<>();
        private final CompletableFuture<Void> released = new CompletableFuture<>();
        private volatile String comparison;
        private volatile String selector;
        private volatile String proof;
        private volatile String physicalKey;
        private volatile String unpublishedKey;
        private volatile boolean unpublishedUnknown;

        ReplacementFactFault(String mode) {
            proofUnknown = mode.endsWith("REPLACEMENT_PROOF_UNKNOWN");
            proofRefresh = mode.endsWith("REPLACEMENT_PROOF_REFRESH");
            recoveryRefresh = mode.endsWith("REPLACEMENT_RECOVERY_REFRESH");
            physicalRefresh = mode.endsWith("REPLACEMENT_PHYSICAL_REFRESH");
            physicalUnknown = mode.endsWith("REPLACEMENT_PHYSICAL_UNKNOWN");
            quotaRefresh = mode.endsWith("REPLACEMENT_QUOTA_REFRESH");
            taskDirectoryRefresh = mode.endsWith("REPLACEMENT_TASK_DIRECTORY_REFRESH");
            taskDirectoryUnknown = mode.endsWith("REPLACEMENT_TASK_DIRECTORY_UNKNOWN");
            taskInventoryUnknown = mode.endsWith("REPLACEMENT_TASK_INVENTORY_UNKNOWN");
            taskDecisionRefresh = mode.endsWith("REPLACEMENT_TASK_DECISION_REFRESH");
            taskDecisionUnknown = mode.endsWith("REPLACEMENT_TASK_DECISION_UNKNOWN");
            taskTerminalUnknown = mode.endsWith("REPLACEMENT_TASK_TERMINAL_UNKNOWN");
            readerScopeRefresh = mode.endsWith("REPLACEMENT_READER_SCOPE_REFRESH");
            readerScopeUnknown = mode.endsWith("REPLACEMENT_READER_SCOPE_UNKNOWN");
            readerColdScopeRefresh = mode.endsWith("COLD_REPLACEMENT_READER_SCOPE_REFRESH");
            readerColdScopeUnknown = mode.endsWith("COLD_REPLACEMENT_READER_SCOPE_UNKNOWN");
            readerScopeMissing = mode.endsWith("REPLACEMENT_READER_SCOPE_MISSING");
            readerOwnerRefresh = mode.endsWith("COLD_REPLACEMENT_READER_OWNER_REFRESH");
            readerOwnerUnknown = mode.endsWith("COLD_REPLACEMENT_READER_OWNER_UNKNOWN");
            active = !modeIsBindingRefresh() || readerScopeMissing || mode.endsWith("REPLACEMENT_FACTS_REFRESH")
                    || mode.equals("TASK_OUTPUT") || mode.equals("TASK_OUTPUT_READ");
        }

        private boolean stopsFactRead(String key) {
            if (proofUnknown || proofRefresh || recoveryRefresh) return key.contains("/proofs/");
            if (taskFactRead()) return stopsTaskFactRead(key);
            if (readerOwnerRefresh || readerOwnerUnknown) return key.contains("/closed-owners-v1/");
            if (readerColdScopeRefresh || readerColdScopeUnknown) return key.contains("/native-recovery-scope/");
            if (readerScopeRefresh || readerScopeUnknown) return key.contains("/native-read-scope/");
            return key.equals(physicalKey);
        }

        private boolean taskFactRead() {
            if (taskDirectoryRefresh || taskDirectoryUnknown || taskInventoryUnknown) return true;
            return taskDecisionRefresh || taskDecisionUnknown || taskTerminalUnknown;
        }

        private boolean stopsTaskFactRead(String key) {
            if (taskDirectoryRefresh || taskDirectoryUnknown) return key.endsWith("/read-m5/kafka-compaction-admission");
            if (taskInventoryUnknown) return key.contains("/kafka-bk-compaction-v2/") && key.endsWith("/task");
            if (taskDecisionRefresh || taskDecisionUnknown) return key.contains("/m5-task-select/") && key.contains("/decisions/");
            return key.contains("/kafka-bk-compaction-v2/") && key.endsWith("/task/terminal");
        }

        private boolean takesFactRead(String key) {
            if (!armed.get() || !stopsFactRead(key)) return false;
            int ordinal = physicalRefresh || physicalUnknown || quotaRefresh ? 2 : 1;
            return factReads.incrementAndGet() == ordinal && armed.compareAndSet(true, false);
        }

        AsyncOxiaClient client(AsyncOxiaClient delegate) {
            if (!active) return delegate;
            return (AsyncOxiaClient) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] {AsyncOxiaClient.class}, (proxy, method, args) -> {
                        String key = args != null && args.length > 0 && args[0] instanceof String value ? value : "";
                        if (key.contains("/read-m4/") && key.endsWith("/selector")) selector = key;
                        final Object response;
                        try {
                            response = method.invoke(delegate, args);
                        } catch (java.lang.reflect.InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                        if (method.getName().equals("get"))
                            return factResponse(key, (CompletionStage<?>) response).toCompletableFuture();
                        return response;
                    });
        }

        private CompletionStage<?> factResponse(String key, CompletionStage<?> response) {
            if (unpublishedKey != null && key.endsWith("/" + unpublishedKey) && armed.compareAndSet(true, false))
                return unpublishedResponse(key, response);
            if (takesFactRead(key))
                return response.thenCompose(value -> {
                    proof = key;
                    entered.complete(null);
                    if (unknownFact())
                        return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("actual replacement fact response lost"));
                    return released.thenApply(ignored -> value);
                });
            if (key.endsWith("/" + comparison) && !released.isDone())
                return response.thenCompose(value -> {
                    entered.complete(null);
                    return released.thenApply(ignored -> value);
                });
            return response;
        }

        private CompletionStage<?> unpublishedResponse(String key, CompletionStage<?> actual) {
            return actual.thenCompose(value -> {
                proof = key;
                entered.complete(null);
                if (unpublishedUnknown)
                    return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("actual unpublished terminal response lost"));
                return released.thenApply(ignored -> value);
            });
        }

        void verifyUnpublished(AsyncOxiaClient actual, NereusKafkaOwnedProviderRuntime.Opened opened,
                NereusKafkaUnpublishedScopeV2.Evidence before, boolean unknown) throws Exception {
            unpublishedKey = com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperTaskTerminalV2.key(before.terminal().task().taskIdSha256());
            unpublishedUnknown = unknown;
            armed.set(true);
            var pending = opened.captureNativeM5UnpublishedEvidence(before.terminal().task().taskIdSha256()).toCompletableFuture();
            try {
                CompletableFuture.anyOf(entered, pending).get(30, TimeUnit.SECONDS);
                assertTrue(entered.isDone(), "unpublished capture did not reach the actual terminal read");
                if (!unknown) refreshFact(actual, proof);
                release();
                var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> pending.get(30, TimeUnit.SECONDS));
                if (unknown) {
                    Throwable cause = failure;
                    while (cause.getCause() != null) cause = cause.getCause();
                    assertInstanceOf(java.util.concurrent.TimeoutException.class, cause);
                    assertEquals("actual unpublished terminal response lost", cause.getMessage());
                    org.junit.jupiter.api.Assertions.assertNotNull(actual.get(proof).get(30, TimeUnit.SECONDS));
                } else {
                    assertInstanceOf(IllegalStateException.class, failure.getCause());
                    assertEquals("native BK record differs from its exact logical observation", failure.getCause().getMessage());
                }
                var retried = opened.captureNativeM5UnpublishedEvidence(before.terminal().task().taskIdSha256()).toCompletableFuture().get(30, TimeUnit.SECONDS);
                if (!unknown) {
                    assertEquals(before.nativeTerminal().key(), retried.nativeTerminal().key());
                    assertEquals(before.nativeTerminal().canonicalStoredBytes(), retried.nativeTerminal().canonicalStoredBytes());
                    assertNotEquals(before.nativeTerminal().metadataVersion(), retried.nativeTerminal().metadataVersion());
                    var independent = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(actual)
                            .read(retried.nativeTerminal().key()).toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow();
                    assertEquals(independent, retried.nativeTerminal());
                }
                assertEquals(before.terminal(), retried.terminal());
                assertEquals(before.physical(), retried.physical(), "rejected observation changed physical writer admission or quota");
                assertEquals(before.members(), retried.members());
            } finally {
                release();
            }
        }

        void verify(AsyncOxiaClient actual, KafkaSealedBookKeeperDescriptorV2 descriptor,
                NereusKafkaOwnedProviderRuntime.Opened opened) throws Exception {
            verify(actual, descriptor, opened, () -> {
                throw new AssertionError("actual recovery mutation was not supplied");
            });
        }

        void verify(AsyncOxiaClient actual, KafkaSealedBookKeeperDescriptorV2 descriptor,
                NereusKafkaOwnedProviderRuntime.Opened opened, java.util.concurrent.Callable<Void> mutation) throws Exception {
            if (!active || descriptor.sourceCut().sources().get(0).kind()
                    != com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind.KAFKA_BK_COMPACTED_GENERATION_V2)
                return;
            comparison = com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperReplacementSemanticsV2.key(descriptor.descriptorSha256());
            if (!modeIsBindingRefresh()) comparison = null;
            var admission = opened.nativeM5CompactionAdmission();
            var before = admission.read().orElseThrow();
            if (readerScopeMissing) {
                verifyMissingScope(actual, opened, before);
                return;
            }
            if (physicalRefresh || physicalUnknown || quotaRefresh) {
                var physical = opened.captureNativeM5ReplacementEvidence().toCompletableFuture().get(30, TimeUnit.SECONDS)
                        .physical().values().iterator().next();
                physicalKey = physical.binding().authorityRoot() + "/" + (quotaRefresh ? physical.reservation().key() : physical.authority().key());
            }
            armed.set(!modeIsBindingRefresh());
            var capture = opened.captureNativeM5ReplacementEvidence().toCompletableFuture();
            try {
                CompletableFuture.anyOf(entered, capture).get(30, TimeUnit.SECONDS);
                assertTrue(entered.isDone(), "actual replacement fact read was not withheld");
                if (unknownFact()) {
                    verifyUnknownFact(actual, capture, before, admission);
                    return;
                }
                assertFalse(capture.isDone(), "replacement capture ignored its pending actual metadata read");
                if (recoveryRefresh) mutation.call();
                else refreshFact(actual, modeIsBindingRefresh() ? selector : proof);
                release();
                var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> capture.get(30, TimeUnit.SECONDS));
                String expected = expectedRefreshRejection();
                assertTrue(failure.getCause().toString().contains(expected), failure.toString());
                assertEquals(before, admission.read().orElseThrow(), "rejected replacement facts released selected cache capacity");
            } finally {
                release();
            }
        }

        private static void refreshFact(AsyncOxiaClient actual, String key) throws Exception {
            var stored = actual.get(key).get(30, TimeUnit.SECONDS);
            actual.put(key, stored.value(), java.util.Set.of(io.oxia.client.api.options.PutOption.IfVersionIdEquals(stored.version().versionId())))
                    .get(30, TimeUnit.SECONDS);
            var advanced = actual.get(key).get(30, TimeUnit.SECONDS);
            assertEquals(CanonicalBytes.copyOf(stored.value()), CanonicalBytes.copyOf(advanced.value()));
            assertFalse(stored.version().equals(advanced.version()), "actual same-value CAS did not advance the Binding version");
        }

        private void verifyUnknownFact(AsyncOxiaClient actual, CompletableFuture<?> capture,
                KafkaCompactionAdmissionV1.State before, KafkaCompactionAdmissionV1 admission) throws Exception {
            var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> capture.get(30, TimeUnit.SECONDS));
            Throwable cause = failure;
            while (cause.getCause() != null) cause = cause.getCause();
            assertInstanceOf(java.util.concurrent.TimeoutException.class, cause);
            assertEquals("actual replacement fact response lost", cause.getMessage());
            var independentlyRead = actual.get(proof).get(30, TimeUnit.SECONDS);
            if (taskTerminalUnknown) assertEquals(null, independentlyRead, "selected Task acquired an unpublished cancellation terminal");
            else org.junit.jupiter.api.Assertions.assertNotNull(independentlyRead, "lost proof response did not follow an actual stored proof read");
            assertEquals(before, admission.read().orElseThrow(), "unknown proof read released selected cache capacity");
        }

        private boolean modeIsBindingRefresh() {
            if (taskFactRead() || readerFactRead()) return false;
            return !(proofUnknown || proofRefresh || recoveryRefresh || physicalRefresh || physicalUnknown || quotaRefresh);
        }

        private boolean readerFactRead() {
            return readerScopeRefresh || readerScopeUnknown || readerScopeMissing || readerOwnerRefresh || readerOwnerUnknown;
        }

        private boolean unknownFact() {
            if (proofUnknown || physicalUnknown || taskDirectoryUnknown || taskInventoryUnknown) return true;
            return taskDecisionUnknown || taskTerminalUnknown || readerScopeUnknown || readerOwnerUnknown;
        }

        private String expectedRefreshRejection() {
            if (recoveryRefresh) return "native recovery state changed during fact capture";
            if (proofRefresh) return "native quiescence authority changed during fact capture";
            if (physicalRefresh) return "native physical authority changed during fact capture";
            if (quotaRefresh) return "native physical reservation changed during fact capture";
            if (taskDirectoryRefresh) return "native Task Cell directory changed during fact capture";
            if (taskDecisionRefresh) return "native Task decision changed during fact capture";
            if (readerScopeRefresh) return "native reader scope changed during fact capture";
            if (readerOwnerRefresh) return "native reader closed Owner changed during fact capture";
            return "native replacement Binding authority changed during fact capture";
        }

        private void verifyMissingScope(AsyncOxiaClient actual, NereusKafkaOwnedProviderRuntime.Opened opened,
                KafkaCompactionAdmissionV1.State before) throws Exception {
            var evidence = opened.captureNativeM5ReplacementEvidence().toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertEquals(1, evidence.readers().records().size());
            var fact = evidence.readers().records().get(0);
            String root = selector.substring(0, selector.indexOf("/v2/"));
            String key = root + "/" + fact.key();
            var stored = actual.get(key).get(30, TimeUnit.SECONDS);
            actual.delete(key, java.util.Set.of(io.oxia.client.api.options.DeleteOption.IfVersionIdEquals(stored.version().versionId())))
                    .get(30, TimeUnit.SECONDS);
            try {
                var unresolved = opened.captureNativeM5ReplacementEvidence().toCompletableFuture().get(30, TimeUnit.SECONDS);
                assertFalse(unresolved.readers().closed());
                assertTrue(unresolved.readers().records().isEmpty());
                var rows = unresolved.references().values().stream().flatMap(List::stream)
                        .filter(row -> java.util.EnumSet.of(ReferenceKindV1.READ_GENERATION_PIN_OR_OPEN_HANDLE,
                                ReferenceKindV1.RECOVERY_CHECKPOINT_OR_SNAPSHOT, ReferenceKindV1.KAFKA_PRODUCER_RECOVERY,
                                ReferenceKindV1.KAFKA_TRANSACTION_OR_ABORTED_RECOVERY,
                                ReferenceKindV1.KAFKA_REPLICA_OR_LEADER_EPOCH_RECOVERY).contains(row.kind())).toList();
                assertFalse(rows.isEmpty());
                assertTrue(rows.stream().allMatch(row -> row.disposition() == ReferenceDispositionV1.PRESENT && !row.enumerationComplete()));
                assertEquals(before, opened.nativeM5CompactionAdmission().read().orElseThrow(), "missing reader scope released cache capacity");
                assertEquals(null, actual.get(key).get(30, TimeUnit.SECONDS), "reader capture recreated a missing scope");
            } finally {
                actual.put(key, stored.value(), java.util.Set.of(io.oxia.client.api.options.PutOption.IfRecordDoesNotExist))
                        .get(30, TimeUnit.SECONDS);
            }
        }

        void release() {
            armed.set(false);
            released.complete(null);
        }
    }
}
