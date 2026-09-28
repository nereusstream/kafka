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

package kafka.log.nereus;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.LogFileUtils;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NereusProducerStateManagerTest {
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition("events", 0);

    @TempDir
    Path tempDir;

    @Test
    void sharedCheckpointRetainsStockFiveBatchWindowAndSequenceWrap() throws Exception {
        NereusProducerStateManager source = fixture("source");
        int[] sequences = {Integer.MAX_VALUE - 2, Integer.MAX_VALUE - 1, Integer.MAX_VALUE, 0, 1, 2, 3};
        for (int offset = 0; offset < sequences.length; offset++) {
            source.replayBatch(batch(MemoryRecords.withIdempotentRecords(
                    offset,
                    Compression.NONE,
                    91,
                    (short) 4,
                    sequences[offset],
                    7,
                    new SimpleRecord(1_000 + offset, ("v-" + offset).getBytes()))));
        }

        var batches = source.activeProducers().get(91L).batchMetadata();
        assertEquals(
                List.of(Integer.MAX_VALUE, 0, 1, 2, 3),
                batches.stream().map(value -> value.lastSeq()).toList());
        NereusProducerStateManager restored = fixture("restored");
        restored.loadSharedState(sharedSnapshot(source));
        assertEquals(
                source.activeProducers().get(91L).batchMetadata().stream()
                        .map(value -> value.lastOffset())
                        .toList(),
                restored.activeProducers().get(91L).batchMetadata().stream()
                        .map(value -> value.lastOffset())
                        .toList());
        assertNoTruthBearingLocalState(source, "source");
        assertNoTruthBearingLocalState(restored, "restored");
    }

    @Test
    void overlappingAbortAndOpenTransactionRestoreExactLsoAndIndex() throws Exception {
        NereusProducerStateManager source = fixture("transactions");
        source.replayBatch(batch(transactional(0, 11, (short) 1)));
        source.replayBatch(batch(transactional(1, 22, (short) 2)));
        source.replayBatch(batch(marker(2, 11, (short) 1, ControlRecordType.ABORT)));

        source.onHighWatermarkUpdated(3);
        assertEquals(1, source.collectAbortedTransactions(0, 3).size());
        var aborted = source.collectAbortedTransactions(0, 3).get(0);
        assertEquals(11, aborted.producerId());
        assertEquals(0, aborted.firstOffset());
        assertEquals(2, aborted.lastOffset());
        assertEquals(1, aborted.lastStableOffset());
        assertEquals(1, source.firstUnstableOffset().orElseThrow().messageOffset);
        assertEquals(11, source.collectAbortedTransactions(0, 3).get(0).producerId());

        NereusProducerStateManager restored = fixture("transaction-restored");
        restored.loadSharedState(sharedSnapshot(source));
        assertEquals(source.collectAbortedTransactions(0, 3), restored.collectAbortedTransactions(0, 3));
        assertEquals(
                source.activeProducers().get(11L).lastTimestamp(),
                restored.activeProducers().get(11L).lastTimestamp());
        assertEquals(1, restored.firstUnstableOffset().orElseThrow().messageOffset);

        restored.replayBatch(batch(marker(3, 22, (short) 2, ControlRecordType.COMMIT)));
        restored.onHighWatermarkUpdated(4);
        assertTrue(restored.firstUnstableOffset().isEmpty());
        assertEquals(1, restored.collectAbortedTransactions(0, 4).size());
        assertNoTruthBearingLocalState(restored, "transaction-restored");
    }

    private static com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentProtocolSnapshotV1 sharedSnapshot(
            NereusProducerStateManager nativeState) {
        var producers =
                new java.util.TreeMap<Long, com.nereusstream.kafka.bookkeeper.commit.KafkaProducerSessionStateV1>();
        var ongoing = new java.util.TreeMap<
                Long, com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1.OngoingTransactionV1>();
        var aborted = nativeState.collectAbortedTransactions(0, nativeState.mapEndOffset());
        for (var entry : nativeState.activeProducers().values()) {
            var recent = entry.batchMetadata().stream()
                    .map(batch -> new com.nereusstream.kafka.bookkeeper.commit.KafkaProducerBatchResultV1(
                            new com.nereusstream.kafka.bookkeeper.commit.KafkaBatchDuplicateIdentityV1(
                                    entry.producerId(), entry.producerEpoch(), batch.firstSeq(), batch.lastSeq()),
                            batch.firstOffset(),
                            batch.lastOffset() + 1,
                            batch.timestamp()))
                    .toList();
            long marker = aborted.stream()
                    .filter(txn -> txn.producerId() == entry.producerId())
                    .mapToLong(txn -> txn.lastOffset())
                    .max()
                    .orElse(-1);
            var last = recent.isEmpty() ? null : recent.get(recent.size() - 1);
            producers.put(
                    entry.producerId(),
                    new com.nereusstream.kafka.bookkeeper.commit.KafkaProducerSessionStateV1(
                            entry.producerId(),
                            entry.producerEpoch(),
                            last == null ? -1 : last.identity().lastSequence(),
                            last == null ? -1 : last.endOffsetExclusive() - 1,
                            recent,
                            entry.coordinatorEpoch(),
                            marker,
                            entry.lastTimestamp()));
            entry.currentTxnFirstOffset()
                    .ifPresent(offset -> ongoing.put(
                            entry.producerId(),
                            new com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1.OngoingTransactionV1(
                                    entry.producerId(), offset)));
        }
        var transactions = new com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1(
                ongoing,
                aborted.stream()
                        .map(txn ->
                                new com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1
                                        .CompletedTransactionV1(
                                        txn.producerId(),
                                        txn.firstOffset(),
                                        txn.lastOffset() + 1,
                                        true,
                                        nativeState
                                                .activeProducers()
                                                .get(txn.producerId())
                                                .coordinatorEpoch()))
                        .toList());
        var digest = com.nereusstream.domain.bytes.Sha256Digest.hash(
                com.nereusstream.domain.bytes.CanonicalBytes.copyOf(new byte[] {1}));
        var topic = new com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity(
                new com.nereusstream.domain.identity.KafkaTopicId(new com.nereusstream.domain.identity.Id128(1, 1)),
                new com.nereusstream.domain.protocol.KafkaTopicName("events"));
        var binding = new com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1(
                new com.nereusstream.domain.identity.TopicBindingId(digest),
                topic,
                0,
                new com.nereusstream.domain.identity.StorageEpochId(digest),
                1,
                7,
                new com.nereusstream.storage.api.bookkeeper.CellProviderScopeId(digest),
                new com.nereusstream.storage.api.bookkeeper.StorageRunId(
                        new com.nereusstream.domain.identity.Id128(1, 2)));
        long end = nativeState.mapEndOffset();
        var state = new com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1(
                new com.nereusstream.kafka.bookkeeper.checkpoint.KafkaRecoveryCheckpointVectorV1(
                        binding, end, end, end, end),
                new com.nereusstream.kafka.bookkeeper.commit.KafkaCommittedProducerStateV1(producers),
                transactions,
                com.nereusstream.kafka.bookkeeper.commit.KafkaLeaderEpochIndexV1.empty()
                        .observe(7, 0));
        state = com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1.fromNbke2(state.toNbke2());
        return com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentCommitCoordinatorV1.bootstrapRecovered(
                        new com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1(
                                binding.bindingId(), topic, 0, 1, binding.storageEpochId(), 2, 8),
                        0,
                        end,
                        state,
                        new com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1(
                                binding.providerScopeId(),
                                new com.nereusstream.storage.api.bookkeeper.StorageRunId(
                                        new com.nereusstream.domain.identity.Id128(1, 3)),
                                new com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity(1),
                                digest),
                        ignored -> {
                            // Recovery publication is observed through the returned snapshot.
                        })
                .capture();
    }

    private NereusProducerStateManager fixture(String name) throws IOException {
        Path directory = Files.createDirectories(tempDir.resolve(name));
        Path transactionIndexPath =
                LogFileUtils.transactionIndexFile(directory.toFile(), 0).toPath();
        NereusTransactionIndex transactionIndex = new NereusTransactionIndex(0, transactionIndexPath.toFile());
        NereusProducerStateManager manager = new NereusProducerStateManager(
                TOPIC_PARTITION,
                directory.toFile(),
                15 * 60 * 1000,
                new ProducerStateManagerConfig(86_400_000, true),
                Time.SYSTEM,
                transactionIndex);
        return manager;
    }

    private static MemoryRecords transactional(long offset, long producerId, short producerEpoch) {
        return MemoryRecords.withTransactionalRecords(
                offset,
                Compression.NONE,
                producerId,
                producerEpoch,
                0,
                7,
                new SimpleRecord(1_000 + offset, ("txn-" + producerId).getBytes()));
    }

    private static MemoryRecords marker(long offset, long producerId, short producerEpoch, ControlRecordType type) {
        return MemoryRecords.withEndTransactionMarker(
                offset, 2_000 + offset, 7, producerId, producerEpoch, new EndTransactionMarker(type, 9));
    }

    private static RecordBatch batch(MemoryRecords records) {
        return records.batches().iterator().next();
    }

    private void assertNoTruthBearingLocalState(NereusProducerStateManager manager, String name) throws IOException {
        manager.takeSnapshot();
        Path directory = tempDir.resolve(name);
        assertFalse(Files.exists(
                LogFileUtils.transactionIndexFile(directory.toFile(), 0).toPath()));
        try (var files = Files.list(directory)) {
            assertTrue(files.noneMatch(path -> path.getFileName().toString().endsWith(".snapshot")));
        }
    }
}
