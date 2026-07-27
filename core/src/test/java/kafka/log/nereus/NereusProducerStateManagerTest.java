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

import com.nereusstream.kafka.checkpoint.KafkaProducerTransactionState;
import com.nereusstream.kafka.checkpoint.KafkaProducerTransactionStateCodecV1;

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
    private static final TopicPartition TOPIC_PARTITION =
            new TopicPartition("events", 0);

    @TempDir
    Path tempDir;

    @Test
    void checkpointRoundTripRetainsStockFiveBatchWindowAndSequenceWrap()
            throws Exception {
        Fixture source = fixture("source");
        int[] sequences = {
            Integer.MAX_VALUE - 2,
            Integer.MAX_VALUE - 1,
            Integer.MAX_VALUE,
            0,
            1,
            2,
            3
        };
        for (int offset = 0; offset < sequences.length; offset++) {
            source.manager.replayBatch(batch(
                    MemoryRecords.withIdempotentRecords(
                            offset,
                            Compression.NONE,
                            91,
                            (short) 4,
                            sequences[offset],
                            7,
                            new SimpleRecord(
                                    1_000 + offset,
                                    ("v-" + offset).getBytes()))));
        }

        KafkaProducerTransactionState canonical =
                source.manager.freezeCanonical(sequences.length);
        assertEquals(1, canonical.producers().size());
        assertEquals(
                List.of(Integer.MAX_VALUE, 0, 1, 2, 3),
                canonical.producers().get(0).batches().stream()
                        .map(KafkaProducerTransactionState.BatchMetadata::lastSequence)
                        .toList());

        KafkaProducerTransactionStateCodecV1 codec =
                new KafkaProducerTransactionStateCodecV1();
        KafkaProducerTransactionState decoded = codec.decodeSections(
                codec.encodeSections(canonical, sequences.length),
                sequences.length);
        Fixture restored = fixture("restored");
        restored.manager.restoreCanonical(decoded);

        assertEquals(
                canonical,
                restored.manager.exportCanonical(sequences.length));
        assertNoTruthBearingLocalState(source);
        assertNoTruthBearingLocalState(restored);
    }

    @Test
    void overlappingAbortAndOpenTransactionRestoreExactLsoAndIndex()
            throws Exception {
        Fixture source = fixture("transactions");
        source.manager.replayBatch(batch(transactional(0, 11, (short) 1)));
        source.manager.replayBatch(batch(transactional(1, 22, (short) 2)));
        source.manager.replayBatch(batch(marker(
                2,
                11,
                (short) 1,
                ControlRecordType.ABORT)));

        KafkaProducerTransactionState checkpoint =
                source.manager.freezeCanonical(3);
        assertEquals(1, checkpoint.openTransactions().size());
        assertEquals(22, checkpoint.openTransactions().get(0).producerId());
        assertEquals(1, checkpoint.openTransactions().get(0).firstOffset());
        assertEquals(1, checkpoint.abortedTransactions().size());
        KafkaProducerTransactionState.AbortedTransaction aborted =
                checkpoint.abortedTransactions().get(0);
        assertEquals(11, aborted.producerId());
        assertEquals(0, aborted.firstOffset());
        assertEquals(2, aborted.lastOffset());
        assertEquals(1, aborted.lastStableOffset());
        assertEquals(
                1,
                source.manager.firstUnstableOffset()
                        .orElseThrow().messageOffset);
        assertEquals(
                11,
                source.manager.collectAbortedTransactions(0, 3)
                        .get(0).producerId());

        Fixture restored = fixture("transaction-restored");
        restored.manager.restoreCanonical(checkpoint);
        assertEquals(
                checkpoint,
                restored.manager.exportCanonical(3));
        assertEquals(
                1,
                restored.manager.firstUnstableOffset()
                        .orElseThrow().messageOffset);

        restored.manager.replayBatch(batch(marker(
                3,
                22,
                (short) 2,
                ControlRecordType.COMMIT)));
        KafkaProducerTransactionState completed =
                restored.manager.freezeCanonical(4);
        assertTrue(completed.openTransactions().isEmpty());
        assertTrue(restored.manager.firstUnstableOffset().isEmpty());
        assertEquals(1, completed.abortedTransactions().size());
        assertNoTruthBearingLocalState(restored);
    }

    private Fixture fixture(String name) throws IOException {
        Path directory = Files.createDirectories(tempDir.resolve(name));
        Path transactionIndexPath = LogFileUtils.transactionIndexFile(
                directory.toFile(), 0).toPath();
        NereusTransactionIndex transactionIndex =
                new NereusTransactionIndex(
                        0, transactionIndexPath.toFile());
        NereusProducerStateManager manager =
                new NereusProducerStateManager(
                        TOPIC_PARTITION,
                        directory.toFile(),
                        15 * 60 * 1000,
                        new ProducerStateManagerConfig(86_400_000, true),
                        Time.SYSTEM,
                        transactionIndex);
        return new Fixture(directory, transactionIndexPath, manager);
    }

    private static MemoryRecords transactional(
            long offset,
            long producerId,
            short producerEpoch
    ) {
        return MemoryRecords.withTransactionalRecords(
                offset,
                Compression.NONE,
                producerId,
                producerEpoch,
                0,
                7,
                new SimpleRecord(
                        1_000 + offset,
                        ("txn-" + producerId).getBytes()));
    }

    private static MemoryRecords marker(
            long offset,
            long producerId,
            short producerEpoch,
            ControlRecordType type
    ) {
        return MemoryRecords.withEndTransactionMarker(
                offset,
                2_000 + offset,
                7,
                producerId,
                producerEpoch,
                new EndTransactionMarker(type, 9));
    }

    private static RecordBatch batch(MemoryRecords records) {
        return records.batches().iterator().next();
    }

    private static void assertNoTruthBearingLocalState(Fixture fixture)
            throws IOException {
        fixture.manager.takeSnapshot();
        assertFalse(Files.exists(fixture.transactionIndexPath));
        try (var files = Files.list(fixture.directory)) {
            assertTrue(files.noneMatch(path ->
                    path.getFileName().toString().endsWith(".snapshot")));
        }
    }

    private record Fixture(
            Path directory,
            Path transactionIndexPath,
            NereusProducerStateManager manager
    ) { }
}
