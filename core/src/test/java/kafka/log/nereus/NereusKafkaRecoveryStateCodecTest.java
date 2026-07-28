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
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.LogFileUtils;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;

import com.nereusstream.api.AppendAuthority;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.kafka.checkpoint.KafkaCanonicalCheckpointState;
import com.nereusstream.kafka.checkpoint.KafkaCanonicalCheckpointStateCodecV1;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.kafka.checkpoint.KafkaDerivedIndexState;
import com.nereusstream.kafka.checkpoint.KafkaLeaderEpochState;
import com.nereusstream.kafka.checkpoint.KafkaProducerTransactionState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.recovery.KafkaReplayBatch;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointHeader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NereusKafkaRecoveryStateCodecTest {
    private static final KafkaPartitionIdentity IDENTITY =
            new KafkaPartitionIdentity(
                    "kraft-cluster",
                    "AAAAAAAAAAAAAAAAAAAAAQ",
                    0,
                    "events");

    @TempDir
    Path tempDir;

    @Test
    void rebuildsAndFreezesExactDerivedStateAcrossLeaderEpochs() throws Exception {
        byte[] first = bytes(MemoryRecords.withRecords(
                0,
                Compression.of(CompressionType.GZIP).build(),
                5,
                new SimpleRecord(100, "a".getBytes()),
                new SimpleRecord(300, "b".getBytes())));
        byte[] second = bytes(MemoryRecords.withRecords(
                2,
                Compression.of(CompressionType.NONE).build(),
                7,
                new SimpleRecord(200, "c".getBytes())));
        NereusKafkaRecoveryStateCodec codec =
                codec(7, 0, 3, "epochs");
        NereusKafkaRecoveredState state = codec.freshState();

        codec.replayBatch(state, new KafkaReplayBatch(0, 1, first));
        codec.replayBatch(state, new KafkaReplayBatch(2, 2, second));
        codec.validateRecoveredState(state, source(0, 3, 7));

        assertTrue(state.frozen());
        assertEquals(0, state.logStartOffset());
        assertEquals(3, state.stableEndOffset());
        assertEquals(3, state.lastStableOffset());
        assertEquals(2, state.batchCount());
        assertEquals(3, state.recordCount());
        assertEquals(first.length + second.length, state.logicalBytes());
        assertEquals(300, state.largestTimestamp().orElseThrow());
        assertEquals(1, state.maxTimestampOffset().orElseThrow());
        assertEquals(
                List.of(
                        new NereusKafkaRecoveredState.LeaderEpochRange(5, 0),
                        new NereusKafkaRecoveredState.LeaderEpochRange(7, 2)),
                state.leaderEpochRanges());
        assertThrows(
                IllegalStateException.class,
                () -> codec.replayBatch(state, new KafkaReplayBatch(3, 3, second)));
    }

    @Test
    void acceptsIdempotentReplayAndCanonicalCheckpointHydration() throws Exception {
        byte[] idempotent = bytes(MemoryRecords.withIdempotentRecords(
                0,
                Compression.of(CompressionType.NONE).build(),
                99,
                (short) 1,
                0,
                7,
                new SimpleRecord(100, "a".getBytes())));
        NereusKafkaRecoveryStateCodec codec =
                codec(7, 0, 1, "idempotent");
        NereusKafkaRecoveredState state = codec.freshState();

        codec.replayBatch(state, new KafkaReplayBatch(0, 0, idempotent));
        codec.validateRecoveredState(state, source(0, 1, 7));
        assertEquals(1, state.producerTransactionState().producers().size());
        assertEquals(
                99,
                state.producerTransactionState().producers().get(0).producerId());

        NereusKafkaRecoveryStateCodec checkpointCodec =
                codec(7, 0, 0, "checkpoint");
        NereusKafkaRecoveredState checkpointState = checkpointCodec.freshState();
        KafkaProducerTransactionState empty =
                new KafkaProducerTransactionState(
                        0, List.of(), List.of(), List.of());
        KafkaCanonicalCheckpointState genesis =
                new KafkaCanonicalCheckpointState(
                        0,
                        0,
                        0,
                        empty,
                        new KafkaLeaderEpochState(0, 0, List.of()),
                        new KafkaVirtualSegmentState(0, 0, List.of(), List.of()),
                        new KafkaDerivedIndexState(0, 0, List.of(), List.of()));
        checkpointCodec.hydrateCheckpoint(
                checkpointState,
                header(0, 0),
                new KafkaCanonicalCheckpointStateCodecV1()
                        .encodeSections(genesis));
        checkpointCodec.validateRecoveredState(
                checkpointState, source(0, 0, 7));
        assertEquals(empty, checkpointState.producerTransactionState());
    }

    @Test
    void hydratesAllCanonicalSectionsBeforeReplayingTheCommittedTail()
            throws Exception {
        KafkaVirtualSegmentState.LogConfigHistoryEntry config =
                KafkaVirtualSegmentState.LogConfigHistoryEntry.create(
                        1,
                        0,
                        1_024,
                        60_000,
                        0,
                        1_024,
                        64,
                        -1,
                        -1,
                        0,
                        86_400_000,
                        0,
                        Long.MAX_VALUE,
                        0.5,
                        KafkaVirtualSegmentState.LogConfigHistoryEntry
                                .CLEANUP_DELETE_FLAG);
        KafkaVirtualSegmentState virtual =
                new KafkaVirtualSegmentState(
                        0,
                        1,
                        List.of(new KafkaVirtualSegmentState.VirtualSegment(
                                0,
                                1,
                                0,
                                1_000,
                                0,
                                0,
                                1_000,
                                0,
                                10,
                                0,
                                10,
                                config.configDigest(),
                                KafkaVirtualSegmentState.RollReason.INITIAL,
                                KafkaVirtualSegmentState.SegmentState.ACTIVE)),
                        List.of(config));
        KafkaCanonicalCheckpointState checkpoint =
                new KafkaCanonicalCheckpointState(
                        1,
                        0,
                        1,
                        new KafkaProducerTransactionState(
                                1, List.of(), List.of(), List.of()),
                        new KafkaLeaderEpochState(
                                0,
                                1,
                                List.of(new KafkaLeaderEpochState
                                        .LeaderEpochRange(5, 0))),
                        virtual,
                        new KafkaDerivedIndexState(
                                0,
                                1,
                                List.of(new KafkaDerivedIndexState
                                        .SegmentTimeIndex(
                                                0,
                                                List.of(new KafkaDerivedIndexState
                                                        .TimeIndexEntry(
                                                                1_000,
                                                                0)))),
                                List.of(new KafkaDerivedIndexState
                                        .SegmentLogicalByteIndex(
                                                0,
                                                10,
                                                List.of()))));
        byte[] tail = bytes(MemoryRecords.withIdempotentRecords(
                1,
                Compression.of(CompressionType.NONE).build(),
                99,
                (short) 1,
                0,
                7,
                new SimpleRecord(2_000, "tail".getBytes())));
        NereusKafkaRecoveryStateCodec codec =
                codec(7, 0, 2, "checkpoint-tail");
        NereusKafkaRecoveredState state = codec.freshState();

        codec.hydrateCheckpoint(
                state,
                header(0, 1),
                new KafkaCanonicalCheckpointStateCodecV1()
                        .encodeSections(checkpoint));
        codec.replayBatch(state, new KafkaReplayBatch(1, 1, tail));
        codec.validateRecoveredState(state, source(0, 2, 7));

        assertEquals(10 + tail.length, state.logicalBytes());
        assertEquals(1, state.batchCount());
        assertEquals(1, state.recordCount());
        assertEquals(2_000, state.largestTimestamp().orElseThrow());
        assertEquals(1, state.maxTimestampOffset().orElseThrow());
        assertEquals(
                List.of(
                        new NereusKafkaRecoveredState.LeaderEpochRange(5, 0),
                        new NereusKafkaRecoveredState.LeaderEpochRange(7, 1)),
                state.leaderEpochRanges());
        assertEquals(
                99,
                state.producerTransactionState()
                        .producers().get(0).producerId());
    }

    @Test
    void hydratesACheckpointCapturedBeforeTheCurrentDurableTrim() throws Exception {
        KafkaVirtualSegmentState.LogConfigHistoryEntry config =
                KafkaVirtualSegmentState.LogConfigHistoryEntry.create(
                        1,
                        0,
                        1_024,
                        60_000,
                        0,
                        1_024,
                        64,
                        -1,
                        -1,
                        0,
                        86_400_000,
                        0,
                        Long.MAX_VALUE,
                        0.5,
                        KafkaVirtualSegmentState.LogConfigHistoryEntry.CLEANUP_DELETE_FLAG);
        KafkaVirtualSegmentState virtual =
                new KafkaVirtualSegmentState(
                        0,
                        3,
                        List.of(
                                segment(config, 0, 1, 10, KafkaVirtualSegmentState.SegmentState.CLOSED),
                                segment(config, 1, 2, 10, KafkaVirtualSegmentState.SegmentState.CLOSED),
                                segment(config, 2, 3, 10, KafkaVirtualSegmentState.SegmentState.ACTIVE)),
                        List.of(config));
        KafkaDerivedIndexState derived =
                new KafkaDerivedIndexState(
                        0,
                        3,
                        List.of(
                                new KafkaDerivedIndexState.SegmentTimeIndex(0, List.of()),
                                new KafkaDerivedIndexState.SegmentTimeIndex(1, List.of()),
                                new KafkaDerivedIndexState.SegmentTimeIndex(2, List.of())),
                        List.of(
                                new KafkaDerivedIndexState.SegmentLogicalByteIndex(0, 10, List.of()),
                                new KafkaDerivedIndexState.SegmentLogicalByteIndex(1, 10, List.of()),
                                new KafkaDerivedIndexState.SegmentLogicalByteIndex(2, 10, List.of())));
        KafkaCanonicalCheckpointState checkpoint =
                new KafkaCanonicalCheckpointState(
                        3,
                        0,
                        3,
                        new KafkaProducerTransactionState(3, List.of(), List.of(), List.of()),
                        new KafkaLeaderEpochState(
                                0,
                                3,
                                List.of(new KafkaLeaderEpochState.LeaderEpochRange(5, 0))),
                        virtual,
                        derived);
        NereusKafkaRecoveryStateCodec codec =
                codec(7, 2, 3, "pre-trim-checkpoint");
        NereusKafkaRecoveredState state = codec.freshState();

        codec.hydrateCheckpoint(
                state,
                header(0, 3),
                new KafkaCanonicalCheckpointStateCodecV1().encodeSections(checkpoint));
        codec.validateRecoveredState(state, source(2, 3, 7));

        assertEquals(2, state.logStartOffset());
        assertEquals(3, state.stableEndOffset());
        assertEquals(3, state.checkpointState().orElseThrow().stableEndOffset());
    }

    @Test
    void rejectsTrailingBytesAndAFrozenSourceMismatch() throws Exception {
        byte[] exact = bytes(MemoryRecords.withRecords(
                0,
                Compression.of(CompressionType.NONE).build(),
                6,
                new SimpleRecord(100, "a".getBytes())));
        byte[] trailing = java.util.Arrays.copyOf(exact, exact.length + 1);
        trailing[trailing.length - 1] = 1;
        NereusKafkaRecoveryStateCodec trailingCodec =
                codec(7, 0, 1, "trailing");

        NereusException malformed = assertThrows(
                NereusException.class,
                () -> trailingCodec.replayBatch(
                        trailingCodec.freshState(),
                        new KafkaReplayBatch(0, 0, trailing)));

        assertEquals(ErrorCode.METADATA_INVARIANT_VIOLATION, malformed.code());

        NereusKafkaRecoveryStateCodec mismatchCodec =
                codec(7, 0, 1, "mismatch");
        NereusKafkaRecoveredState mismatch = mismatchCodec.freshState();
        mismatchCodec.replayBatch(
                mismatch,
                new KafkaReplayBatch(0, 0, exact));
        NereusException sourceMismatch = assertThrows(
                NereusException.class,
                () -> mismatchCodec.validateRecoveredState(
                        mismatch,
                        source(0, 1, 8)));
        assertEquals(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                sourceMismatch.code());
    }

    private NereusKafkaRecoveryStateCodec codec(
            int leaderEpoch,
            long logStartOffset,
            long stableEndOffset,
            String directory
    ) throws IOException {
        Path path = Files.createDirectories(tempDir.resolve(directory));
        NereusTransactionIndex transactionIndex =
                new NereusTransactionIndex(
                        logStartOffset,
                        LogFileUtils.transactionIndexFile(
                                path.toFile(), logStartOffset));
        NereusProducerStateManager producerStateManager =
                new NereusProducerStateManager(
                        new TopicPartition("events", 0),
                        path.toFile(),
                        15 * 60 * 1000,
                        new ProducerStateManagerConfig(86_400_000, false),
                        Time.SYSTEM,
                        transactionIndex);
        return new NereusKafkaRecoveryStateCodec(
                IDENTITY,
                leaderEpoch,
                logStartOffset,
                stableEndOffset,
                producerStateManager);
    }

    private static KafkaCheckpointSourceState source(
            long trimOffset,
            long endOffset,
            int leaderEpoch
    ) {
        return new KafkaCheckpointSourceState(
                new AppendAuthority(
                        "kafka-partition-leader-v1",
                        IDENTITY.durableId().canonicalIdentity(),
                        leaderEpoch,
                        "broker-1",
                        9),
                "writer-1",
                1,
                "fencing-token",
                1,
                trimOffset,
                endOffset,
                1,
                "commit-1",
                new Checksum(ChecksumType.SHA256, "a".repeat(64)),
                false,
                endOffset);
    }

    private static KafkaCheckpointHeader header(
            long logStartOffset,
            long checkpointOffset
    ) {
        return new KafkaCheckpointHeader(
                0,
                IDENTITY.kafkaClusterId(),
                IDENTITY.topicId(),
                IDENTITY.partition(),
                1,
                new StreamId("stream-1"),
                1,
                7,
                checkpointOffset,
                logStartOffset,
                checkpointOffset,
                1,
                "commit-1",
                new Checksum(ChecksumType.SHA256, "a".repeat(64)));
    }

    private static KafkaVirtualSegmentState.VirtualSegment segment(
            KafkaVirtualSegmentState.LogConfigHistoryEntry config,
            long baseOffset,
            long endOffset,
            long logicalBytes,
            KafkaVirtualSegmentState.SegmentState state
    ) {
        return new KafkaVirtualSegmentState.VirtualSegment(
                baseOffset,
                endOffset,
                baseOffset,
                1_000 + baseOffset,
                state == KafkaVirtualSegmentState.SegmentState.CLOSED
                        ? 1_001 + baseOffset
                        : 0,
                0,
                1_000 + baseOffset,
                baseOffset,
                logicalBytes,
                baseOffset * logicalBytes,
                endOffset * logicalBytes,
                config.configDigest(),
                baseOffset == 0
                        ? KafkaVirtualSegmentState.RollReason.INITIAL
                        : KafkaVirtualSegmentState.RollReason.SIZE,
                state);
    }

    private static byte[] bytes(MemoryRecords records) {
        ByteBuffer buffer = records.buffer().duplicate();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
}
