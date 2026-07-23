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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;

import com.nereusstream.api.AppendAuthority;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.recovery.KafkaReplayBatch;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NereusKafkaRecoveryStateCodecTest {
    private static final KafkaPartitionIdentity IDENTITY =
            new KafkaPartitionIdentity(
                    "kraft-cluster",
                    "AAAAAAAAAAAAAAAAAAAAAQ",
                    0,
                    "events");

    @Test
    void rebuildsAndFreezesExactM3DerivedStateAcrossLeaderEpochs() {
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
                new NereusKafkaRecoveryStateCodec(IDENTITY, 7, 0, 3);
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
    void rejectsIdempotentDataAndCheckpointHydrationUntilM4() {
        byte[] idempotent = bytes(MemoryRecords.withIdempotentRecords(
                0,
                Compression.of(CompressionType.NONE).build(),
                99,
                (short) 1,
                0,
                7,
                new SimpleRecord(100, "a".getBytes())));
        NereusKafkaRecoveryStateCodec codec =
                new NereusKafkaRecoveryStateCodec(IDENTITY, 7, 0, 1);
        NereusKafkaRecoveredState state = codec.freshState();

        NereusException unsupported = assertThrows(
                NereusException.class,
                () -> codec.replayBatch(
                        state,
                        new KafkaReplayBatch(0, 0, idempotent)));

        assertEquals(ErrorCode.UNSUPPORTED_FORMAT, unsupported.code());
        assertFalse(unsupported.retriable());

        NereusKafkaRecoveryStateCodec checkpointCodec =
                new NereusKafkaRecoveryStateCodec(IDENTITY, 7, 0, 0);
        NereusKafkaRecoveredState checkpointState = checkpointCodec.freshState();
        NereusException checkpointUnsupported = assertThrows(
                NereusException.class,
                () -> checkpointCodec.hydrateCheckpoint(
                        checkpointState,
                        List.of(),
                        0));
        assertEquals(ErrorCode.UNSUPPORTED_FORMAT, checkpointUnsupported.code());
    }

    @Test
    void rejectsTrailingBytesAndAFrozenSourceMismatch() {
        byte[] exact = bytes(MemoryRecords.withRecords(
                0,
                Compression.of(CompressionType.NONE).build(),
                6,
                new SimpleRecord(100, "a".getBytes())));
        byte[] trailing = java.util.Arrays.copyOf(exact, exact.length + 1);
        trailing[trailing.length - 1] = 1;
        NereusKafkaRecoveryStateCodec trailingCodec =
                new NereusKafkaRecoveryStateCodec(IDENTITY, 7, 0, 1);

        NereusException malformed = assertThrows(
                NereusException.class,
                () -> trailingCodec.replayBatch(
                        trailingCodec.freshState(),
                        new KafkaReplayBatch(0, 0, trailing)));

        assertEquals(ErrorCode.METADATA_INVARIANT_VIOLATION, malformed.code());

        NereusKafkaRecoveryStateCodec mismatchCodec =
                new NereusKafkaRecoveryStateCodec(IDENTITY, 7, 0, 1);
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

    private static byte[] bytes(MemoryRecords records) {
        ByteBuffer buffer = records.buffer().duplicate();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
}
