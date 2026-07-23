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

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.recovery.KafkaReplayBatch;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSection;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * Fresh M3-derived Kafka state rebuilt from exact COMMITTED RecordBatch bytes.
 *
 * <p>The object is mutable only while owned by one recovery codec. Validation freezes it before the partition
 * publisher can expose it. M3 deliberately rejects producer, transaction, control and NKC1-derived state; M4 replaces
 * those fail-closed boundaries with stock producer/transaction recovery.
 */
public final class NereusKafkaRecoveredState {
    private final KafkaPartitionIdentity identity;
    private final int leaderEpoch;
    private final long logStartOffset;
    private final long expectedStableEndOffset;
    private final List<LeaderEpochRange> leaderEpochRanges = new ArrayList<>();

    private long nextOffset;
    private int batchCount;
    private long recordCount;
    private long logicalBytes;
    private long largestTimestamp = RecordBatch.NO_TIMESTAMP;
    private long maxTimestampOffset = -1;
    private boolean frozen;

    NereusKafkaRecoveredState(
            KafkaPartitionIdentity identity,
            int leaderEpoch,
            long logStartOffset,
            long expectedStableEndOffset
    ) {
        this.identity = Objects.requireNonNull(identity, "identity");
        if (leaderEpoch < 0
                || logStartOffset < 0
                || expectedStableEndOffset < logStartOffset) {
            throw new IllegalArgumentException("invalid Nereus Kafka recovery state bounds");
        }
        this.leaderEpoch = leaderEpoch;
        this.logStartOffset = logStartOffset;
        this.expectedStableEndOffset = expectedStableEndOffset;
    }

    void hydrateCheckpoint(
            List<KafkaCheckpointSection> sections,
            long checkpointOffset
    ) {
        requireMutable();
        Objects.requireNonNull(sections, "sections");
        throw new NereusException(
                ErrorCode.UNSUPPORTED_FORMAT,
                false,
                "F9-M3 Kafka recovery does not yet consume NKC1 derived-state sections at offset "
                        + checkpointOffset);
    }

    void replay(KafkaReplayBatch replay) {
        requireMutable();
        Objects.requireNonNull(replay, "replay");
        if (replay.baseOffset() != nextOffset) {
            throw invariant("Kafka recovery state received a non-contiguous batch");
        }
        RecordBatch batch = exactBatch(replay);
        requireM3Batch(batch);
        long batchRecords = validateRecords(batch);
        long expectedNext = addExact(
                batch.lastOffset(), 1, "Kafka recovery batch offset overflows");
        observeLeaderEpoch(batch.partitionLeaderEpoch(), batch.baseOffset());
        nextOffset = expectedNext;
        batchCount = addExact(batchCount, 1, "Kafka recovery batch count overflows");
        recordCount = addExact(
                recordCount, batchRecords, "Kafka recovery record count overflows");
        logicalBytes = addExact(
                logicalBytes,
                replay.encodedBatch().length,
                "Kafka recovery logical bytes overflow");
    }

    private RecordBatch exactBatch(KafkaReplayBatch replay) {
        byte[] encoded = replay.encodedBatch();
        MemoryRecords records = MemoryRecords.readableRecords(ByteBuffer.wrap(encoded));
        if (records.validBytes() != encoded.length) {
            throw invariant("Kafka recovery batch contains trailing or incomplete bytes");
        }
        Iterator<? extends RecordBatch> batches = records.batches().iterator();
        if (!batches.hasNext()) {
            throw invariant("Kafka recovery entry contains no RecordBatch");
        }
        RecordBatch batch = batches.next();
        if (batches.hasNext()
                || batch.magic() != RecordBatch.MAGIC_VALUE_V2
                || batch.baseOffset() != replay.baseOffset()
                || batch.lastOffset() != replay.lastOffset()) {
            throw invariant("Kafka recovery entry does not contain one exact magic-v2 RecordBatch");
        }
        batch.ensureValid();
        return batch;
    }

    private static void requireM3Batch(RecordBatch batch) {
        if (batch.hasProducerId() || batch.isTransactional() || batch.isControlBatch()) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_FORMAT,
                    false,
                    "F9-M3 recovery accepts only non-idempotent non-transactional data batches");
        }
    }

    private long validateRecords(RecordBatch batch) {
        long recordOffset = batch.baseOffset();
        long batchRecords = 0;
        for (Record record : batch) {
            record.ensureValid();
            if (record.offset() != recordOffset) {
                throw invariant("Kafka recovery RecordBatch contains a non-dense record offset");
            }
            observeTimestamp(record.timestamp(), record.offset());
            recordOffset = addExact(recordOffset, 1, "Kafka recovery record offset overflows");
            batchRecords = addExact(batchRecords, 1, "Kafka recovery record count overflows");
        }
        long expectedNext = addExact(batch.lastOffset(), 1, "Kafka recovery batch offset overflows");
        if (recordOffset != expectedNext || batchRecords != expectedNext - batch.baseOffset()) {
            throw invariant("Kafka recovery RecordBatch logical span does not match its records");
        }
        return batchRecords;
    }

    void freeze(KafkaCheckpointSourceState source) {
        requireMutable();
        Objects.requireNonNull(source, "source");
        if (source.trimOffset() != logStartOffset
                || source.endOffset() != expectedStableEndOffset
                || nextOffset != expectedStableEndOffset
                || source.authority().authorityEpoch() != leaderEpoch
                || source.appendInFlight()
                || source.stateMapEndOffset() != source.endOffset()) {
            throw invariant("Kafka recovered state does not match the frozen stable source");
        }
        observeLeaderEpoch(leaderEpoch, expectedStableEndOffset);
        frozen = true;
    }

    public KafkaPartitionIdentity identity() {
        return identity;
    }

    public int leaderEpoch() {
        return leaderEpoch;
    }

    public long logStartOffset() {
        return logStartOffset;
    }

    public long stableEndOffset() {
        requireFrozen();
        return expectedStableEndOffset;
    }

    public long lastStableOffset() {
        return stableEndOffset();
    }

    public int batchCount() {
        requireFrozen();
        return batchCount;
    }

    public long recordCount() {
        requireFrozen();
        return recordCount;
    }

    public long logicalBytes() {
        requireFrozen();
        return logicalBytes;
    }

    public OptionalLong largestTimestamp() {
        requireFrozen();
        return largestTimestamp == RecordBatch.NO_TIMESTAMP
                ? OptionalLong.empty()
                : OptionalLong.of(largestTimestamp);
    }

    public OptionalLong maxTimestampOffset() {
        requireFrozen();
        return maxTimestampOffset < 0
                ? OptionalLong.empty()
                : OptionalLong.of(maxTimestampOffset);
    }

    public List<LeaderEpochRange> leaderEpochRanges() {
        requireFrozen();
        return List.copyOf(leaderEpochRanges);
    }

    public boolean frozen() {
        return frozen;
    }

    private void observeLeaderEpoch(int observedEpoch, long startOffset) {
        if (observedEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH) {
            return;
        }
        if (observedEpoch < 0 || observedEpoch > leaderEpoch) {
            throw invariant("Kafka recovery batch has an invalid partition leader epoch");
        }
        if (leaderEpochRanges.isEmpty()) {
            leaderEpochRanges.add(new LeaderEpochRange(observedEpoch, startOffset));
            return;
        }
        LeaderEpochRange last = leaderEpochRanges.get(leaderEpochRanges.size() - 1);
        if (observedEpoch < last.leaderEpoch()) {
            throw invariant("Kafka recovery leader epochs are not monotonic");
        }
        if (observedEpoch > last.leaderEpoch()) {
            if (startOffset <= last.startOffset()) {
                throw invariant("Kafka recovery leader epoch start offsets are not monotonic");
            }
            leaderEpochRanges.add(new LeaderEpochRange(observedEpoch, startOffset));
        }
    }

    private void observeTimestamp(long timestamp, long offset) {
        if (timestamp < 0) {
            return;
        }
        if (largestTimestamp == RecordBatch.NO_TIMESTAMP
                || timestamp > largestTimestamp
                || (timestamp == largestTimestamp && offset < maxTimestampOffset)) {
            largestTimestamp = timestamp;
            maxTimestampOffset = offset;
        }
    }

    private void requireMutable() {
        if (frozen) {
            throw new IllegalStateException("Nereus Kafka recovered state is already frozen");
        }
    }

    private void requireFrozen() {
        if (!frozen) {
            throw new IllegalStateException("Nereus Kafka recovered state is not frozen");
        }
    }

    private static int addExact(int left, int right, String message) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException failure) {
            throw invariant(message);
        }
    }

    private static long addExact(long left, long right, String message) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException failure) {
            throw invariant(message);
        }
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                message);
    }

    public record LeaderEpochRange(int leaderEpoch, long startOffset) {
        public LeaderEpochRange {
            if (leaderEpoch < 0 || startOffset < 0) {
                throw new IllegalArgumentException("invalid Kafka leader epoch range");
            }
        }
    }
}
