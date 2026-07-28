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
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.RollParams;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.checkpoint.KafkaCanonicalCheckpointState;
import com.nereusstream.kafka.checkpoint.KafkaDerivedIndexState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Partition-lock-owned virtual segment, config-history, and derived-index state.
 *
 * <p>The tracker never owns durable bytes. It advances only after the Nereus append is known stable
 * and can always be reconstructed from one canonical checkpoint plus committed tail batches.
 */
final class NereusCanonicalLogState {
    private static final int OFFSET_INDEX_ENTRY_BYTES = 8;
    private static final int TIME_INDEX_ENTRY_BYTES = 12;

    private final String partitionIdentity;
    private final ArrayList<MutableSegment> segments = new ArrayList<>();
    private final ArrayList<KafkaVirtualSegmentState.LogConfigHistoryEntry> configHistory =
            new ArrayList<>();
    private final TreeMap<Long, Position> exactBatchPositions = new TreeMap<>();

    private long logStartOffset;
    private long stableEndOffset;
    private long nextRollSequence;
    private KafkaVirtualSegmentState.RollReason preparedRollReason;
    private PendingRoll pendingRoll;

    NereusCanonicalLogState(String partitionIdentity) {
        this.partitionIdentity = Objects.requireNonNull(
                partitionIdentity, "partitionIdentity");
        if (partitionIdentity.isBlank()) {
            throw new IllegalArgumentException("partitionIdentity must not be blank");
        }
    }

    void restore(
            long expectedLogStartOffset,
            long expectedStableEndOffset,
            Optional<KafkaCanonicalCheckpointState> checkpoint,
            List<BatchObservation> committedTail,
            LogConfig currentConfig,
            long currentMetadataOffset,
            long nowMillis
    ) {
        if (expectedLogStartOffset < 0
                || expectedStableEndOffset < expectedLogStartOffset
                || currentMetadataOffset < 0
                || nowMillis < 0) {
            throw new IllegalArgumentException("invalid canonical Kafka restore bounds");
        }
        Objects.requireNonNull(checkpoint, "checkpoint");
        Objects.requireNonNull(committedTail, "committedTail");
        Objects.requireNonNull(currentConfig, "currentConfig");
        segments.clear();
        configHistory.clear();
        exactBatchPositions.clear();
        preparedRollReason = null;
        pendingRoll = null;
        nextRollSequence = 0;
        logStartOffset = expectedLogStartOffset;
        stableEndOffset = expectedLogStartOffset;

        if (checkpoint.isPresent()) {
            restoreCheckpointForCurrentWindow(
                    checkpoint.orElseThrow(),
                    expectedLogStartOffset,
                    expectedStableEndOffset);
        } else {
            addConfig(canonicalConfig(currentConfig, currentMetadataOffset, logStartOffset));
        }

        for (BatchObservation batch : committedTail) {
            recoverCommittedBatch(Objects.requireNonNull(batch, "committedTailBatch"));
        }
        if (stableEndOffset != expectedStableEndOffset) {
            throw invariant("canonical committed tail does not reach the recovered stable end");
        }

        KafkaVirtualSegmentState.LogConfigHistoryEntry exactCurrent =
                canonicalConfig(
                        currentConfig,
                        Math.max(currentMetadataOffset, nextMetadataOffset()),
                        stableEndOffset);
        KafkaVirtualSegmentState.LogConfigHistoryEntry previous = currentConfig();
        if (previous == null
                || !previous.configDigest().equals(exactCurrent.configDigest())
                || currentMetadataOffset > previous.metadataOffset()) {
            addConfig(exactCurrent);
        }
    }

    private void restoreCheckpointForCurrentWindow(
            KafkaCanonicalCheckpointState checkpoint,
            long expectedLogStartOffset,
            long expectedStableEndOffset
    ) {
        if (checkpoint.logStartOffset() > expectedLogStartOffset
                || checkpoint.stableEndOffset() > expectedStableEndOffset) {
            throw invariant("canonical checkpoint does not match recovered source bounds");
        }
        restoreCheckpoint(checkpoint);
        if (logStartOffset < expectedLogStartOffset) {
            advanceLogStart(expectedLogStartOffset);
        }
    }

    private void restoreCheckpoint(KafkaCanonicalCheckpointState checkpoint) {
        KafkaVirtualSegmentState virtual = checkpoint.virtualSegmentState();
        KafkaDerivedIndexState derived = checkpoint.derivedIndexState();
        logStartOffset = checkpoint.logStartOffset();
        stableEndOffset = checkpoint.stableEndOffset();
        configHistory.addAll(virtual.configHistory());

        Map<Long, KafkaDerivedIndexState.SegmentTimeIndex> timeByBase = new HashMap<>();
        derived.timeIndexes().forEach(index -> timeByBase.put(index.segmentBaseOffset(), index));
        Map<Long, KafkaDerivedIndexState.SegmentLogicalByteIndex> logicalByBase =
                new HashMap<>();
        derived.logicalByteIndexes().forEach(index ->
                logicalByBase.put(index.segmentBaseOffset(), index));

        for (KafkaVirtualSegmentState.VirtualSegment segment : virtual.segments()) {
            KafkaDerivedIndexState.SegmentTimeIndex time = timeByBase.get(segment.baseOffset());
            KafkaDerivedIndexState.SegmentLogicalByteIndex logical =
                    logicalByBase.get(segment.baseOffset());
            if (logical == null) {
                throw invariant("canonical checkpoint omitted a logical-byte segment");
            }
            MutableSegment restored = MutableSegment.restore(
                    segment,
                    time == null ? List.of() : time.entries(),
                    logical.samples());
            segments.add(restored);
            nextRollSequence = Math.max(
                    nextRollSequence,
                    addExact(segment.rollSequence(), 1, "roll sequence overflow"));
        }
    }

    void updateConfig(LogConfig config, long metadataOffset) {
        Objects.requireNonNull(config, "config");
        if (metadataOffset < 0) {
            throw new IllegalArgumentException("metadataOffset must be non-negative");
        }
        KafkaVirtualSegmentState.LogConfigHistoryEntry previous = currentConfig();
        if (previous != null && metadataOffset <= previous.metadataOffset()) {
            if (metadataOffset == previous.metadataOffset()
                    && previous.configDigest().equals(
                            canonicalConfig(config, metadataOffset, stableEndOffset)
                                    .configDigest())) {
                return;
            }
            throw invariant("Kafka log config metadata offset did not advance");
        }
        addConfig(canonicalConfig(config, metadataOffset, stableEndOffset));
        if (pendingRoll != null) {
            pendingRoll = new PendingRoll(
                    pendingRoll.baseOffset(),
                    KafkaVirtualSegmentState.RollReason.CONFIG,
                    pendingRoll.createdAtMillis(),
                    deterministicJitter(stableEndOffset, currentConfig()));
        }
    }

    long nextSyntheticMetadataOffset() {
        return nextMetadataOffset();
    }

    boolean matchesCurrentConfig(LogConfig config) {
        Objects.requireNonNull(config, "config");
        KafkaVirtualSegmentState.LogConfigHistoryEntry current = currentConfig();
        return current != null
                && current.configDigest().equals(
                        canonicalConfig(
                                config,
                                current.metadataOffset(),
                                stableEndOffset).configDigest());
    }

    private long nextMetadataOffset() {
        KafkaVirtualSegmentState.LogConfigHistoryEntry current = currentConfig();
        return current == null
                ? 0
                : addExact(current.metadataOffset(), 1, "config metadata offset overflow");
    }

    boolean prepareRoll(long activeBaseOffset, RollParams params) {
        Objects.requireNonNull(params, "params");
        preparedRollReason = evaluateRoll(activeBaseOffset, params);
        return preparedRollReason != null;
    }

    private KafkaVirtualSegmentState.RollReason evaluateRoll(
            long activeBaseOffset,
            RollParams params
    ) {
        if (pendingRoll != null) {
            if (pendingRoll.baseOffset() != activeBaseOffset) {
                throw invariant("synthetic active segment does not match pending virtual roll");
            }
            return null;
        }
        MutableSegment active = activeSegment();
        if (active == null || active.logicalBytes == 0) {
            return null;
        }
        if (active.baseOffset != activeBaseOffset) {
            throw invariant("synthetic active segment does not match canonical virtual segment");
        }
        KafkaVirtualSegmentState.LogConfigHistoryEntry config = currentConfig();
        if (config == null) {
            throw invariant("canonical Kafka state has no current log config");
        }
        return evaluateActiveRoll(active, config, params);
    }

    private KafkaVirtualSegmentState.RollReason evaluateActiveRoll(
            MutableSegment active,
            KafkaVirtualSegmentState.LogConfigHistoryEntry config,
            RollParams params
    ) {
        if (!active.configDigest.equals(config.configDigest())) {
            return KafkaVirtualSegmentState.RollReason.CONFIG;
        }
        KafkaVirtualSegmentState.RollReason sizeOrTime =
                evaluateSizeOrTimeRoll(active, config, params);
        if (sizeOrTime != null) {
            return sizeOrTime;
        }
        if (params.maxOffsetInMessages() - active.baseOffset > Integer.MAX_VALUE) {
            return KafkaVirtualSegmentState.RollReason.RELATIVE_OFFSET_OVERFLOW;
        }
        return evaluateIndexRoll(active, config);
    }

    private KafkaVirtualSegmentState.RollReason evaluateSizeOrTimeRoll(
            MutableSegment active,
            KafkaVirtualSegmentState.LogConfigHistoryEntry config,
            RollParams params
    ) {
        if (active.logicalBytes > config.segmentBytes() - params.messagesSize()) {
            return KafkaVirtualSegmentState.RollReason.SIZE;
        }
        long elapsed = active.rollingTimestamp >= 0 && params.maxTimestampInMessages() >= 0
                ? params.maxTimestampInMessages() - active.rollingTimestamp
                : params.now() - active.createdAtMillis;
        return elapsed > config.segmentMs() - active.rollJitterMillis
                ? KafkaVirtualSegmentState.RollReason.TIME
                : null;
    }

    private KafkaVirtualSegmentState.RollReason evaluateIndexRoll(
            MutableSegment active,
            KafkaVirtualSegmentState.LogConfigHistoryEntry config
    ) {
        int offsetCapacity = Math.max(
                1, config.segmentIndexBytes() / OFFSET_INDEX_ENTRY_BYTES);
        int timeCapacity = Math.max(
                1, config.segmentIndexBytes() / TIME_INDEX_ENTRY_BYTES - 1);
        if (active.logicalSamples.size() >= offsetCapacity
                || active.timeEntries.size() >= timeCapacity) {
            return KafkaVirtualSegmentState.RollReason.INDEX_FULL;
        }
        return null;
    }

    void stageRoll(long baseOffset, long nowMillis) {
        if (baseOffset < 0 || nowMillis < 0) {
            throw new IllegalArgumentException("invalid virtual roll fields");
        }
        MutableSegment active = activeSegment();
        if (active == null || active.logicalBytes == 0 || baseOffset != stableEndOffset) {
            throw invariant("virtual roll requires one non-empty active segment at stable end");
        }
        KafkaVirtualSegmentState.RollReason reason = preparedRollReason == null
                ? KafkaVirtualSegmentState.RollReason.MANUAL
                : preparedRollReason;
        preparedRollReason = null;
        KafkaVirtualSegmentState.LogConfigHistoryEntry config = currentConfig();
        pendingRoll = new PendingRoll(
                baseOffset,
                reason,
                Math.max(nowMillis, active.createdAtMillis),
                deterministicJitter(baseOffset, config));
    }

    void cancelPendingRoll(long baseOffset) {
        if (pendingRoll != null && pendingRoll.baseOffset() == baseOffset) {
            pendingRoll = null;
        }
        preparedRollReason = null;
    }

    long segmentLogicalBytes(long segmentBaseOffset) {
        if (pendingRoll != null && pendingRoll.baseOffset() == segmentBaseOffset) {
            return 0;
        }
        for (MutableSegment segment : segments) {
            if (segment.baseOffset == segmentBaseOffset) {
                return segment.logicalBytes;
            }
        }
        return 0;
    }

    long segmentRollJitter(long segmentBaseOffset) {
        if (pendingRoll != null && pendingRoll.baseOffset() == segmentBaseOffset) {
            return pendingRoll.rollJitterMillis();
        }
        for (MutableSegment segment : segments) {
            if (segment.baseOffset == segmentBaseOffset) {
                return segment.rollJitterMillis;
            }
        }
        KafkaVirtualSegmentState.LogConfigHistoryEntry config = currentConfig();
        return config == null ? 0 : deterministicJitter(segmentBaseOffset, config);
    }

    void commitStable(
            List<BatchObservation> batches,
            long nowMillis
    ) {
        Objects.requireNonNull(batches, "batches");
        if (batches.isEmpty() || nowMillis < 0) {
            throw new IllegalArgumentException("stable append must contain observed batches");
        }
        long expectedOffset = stableEndOffset;
        for (BatchObservation batch : batches) {
            if (batch.baseOffset() != expectedOffset) {
                throw invariant("stable append batches are not dense from canonical end");
            }
            expectedOffset = batch.endOffset();
        }
        beginSegmentIfRequired(batches.get(0), nowMillis);
        for (BatchObservation batch : batches) {
            appendBatch(batch);
        }
    }

    private void recoverCommittedBatch(BatchObservation batch) {
        long replayTime = batch.largestTimestamp() >= 0
                ? batch.largestTimestamp()
                : Math.max(0, activeCreatedAt());
        if (activeSegment() != null && pendingRoll == null) {
            KafkaVirtualSegmentState.LogConfigHistoryEntry config = currentConfig();
            RollParams params = new RollParams(
                    config.segmentMs(),
                    Math.toIntExact(Math.min(Integer.MAX_VALUE, config.segmentBytes())),
                    batch.largestTimestamp(),
                    batch.endOffset() - 1,
                    batch.logicalBytes(),
                    replayTime);
            KafkaVirtualSegmentState.RollReason reason =
                    evaluateRoll(activeSegment().baseOffset, params);
            if (reason != null) {
                preparedRollReason = reason;
                stageRoll(batch.baseOffset(), replayTime);
            }
        }
        commitStable(List.of(batch), replayTime);
    }

    private void beginSegmentIfRequired(BatchObservation first, long nowMillis) {
        KafkaVirtualSegmentState.LogConfigHistoryEntry config = currentConfig();
        if (config == null) {
            throw invariant("stable append has no effective Kafka log config");
        }
        if (pendingRoll != null) {
            if (pendingRoll.baseOffset() != first.baseOffset()) {
                throw invariant("stable append does not match pending virtual roll");
            }
            MutableSegment previous = activeSegment();
            previous.endOffset = first.baseOffset();
            previous.closedAtMillis = Math.max(
                    pendingRoll.createdAtMillis(), previous.createdAtMillis);
            previous.state = KafkaVirtualSegmentState.SegmentState.CLOSED;
            MutableSegment next = MutableSegment.empty(
                    first.baseOffset(),
                    nextRollSequence++,
                    pendingRoll.createdAtMillis(),
                    pendingRoll.rollJitterMillis(),
                    config.configDigest(),
                    pendingRoll.reason(),
                    cumulativeLogicalBytes());
            segments.add(next);
            pendingRoll = null;
            return;
        }
        if (segments.isEmpty()) {
            long createdAt = Math.max(0, nowMillis);
            segments.add(MutableSegment.empty(
                    first.baseOffset(),
                    nextRollSequence++,
                    createdAt,
                    deterministicJitter(first.baseOffset(), config),
                    config.configDigest(),
                    KafkaVirtualSegmentState.RollReason.INITIAL,
                    cumulativeLogicalBytes()));
        }
    }

    private void appendBatch(BatchObservation batch) {
        MutableSegment active = activeSegment();
        if (active == null
                || active.state != KafkaVirtualSegmentState.SegmentState.ACTIVE
                || batch.baseOffset() != stableEndOffset
                || batch.baseOffset() != active.endOffset) {
            throw invariant("stable batch does not extend the active virtual segment");
        }
        KafkaVirtualSegmentState.LogConfigHistoryEntry segmentConfig =
                configByDigest(active.configDigest);
        if (active.bytesSinceLastIndexEntry > segmentConfig.indexIntervalBytes()) {
            long cumulative = active.logicalBytes;
            active.logicalSamples.add(new KafkaDerivedIndexState.LogicalByteSample(
                    batch.baseOffset(), cumulative));
            if (active.largestTimestamp >= 0
                    && (active.timeEntries.isEmpty()
                            || active.largestTimestamp
                                    > active.timeEntries.get(
                                            active.timeEntries.size() - 1).timestamp())) {
                active.timeEntries.add(new KafkaDerivedIndexState.TimeIndexEntry(
                        active.largestTimestamp, active.maxTimestampOffset));
            }
            active.bytesSinceLastIndexEntry = 0;
        }
        exactBatchPositions.put(
                batch.baseOffset(),
                new Position(active.baseOffset, active.logicalBytes));
        active.endOffset = batch.endOffset();
        active.logicalBytes = addExact(
                active.logicalBytes,
                batch.logicalBytes(),
                "virtual segment logical bytes overflow");
        active.lastCumulativeBytes = addExact(
                active.lastCumulativeBytes,
                batch.logicalBytes(),
                "canonical cumulative bytes overflow");
        active.bytesSinceLastIndexEntry = addExact(
                active.bytesSinceLastIndexEntry,
                batch.logicalBytes(),
                "virtual index interval bytes overflow");
        if (batch.largestTimestamp() >= 0
                && (active.largestTimestamp < 0
                        || batch.largestTimestamp() > active.largestTimestamp
                        || (batch.largestTimestamp() == active.largestTimestamp
                                && batch.maxTimestampOffset() < active.maxTimestampOffset))) {
            active.largestTimestamp = batch.largestTimestamp();
            active.maxTimestampOffset = batch.maxTimestampOffset();
        }
        if (active.rollingTimestamp < 0 && batch.largestTimestamp() >= 0) {
            active.rollingTimestamp = batch.largestTimestamp();
        }
        stableEndOffset = batch.endOffset();
    }

    void advanceLogStart(long durableOffset) {
        if (durableOffset < logStartOffset || durableOffset > stableEndOffset) {
            throw new IllegalArgumentException("durable log start is outside canonical bounds");
        }
        logStartOffset = durableOffset;
        segments.removeIf(segment -> segment.endOffset <= durableOffset);
        exactBatchPositions.headMap(durableOffset, false).clear();
        if (segments.isEmpty()) {
            pendingRoll = null;
            preparedRollReason = null;
        }
        pruneConfigHistory();
    }

    KafkaVirtualSegmentState virtualSegments() {
        ArrayList<KafkaVirtualSegmentState.VirtualSegment> frozen =
                new ArrayList<>(segments.size());
        for (MutableSegment segment : segments) {
            frozen.add(segment.freeze());
        }
        return new KafkaVirtualSegmentState(
                logStartOffset,
                stableEndOffset,
                frozen,
                List.copyOf(configHistory));
    }

    KafkaDerivedIndexState derivedIndexes() {
        ArrayList<KafkaDerivedIndexState.SegmentTimeIndex> times =
                new ArrayList<>(segments.size());
        ArrayList<KafkaDerivedIndexState.SegmentLogicalByteIndex> logical =
                new ArrayList<>(segments.size());
        for (MutableSegment segment : segments) {
            List<KafkaDerivedIndexState.TimeIndexEntry> retainedTimes =
                    segment.timeEntries.stream()
                            .filter(entry -> entry.offset() >= logStartOffset)
                            .toList();
            List<KafkaDerivedIndexState.LogicalByteSample> retainedLogical =
                    segment.logicalSamples.stream()
                            .filter(sample -> sample.entryStartOffset() >= logStartOffset)
                            .toList();
            times.add(new KafkaDerivedIndexState.SegmentTimeIndex(
                    segment.baseOffset, retainedTimes));
            logical.add(new KafkaDerivedIndexState.SegmentLogicalByteIndex(
                    segment.baseOffset, segment.logicalBytes, retainedLogical));
        }
        return new KafkaDerivedIndexState(
                logStartOffset, stableEndOffset, times, logical);
    }

    Position positionForOffset(long offset) {
        if (offset < logStartOffset || offset > stableEndOffset) {
            return new Position(offset, 0);
        }
        MutableSegment segment = segmentForOffset(offset);
        if (segment == null) {
            return new Position(offset, 0);
        }
        if (offset == stableEndOffset
                && segment.state == KafkaVirtualSegmentState.SegmentState.ACTIVE) {
            return new Position(segment.baseOffset, segment.logicalBytes);
        }
        Map.Entry<Long, Position> exact = exactBatchPositions.floorEntry(offset);
        if (exact != null && exact.getValue().segmentBaseOffset() == segment.baseOffset) {
            return exact.getValue();
        }
        long relative = 0;
        for (KafkaDerivedIndexState.LogicalByteSample sample : segment.logicalSamples) {
            if (sample.entryStartOffset() > offset) {
                break;
            }
            relative = sample.cumulativeLogicalBytes();
        }
        return new Position(segment.baseOffset, relative);
    }

    long timestampScanCandidate(long targetTimestamp) {
        for (MutableSegment segment : segments) {
            if (segment.endOffset <= logStartOffset
                    || segment.largestTimestamp < targetTimestamp) {
                continue;
            }
            long candidate = Math.max(segment.baseOffset, logStartOffset);
            for (KafkaDerivedIndexState.TimeIndexEntry entry : segment.timeEntries) {
                if (entry.timestamp() > targetTimestamp) {
                    break;
                }
                candidate = Math.max(candidate, entry.offset());
            }
            return candidate;
        }
        return stableEndOffset;
    }

    Optional<TimestampPosition> maxTimestampPosition() {
        long timestamp = RecordBatch.NO_TIMESTAMP;
        long offset = -1;
        for (MutableSegment segment : segments) {
            if (segment.largestTimestamp > timestamp
                    || (segment.largestTimestamp == timestamp
                            && segment.maxTimestampOffset >= 0
                            && (offset < 0 || segment.maxTimestampOffset < offset))) {
                timestamp = segment.largestTimestamp;
                offset = segment.maxTimestampOffset;
            }
        }
        return timestamp < 0
                ? Optional.empty()
                : Optional.of(new TimestampPosition(timestamp, offset));
    }

    List<Long> segmentBaseOffsets(long emptyDefault) {
        if (segments.isEmpty()) {
            return List.of(emptyDefault);
        }
        return segments.stream().map(segment -> segment.baseOffset).toList();
    }

    private MutableSegment segmentForOffset(long offset) {
        MutableSegment selected = null;
        for (MutableSegment segment : segments) {
            if (segment.baseOffset <= offset
                    && (offset < segment.endOffset
                            || offset == stableEndOffset
                                    && segment.state
                                            == KafkaVirtualSegmentState.SegmentState.ACTIVE)) {
                selected = segment;
            }
        }
        return selected;
    }

    private void addConfig(KafkaVirtualSegmentState.LogConfigHistoryEntry entry) {
        KafkaVirtualSegmentState.LogConfigHistoryEntry previous = currentConfig();
        if (previous != null
                && (entry.metadataOffset() <= previous.metadataOffset()
                        || entry.effectiveFromOffset() < previous.effectiveFromOffset())) {
            throw invariant("Kafka log config history is not monotonic");
        }
        configHistory.add(entry);
    }

    private void pruneConfigHistory() {
        LinkedHashSet<Checksum> referenced = new LinkedHashSet<>();
        segments.forEach(segment -> referenced.add(segment.configDigest));
        KafkaVirtualSegmentState.LogConfigHistoryEntry current = currentConfig();
        configHistory.removeIf(entry ->
                !referenced.contains(entry.configDigest()) && entry != current);
    }

    private KafkaVirtualSegmentState.LogConfigHistoryEntry configByDigest(Checksum digest) {
        for (KafkaVirtualSegmentState.LogConfigHistoryEntry entry : configHistory) {
            if (entry.configDigest().equals(digest)
                    && entry.effectiveFromOffset() <= stableEndOffset) {
                return entry;
            }
        }
        throw invariant("virtual segment references an unavailable Kafka log config");
    }

    private KafkaVirtualSegmentState.LogConfigHistoryEntry currentConfig() {
        return configHistory.isEmpty()
                ? null
                : configHistory.get(configHistory.size() - 1);
    }

    private MutableSegment activeSegment() {
        return segments.isEmpty() ? null : segments.get(segments.size() - 1);
    }

    private long activeCreatedAt() {
        MutableSegment active = activeSegment();
        return active == null ? 0 : active.createdAtMillis;
    }

    private long cumulativeLogicalBytes() {
        MutableSegment active = activeSegment();
        return active == null ? 0 : active.lastCumulativeBytes;
    }

    private long deterministicJitter(
            long baseOffset,
            KafkaVirtualSegmentState.LogConfigHistoryEntry config
    ) {
        long maximum = config.segmentJitterMillis();
        if (maximum == 0) {
            return 0;
        }
        long mixed = 31L * partitionIdentity.hashCode()
                + 17L * config.configDigest().value().hashCode()
                + baseOffset;
        return Math.floorMod(mixed, addExact(maximum, 1, "segment jitter overflow"));
    }

    static KafkaVirtualSegmentState.LogConfigHistoryEntry canonicalConfig(
            LogConfig config,
            long metadataOffset,
            long effectiveFromOffset
    ) {
        Objects.requireNonNull(config, "config");
        int cleanupPolicyFlags = (config.delete
                ? KafkaVirtualSegmentState.LogConfigHistoryEntry.CLEANUP_DELETE_FLAG
                : 0)
                | (config.compact
                        ? KafkaVirtualSegmentState.LogConfigHistoryEntry.CLEANUP_COMPACT_FLAG
                        : 0);
        if (cleanupPolicyFlags == 0) {
            throw invariant("Nereus Kafka log has no supported cleanup policy");
        }
        return KafkaVirtualSegmentState.LogConfigHistoryEntry.create(
                metadataOffset,
                effectiveFromOffset,
                config.segmentSize(),
                config.segmentMs,
                config.segmentJitterMs,
                config.maxIndexSize,
                config.indexInterval,
                config.retentionSize,
                config.retentionMs,
                config.fileDeleteDelayMs,
                config.deleteRetentionMs,
                config.compactionLagMs,
                config.maxCompactionLagMs,
                config.minCleanableRatio,
                cleanupPolicyFlags);
    }

    static List<BatchObservation> observe(MemoryRecords records) {
        Objects.requireNonNull(records, "records");
        ArrayList<BatchObservation> observations = new ArrayList<>();
        int observedBytes = 0;
        for (RecordBatch batch : records.batches()) {
            long largestTimestamp = RecordBatch.NO_TIMESTAMP;
            long maxTimestampOffset = -1;
            for (Record record : batch) {
                if (record.timestamp() >= 0
                        && (largestTimestamp < 0
                                || record.timestamp() > largestTimestamp
                                || (record.timestamp() == largestTimestamp
                                        && record.offset() < maxTimestampOffset))) {
                    largestTimestamp = record.timestamp();
                    maxTimestampOffset = record.offset();
                }
            }
            observations.add(new BatchObservation(
                    batch.baseOffset(),
                    addExact(batch.lastOffset(), 1, "batch end offset overflow"),
                    batch.sizeInBytes(),
                    largestTimestamp,
                    maxTimestampOffset));
            observedBytes = addExact(
                    observedBytes, batch.sizeInBytes(), "observed append bytes overflow");
        }
        if (observations.isEmpty() || observedBytes != records.sizeInBytes()) {
            throw invariant("stable append does not contain exact Kafka batches");
        }
        return List.copyOf(observations);
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
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    record BatchObservation(
            long baseOffset,
            long endOffset,
            int logicalBytes,
            long largestTimestamp,
            long maxTimestampOffset
    ) {
        BatchObservation {
            if (invalidObservation(
                    baseOffset,
                    endOffset,
                    logicalBytes,
                    largestTimestamp,
                    maxTimestampOffset)) {
                throw new IllegalArgumentException("invalid canonical Kafka batch observation");
            }
        }

        private static boolean invalidObservation(
                long baseOffset,
                long endOffset,
                int logicalBytes,
                long largestTimestamp,
                long maxTimestampOffset
        ) {
            if (baseOffset < 0 || endOffset <= baseOffset || logicalBytes <= 0) {
                return true;
            }
            if ((largestTimestamp < 0) != (maxTimestampOffset < 0)) {
                return true;
            }
            return maxTimestampOffset >= 0
                    && (maxTimestampOffset < baseOffset
                            || maxTimestampOffset >= endOffset);
        }
    }

    record Position(long segmentBaseOffset, long relativeLogicalBytes) {
        Position {
            if (segmentBaseOffset < 0 || relativeLogicalBytes < 0) {
                throw new IllegalArgumentException("invalid canonical Kafka position");
            }
        }
    }

    record TimestampPosition(long timestamp, long offset) {
        TimestampPosition {
            if (timestamp < 0 || offset < 0) {
                throw new IllegalArgumentException("invalid canonical Kafka timestamp position");
            }
        }
    }

    private record PendingRoll(
            long baseOffset,
            KafkaVirtualSegmentState.RollReason reason,
            long createdAtMillis,
            long rollJitterMillis
    ) {
        private PendingRoll {
            if (baseOffset < 0 || createdAtMillis < 0 || rollJitterMillis < 0) {
                throw new IllegalArgumentException("invalid pending virtual roll");
            }
            Objects.requireNonNull(reason, "reason");
        }
    }

    private static final class MutableSegment {
        private final long baseOffset;
        private final long rollSequence;
        private final long createdAtMillis;
        private final long rollJitterMillis;
        private final Checksum configDigest;
        private final KafkaVirtualSegmentState.RollReason rollReason;
        private final long firstCumulativeBytes;
        private final ArrayList<KafkaDerivedIndexState.TimeIndexEntry> timeEntries;
        private final ArrayList<KafkaDerivedIndexState.LogicalByteSample> logicalSamples;

        private long endOffset;
        private long closedAtMillis;
        private long largestTimestamp;
        private long maxTimestampOffset;
        private long logicalBytes;
        private long lastCumulativeBytes;
        private long rollingTimestamp;
        private int bytesSinceLastIndexEntry;
        private KafkaVirtualSegmentState.SegmentState state;

        private MutableSegment(
                KafkaVirtualSegmentState.VirtualSegment segment,
                long rollingTimestamp,
                int bytesSinceLastIndexEntry,
                List<KafkaDerivedIndexState.TimeIndexEntry> timeEntries,
                List<KafkaDerivedIndexState.LogicalByteSample> logicalSamples
        ) {
            KafkaVirtualSegmentState.VirtualSegment exact =
                    Objects.requireNonNull(segment, "segment");
            this.baseOffset = exact.baseOffset();
            this.endOffset = exact.endOffset();
            this.rollSequence = exact.rollSequence();
            this.createdAtMillis = exact.createdAtMillis();
            this.closedAtMillis = exact.closedAtMillis();
            this.rollJitterMillis = exact.rollJitterMillis();
            this.largestTimestamp = exact.largestTimestamp();
            this.maxTimestampOffset = exact.maxTimestampOffset();
            this.logicalBytes = exact.logicalBytes();
            this.firstCumulativeBytes = exact.firstCumulativeBytes();
            this.lastCumulativeBytes = exact.lastCumulativeBytes();
            this.configDigest = exact.configDigest();
            this.rollReason = exact.rollReason();
            this.state = exact.state();
            this.rollingTimestamp = rollingTimestamp;
            this.bytesSinceLastIndexEntry = bytesSinceLastIndexEntry;
            this.timeEntries = new ArrayList<>(timeEntries);
            this.logicalSamples = new ArrayList<>(logicalSamples);
        }

        private static MutableSegment empty(
                long baseOffset,
                long rollSequence,
                long createdAtMillis,
                long rollJitterMillis,
                Checksum configDigest,
                KafkaVirtualSegmentState.RollReason rollReason,
                long cumulativeLogicalBytes
        ) {
            return new MutableSegment(
                    new KafkaVirtualSegmentState.VirtualSegment(
                            baseOffset,
                            baseOffset,
                            rollSequence,
                            createdAtMillis,
                            0,
                            rollJitterMillis,
                            RecordBatch.NO_TIMESTAMP,
                            -1,
                            0,
                            cumulativeLogicalBytes,
                            cumulativeLogicalBytes,
                            configDigest,
                            rollReason,
                            KafkaVirtualSegmentState.SegmentState.ACTIVE),
                    RecordBatch.NO_TIMESTAMP,
                    0,
                    List.of(),
                    List.of());
        }

        private static MutableSegment restore(
                KafkaVirtualSegmentState.VirtualSegment segment,
                List<KafkaDerivedIndexState.TimeIndexEntry> timeEntries,
                List<KafkaDerivedIndexState.LogicalByteSample> logicalSamples
        ) {
            long rollingTimestamp = timeEntries.isEmpty()
                    ? segment.largestTimestamp()
                    : timeEntries.get(0).timestamp();
            long indexedBytes = logicalSamples.isEmpty()
                    ? 0
                    : logicalSamples.get(logicalSamples.size() - 1)
                            .cumulativeLogicalBytes();
            long unindexed = segment.logicalBytes() - indexedBytes;
            return new MutableSegment(
                    segment,
                    rollingTimestamp,
                    Math.toIntExact(Math.min(Integer.MAX_VALUE, unindexed)),
                    timeEntries,
                    logicalSamples);
        }

        private KafkaVirtualSegmentState.VirtualSegment freeze() {
            return new KafkaVirtualSegmentState.VirtualSegment(
                    baseOffset,
                    endOffset,
                    rollSequence,
                    createdAtMillis,
                    closedAtMillis,
                    rollJitterMillis,
                    largestTimestamp,
                    maxTimestampOffset,
                    logicalBytes,
                    firstCumulativeBytes,
                    lastCumulativeBytes,
                    configDigest,
                    rollReason,
                    state);
        }
    }
}
