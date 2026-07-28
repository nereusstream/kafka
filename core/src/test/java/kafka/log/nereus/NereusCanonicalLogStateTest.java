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

import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.RollParams;

import com.nereusstream.kafka.checkpoint.KafkaCanonicalCheckpointState;
import com.nereusstream.kafka.checkpoint.KafkaDerivedIndexState;
import com.nereusstream.kafka.checkpoint.KafkaLeaderEpochState;
import com.nereusstream.kafka.checkpoint.KafkaProducerTransactionState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NereusCanonicalLogStateTest {

    @Test
    void rollsByLogicalSizeAndPrunesTheDurablyTrimmedPrefix() {
        LogConfig config = config(100, 64);
        NereusCanonicalLogState state = empty(config, 3);

        state.commitStable(List.of(batch(0, 60, 100)), 1_000);
        assertTrue(state.prepareRoll(
                0,
                new RollParams(
                        config.maxSegmentMs(),
                        config.segmentSize(),
                        200,
                        1,
                        50,
                        2_000)));
        state.stageRoll(1, 2_000);
        state.commitStable(List.of(batch(1, 50, 200)), 2_000);

        KafkaVirtualSegmentState virtual = state.virtualSegments();
        assertEquals(2, virtual.segments().size());
        assertEquals(
                KafkaVirtualSegmentState.RollReason.INITIAL,
                virtual.segments().get(0).rollReason());
        assertEquals(
                KafkaVirtualSegmentState.SegmentState.CLOSED,
                virtual.segments().get(0).state());
        assertEquals(
                KafkaVirtualSegmentState.RollReason.SIZE,
                virtual.segments().get(1).rollReason());
        assertEquals(110, virtual.segments().stream()
                .mapToLong(KafkaVirtualSegmentState.VirtualSegment::logicalBytes)
                .sum());
        assertEquals(new NereusCanonicalLogState.Position(1, 0), state.positionForOffset(1));

        state.advanceLogStart(1);

        assertEquals(1, state.virtualSegments().segments().size());
        assertEquals(1, state.virtualSegments().logStartOffset());
        assertEquals(50, state.virtualSegments().segments().get(0).logicalBytes());
    }

    @Test
    void buildsSparseIndexesAndForcesAConfigBoundary() {
        LogConfig first = config(1_024, 20);
        NereusCanonicalLogState state = empty(first, 7);

        state.commitStable(List.of(
                batch(0, 15, 100),
                batch(1, 15, 200),
                batch(2, 15, 300)), 1_000);

        KafkaDerivedIndexState indexes = state.derivedIndexes();
        assertEquals(
                List.of(new KafkaDerivedIndexState.LogicalByteSample(2, 30)),
                indexes.logicalByteIndexes().get(0).samples());
        assertEquals(
                List.of(new KafkaDerivedIndexState.TimeIndexEntry(200, 1)),
                indexes.timeIndexes().get(0).entries());

        LogConfig second = config(2_048, 20);
        state.updateConfig(second, 11);
        assertTrue(state.prepareRoll(
                0,
                new RollParams(
                        second.maxSegmentMs(),
                        second.segmentSize(),
                        400,
                        3,
                        15,
                        2_000)));
        state.stageRoll(3, 2_000);
        state.commitStable(List.of(batch(3, 15, 400)), 2_000);

        KafkaVirtualSegmentState virtual = state.virtualSegments();
        assertEquals(2, virtual.configHistory().size());
        assertEquals(11, virtual.configHistory().get(1).metadataOffset());
        assertEquals(
                KafkaVirtualSegmentState.RollReason.CONFIG,
                virtual.segments().get(1).rollReason());
        assertFalse(virtual.segments().get(0).configDigest()
                .equals(virtual.segments().get(1).configDigest()));
    }

    @Test
    void restoresAPreTrimCheckpointThenPrunesToTheCurrentDurableLogStart() {
        LogConfig config = config(100, 64);
        NereusCanonicalLogState captured = empty(config, 3);
        captured.commitStable(List.of(batch(0, 60, 100)), 1_000);
        captured.stageRoll(1, 2_000);
        captured.commitStable(List.of(batch(1, 60, 200)), 2_000);
        captured.stageRoll(2, 3_000);
        captured.commitStable(List.of(batch(2, 60, 300)), 3_000);
        KafkaCanonicalCheckpointState checkpoint =
                new KafkaCanonicalCheckpointState(
                        3,
                        0,
                        3,
                        new KafkaProducerTransactionState(
                                3,
                                List.of(),
                                List.of(),
                                List.of()),
                        new KafkaLeaderEpochState(
                                0,
                                3,
                                List.of(
                                        new KafkaLeaderEpochState.LeaderEpochRange(
                                                7,
                                                0))),
                        captured.virtualSegments(),
                        captured.derivedIndexes());
        NereusCanonicalLogState restored =
                new NereusCanonicalLogState("cluster/topic/0");

        restored.restore(
                2,
                3,
                Optional.of(checkpoint),
                List.of(),
                config,
                3,
                4_000);

        assertEquals(2, restored.virtualSegments().logStartOffset());
        assertEquals(
                List.of(2L),
                restored.virtualSegments().segments().stream()
                        .map(KafkaVirtualSegmentState.VirtualSegment::baseOffset)
                        .toList());
        assertEquals(List.of(2L), restored.segmentBaseOffsets(3));
    }

    private static NereusCanonicalLogState empty(
            LogConfig config,
            long metadataOffset
    ) {
        NereusCanonicalLogState state =
                new NereusCanonicalLogState("cluster/topic/0");
        state.restore(
                0,
                0,
                Optional.empty(),
                List.of(),
                config,
                metadataOffset,
                1_000);
        return state;
    }

    private static NereusCanonicalLogState.BatchObservation batch(
            long baseOffset,
            int bytes,
            long timestamp
    ) {
        return new NereusCanonicalLogState.BatchObservation(
                baseOffset,
                baseOffset + 1,
                bytes,
                timestamp,
                baseOffset);
    }

    private static LogConfig config(int segmentBytes, int indexIntervalBytes) {
        Properties properties = new Properties();
        properties.put(
                LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG,
                Integer.toString(segmentBytes));
        properties.put(
                "index.interval.bytes",
                Integer.toString(indexIntervalBytes));
        properties.put("segment.jitter.ms", "0");
        return new LogConfig(properties);
    }
}
