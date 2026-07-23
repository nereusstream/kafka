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
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;

import com.nereusstream.kafka.partition.KafkaTimestampAndOffset;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NereusRecordTimestampInspectorTest {
    private final NereusRecordTimestampInspector inspector = new NereusRecordTimestampInspector();

    @Test
    void findsTheFirstExactRecordAtOrAfterTheTargetWithoutMutatingTheBuffer() {
        ByteBuffer records = records().buffer();
        int position = records.position();
        int limit = records.limit();

        KafkaTimestampAndOffset result = inspector.firstAtOrAfter(records, 7, 1_500).orElseThrow();

        assertEquals(2_000, result.timestampMillis());
        assertEquals(8, result.offset());
        assertFalse(result.leaderEpoch().isPresent());
        assertEquals(position, records.position());
        assertEquals(limit, records.limit());
        assertTrue(inspector.firstAtOrAfter(records, 10, 1_500).isEmpty());
    }

    @Test
    void maximumUsesTheLowestOffsetWhenExactRecordTimestampsTie() {
        ByteBuffer records = records().buffer();

        KafkaTimestampAndOffset result = inspector.maximum(records, 7).orElseThrow();

        assertEquals(2_000, result.timestampMillis());
        assertEquals(8, result.offset());
        assertEquals(9, inspector.maximum(records, 9).orElseThrow().offset());
        assertTrue(inspector.maximum(records, 10).isEmpty());
    }

    private static MemoryRecords records() {
        return MemoryRecords.withRecords(
                (byte) 2,
                7,
                Compression.gzip().build(),
                TimestampType.CREATE_TIME,
                new SimpleRecord(1_000, "a".getBytes()),
                new SimpleRecord(2_000, "b".getBytes()),
                new SimpleRecord(2_000, "c".getBytes()));
    }
}
