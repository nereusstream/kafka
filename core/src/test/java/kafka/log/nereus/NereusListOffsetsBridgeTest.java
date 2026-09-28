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
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ListOffsetsRequest;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadBatchV1;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NereusListOffsetsBridgeTest {
    @Test
    void timestampScanUsesCapturedPrefixAndRejectsIncompleteNoMatch() {
        var prefix = batch(0, 10, 20);
        var later = batch(2, 30);
        var limits = new NereusListOffsetsScanConfig(10, 1024, Duration.ofSeconds(1));
        assertEquals(
                1,
                NereusUnifiedLog.scanTimestamp(List.of(prefix, later), 2, ListOffsetsRequest.MAX_TIMESTAMP, limits)
                        .orElseThrow()
                        .offset);
        assertEquals(
                1,
                NereusUnifiedLog.scanTimestamp(List.of(prefix), 2, 15, limits).orElseThrow().offset);
        assertEquals(java.util.Optional.empty(), NereusUnifiedLog.scanTimestamp(List.of(prefix), 2, 25, limits));
        assertThrows(
                ThrottlingQuotaExceededException.class,
                () -> NereusUnifiedLog.scanTimestamp(List.of(prefix), 3, 25, limits));
        assertThrows(
                ThrottlingQuotaExceededException.class,
                () -> NereusUnifiedLog.scanTimestamp(
                        List.of(prefix),
                        2,
                        ListOffsetsRequest.MAX_TIMESTAMP,
                        new NereusListOffsetsScanConfig(1, 1024, Duration.ofSeconds(1))));
        assertThrows(
                ThrottlingQuotaExceededException.class,
                () -> NereusUnifiedLog.scanTimestamp(
                        List.of(prefix),
                        2,
                        15,
                        new NereusListOffsetsScanConfig(
                                10, prefix.rawAssignedRecordBatch().length() - 1, Duration.ofSeconds(1))));
    }

    private static KafkaBookKeeperReadBatchV1 batch(long baseOffset, long... timestamps) {
        SimpleRecord[] input = new SimpleRecord[timestamps.length];
        for (int i = 0; i < timestamps.length; i++) input[i] = new SimpleRecord(timestamps[i], new byte[] {1});
        var records = MemoryRecords.withRecords(baseOffset, Compression.NONE, input);
        byte[] bytes = new byte[records.sizeInBytes()];
        records.buffer().duplicate().get(bytes);
        return new KafkaBookKeeperReadBatchV1(
                baseOffset, baseOffset + timestamps.length, 1, CanonicalBytes.copyOf(bytes));
    }
}
