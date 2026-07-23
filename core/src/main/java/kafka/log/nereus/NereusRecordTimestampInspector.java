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

import com.nereusstream.kafka.partition.KafkaRecordTimestampInspector;
import com.nereusstream.kafka.partition.KafkaTimestampAndOffset;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/** Uses the locked stock Kafka record iterator to answer exact Nereus timestamp queries. */
public final class NereusRecordTimestampInspector implements KafkaRecordTimestampInspector {
    @Override
    public Optional<KafkaTimestampAndOffset> firstAtOrAfter(
            ByteBuffer exactRecords,
            long minimumOffset,
            long targetTimestampMillis
    ) {
        validate(exactRecords, minimumOffset, targetTimestampMillis);
        for (RecordBatch batch : records(exactRecords).batches()) {
            for (Record record : batch) {
                if (record.offset() >= minimumOffset && record.timestamp() >= targetTimestampMillis) {
                    return Optional.of(result(batch, record));
                }
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<KafkaTimestampAndOffset> maximum(ByteBuffer exactRecords, long minimumOffset) {
        Objects.requireNonNull(exactRecords, "exactRecords");
        if (minimumOffset < 0) {
            throw new IllegalArgumentException("minimumOffset must be non-negative");
        }
        KafkaTimestampAndOffset maximum = null;
        for (RecordBatch batch : records(exactRecords).batches()) {
            for (Record record : batch) {
                if (record.offset() < minimumOffset || record.timestamp() < 0) {
                    continue;
                }
                KafkaTimestampAndOffset candidate = result(batch, record);
                if (maximum == null
                        || candidate.timestampMillis() > maximum.timestampMillis()
                        || (candidate.timestampMillis() == maximum.timestampMillis()
                        && candidate.offset() < maximum.offset())) {
                    maximum = candidate;
                }
            }
        }
        return Optional.ofNullable(maximum);
    }

    private static MemoryRecords records(ByteBuffer exactRecords) {
        return MemoryRecords.readableRecords(exactRecords.duplicate());
    }

    private static KafkaTimestampAndOffset result(RecordBatch batch, Record record) {
        OptionalInt leaderEpoch = batch.partitionLeaderEpoch() >= 0
                ? OptionalInt.of(batch.partitionLeaderEpoch())
                : OptionalInt.empty();
        return new KafkaTimestampAndOffset(record.timestamp(), record.offset(), leaderEpoch);
    }

    private static void validate(ByteBuffer exactRecords, long minimumOffset, long targetTimestampMillis) {
        Objects.requireNonNull(exactRecords, "exactRecords");
        if (minimumOffset < 0 || targetTimestampMillis < 0) {
            throw new IllegalArgumentException("offset and timestamp bounds must be non-negative");
        }
    }
}
