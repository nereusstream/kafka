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

import com.nereusstream.kafka.partition.KafkaListOffsetQuery;
import com.nereusstream.kafka.partition.KafkaListOffsetsRequest;

import java.time.Duration;
import java.util.Objects;
import java.util.OptionalLong;

/** Immutable hard limits used to build one leader-fenced Nereus ListOffsets request. */
public record NereusListOffsetsScanConfig(
        long maxScanRecords,
        long maxScanBytes,
        int readTargetBytes,
        int hardMaxReadBytes,
        int maxReadOperations,
        Duration timeout
) {
    public NereusListOffsetsScanConfig {
        Objects.requireNonNull(timeout, "timeout");
        if (maxScanRecords <= 0
                || maxScanBytes <= 0
                || readTargetBytes <= 0
                || hardMaxReadBytes < readTargetBytes
                || maxReadOperations <= 0) {
            throw new IllegalArgumentException("invalid Nereus ListOffsets scan configuration");
        }
        if (timeout.isZero() || timeout.isNegative() || timeout.toMillis() <= 0) {
            throw new IllegalArgumentException("invalid Nereus ListOffsets scan configuration");
        }
    }

    KafkaListOffsetsRequest request(
            KafkaListOffsetQuery query,
            OptionalLong targetTimestampMillis,
            int expectedLeaderEpoch
    ) {
        return new KafkaListOffsetsRequest(
                query,
                targetTimestampMillis,
                expectedLeaderEpoch,
                maxScanRecords,
                maxScanBytes,
                readTargetBytes,
                hardMaxReadBytes,
                maxReadOperations,
                timeout);
    }
}
