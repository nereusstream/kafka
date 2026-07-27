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

import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.LazyIndex;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogFileUtils;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.RollParams;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

/**
 * Ephemeral stock segment facade backed by canonical Nereus virtual-segment facts.
 *
 * <p>Its files and stock indexes are compatibility shells only. Size and roll decisions come from
 * the partition-lock-owned canonical state.
 */
final class NereusLogSegment extends LogSegment {
    private final NereusCanonicalLogState canonicalState;

    private NereusLogSegment(
            FileRecords records,
            LazyIndex<org.apache.kafka.storage.internals.log.OffsetIndex> offsetIndex,
            LazyIndex<org.apache.kafka.storage.internals.log.TimeIndex> timeIndex,
            NereusTransactionIndex transactionIndex,
            long baseOffset,
            int indexIntervalBytes,
            long rollJitterMillis,
            Time time,
            NereusCanonicalLogState canonicalState
    ) {
        super(
                records,
                offsetIndex,
                timeIndex,
                transactionIndex,
                baseOffset,
                indexIntervalBytes,
                rollJitterMillis,
                time);
        this.canonicalState = Objects.requireNonNull(canonicalState, "canonicalState");
    }

    static NereusLogSegment open(
            File directory,
            long baseOffset,
            LogConfig config,
            Time time,
            NereusTransactionIndex transactionIndex,
            NereusCanonicalLogState canonicalState
    ) throws IOException {
        long rollJitterMillis = canonicalState.segmentRollJitter(baseOffset);
        return new NereusLogSegment(
                FileRecords.open(
                        LogFileUtils.logFile(directory, baseOffset),
                        false,
                        config.initFileSize(),
                        config.preallocate),
                LazyIndex.forOffset(
                        LogFileUtils.offsetIndexFile(directory, baseOffset),
                        baseOffset,
                        config.maxIndexSize),
                LazyIndex.forTime(
                        LogFileUtils.timeIndexFile(directory, baseOffset),
                        baseOffset,
                        config.maxIndexSize),
                transactionIndex,
                baseOffset,
                config.indexInterval,
                rollJitterMillis,
                time,
                canonicalState);
    }

    @Override
    public int size() {
        return Math.toIntExact(Math.min(
                Integer.MAX_VALUE,
                canonicalState.segmentLogicalBytes(baseOffset())));
    }

    @Override
    public long rollJitterMs() {
        return canonicalState.segmentRollJitter(baseOffset());
    }

    @Override
    public boolean shouldRoll(RollParams rollParams) {
        return canonicalState.prepareRoll(baseOffset(), rollParams);
    }
}
