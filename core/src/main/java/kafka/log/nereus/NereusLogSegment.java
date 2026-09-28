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
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.LazyIndex;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogFileUtils;
import org.apache.kafka.storage.internals.log.LogSegment;

import java.io.File;
import java.io.IOException;

/** Local position bookkeeping for stock append machinery; no RecordBatch is persisted here. */
final class NereusLogSegment extends LogSegment {
    private long logicalBytes;

    private NereusLogSegment(File dir, long offset, LogConfig config, Time time, NereusTransactionIndex transactions)
            throws IOException {
        super(
                FileRecords.open(LogFileUtils.logFile(dir, offset), false, 0, false),
                LazyIndex.forOffset(LogFileUtils.offsetIndexFile(dir, offset), offset, config.maxIndexSize),
                LazyIndex.forTime(LogFileUtils.timeIndexFile(dir, offset), offset, config.maxIndexSize),
                transactions,
                offset,
                config.indexInterval,
                config.randomSegmentJitter(),
                time);
    }

    static NereusLogSegment open(
            File dir, long offset, LogConfig config, Time time, NereusTransactionIndex transactions)
            throws IOException {
        return new NereusLogSegment(dir, offset, config, time, transactions);
    }

    void observe(MemoryRecords records) {
        logicalBytes = Math.addExact(logicalBytes, records.sizeInBytes());
    }

    @Override
    public int size() {
        return Math.toIntExact(Math.min(Integer.MAX_VALUE, logicalBytes));
    }
}
