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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.log.LocalLog;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogSegments;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/** Disposable native log machinery; authoritative bytes and successful appends belong to shared BK. */
public final class NereusLocalLog extends LocalLog {
    private final NereusTransactionIndex transactionIndex;
    private StableAppend stableAppend;

    NereusLocalLog(
            File dir,
            LogConfig config,
            LogSegments segments,
            Scheduler scheduler,
            Time time,
            TopicPartition topicPartition,
            LogDirFailureChannel failures,
            NereusTransactionIndex transactions) {
        super(dir, config, segments, 0, new LogOffsetMetadata(0), scheduler, time, topicPartition, failures);
        transactionIndex = transactions;
    }

    void bindStableAppend(StableAppend callback) {
        if (stableAppend != null) throw new IllegalStateException("shared BK append is already bound");
        stableAppend = Objects.requireNonNull(callback, "callback");
    }

    @Override
    public void append(long lastOffset, MemoryRecords records) throws IOException {
        if (stableAppend == null) throw new IOException("shared BK append is not bound");
        stableAppend.append(lastOffset, records);
        ((NereusLogSegment) segments().activeSegment()).observe(records);
        updateLogEndOffset(Math.addExact(lastOffset, 1));
    }

    @Override
    public org.apache.kafka.storage.internals.log.LogSegment roll(Long expectedNextOffset) {
        long next = Math.max(Objects.requireNonNull(expectedNextOffset), logEndOffset());
        if (segments().activeSegment().baseOffset() == next) return segments().activeSegment();
        try {
            segments().activeSegment().onBecomeInactiveSegment();
            var segment = NereusLogSegment.open(dir(), next, config(), time(), transactionIndex);
            segments().add(segment);
            updateLogEndOffset(logEndOffset());
            return segment;
        } catch (IOException failure) {
            throw new org.apache.kafka.common.errors.KafkaStorageException(
                    "shared BK local shell roll failed", failure);
        }
    }

    @Override
    public List<org.apache.kafka.storage.internals.log.LogSegment> truncateFullyAndStartAt(long offset) {
        transactionIndex.reset();
        updateLogEndOffset(offset);
        return List.of();
    }

    @FunctionalInterface
    interface StableAppend {
        void append(long lastOffset, MemoryRecords records);
    }
}
