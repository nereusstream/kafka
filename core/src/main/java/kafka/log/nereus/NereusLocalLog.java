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
import java.util.Objects;

/**
 * Ephemeral local shell used only to satisfy stock UnifiedLog state machinery. Its segment files are cache artifacts,
 * never partition truth. Durable append/read overrides are owned by {@link NereusUnifiedLog}.
 */
public final class NereusLocalLog extends LocalLog {
    private StableAppend stableAppend;

    NereusLocalLog(
            File dir,
            LogConfig config,
            LogSegments segments,
            long recoveryPoint,
            LogOffsetMetadata nextOffsetMetadata,
            Scheduler scheduler,
            Time time,
            TopicPartition topicPartition,
            LogDirFailureChannel logDirFailureChannel
    ) {
        super(
                dir,
                config,
                segments,
                recoveryPoint,
                nextOffsetMetadata,
                scheduler,
                time,
                topicPartition,
                logDirFailureChannel);
    }

    void bindStableAppend(StableAppend exact) {
        Objects.requireNonNull(exact, "exact");
        if (stableAppend != null) {
            throw new IllegalStateException("Nereus LocalLog stable append is already bound");
        }
        stableAppend = exact;
    }

    @Override
    public void append(long lastOffset, MemoryRecords records) throws IOException {
        StableAppend exact = stableAppend;
        if (exact == null) {
            throw new IOException("Nereus LocalLog stable append is not bound");
        }
        exact.append(lastOffset, records);
        updateLogEndOffset(lastOffset + 1);
    }

    @FunctionalInterface
    interface StableAppend {
        void append(long lastOffset, MemoryRecords records);
    }
}
