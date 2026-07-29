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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.LogConfigHistoryEntry;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.RollReason;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.SegmentState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.VirtualSegment;
import com.nereusstream.kafka.retention.KafkaRetentionPlanner;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaRetentionOracleTest {
    private static final long NOW_MILLIS = 10_000L;
    private static final int MIN_SEGMENT_BYTES = 1_048_576;
    private static final int PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS = 600_000;
    private static final String SEGMENT_BYTES_CONFIG = "segment.bytes";
    private static final String RETENTION_BYTES_CONFIG = "retention.bytes";
    private static final String RETENTION_MS_CONFIG = "retention.ms";
    private static final String CLEANUP_POLICY_CONFIG = "cleanup.policy";
    private static final String CLEANUP_POLICY_DELETE = "delete";
    private static final String CLEANUP_POLICY_COMPACT = "compact";
    private static final long[] RECORD_TIMESTAMPS = {
        1_000L,
        1_000L,
        2_000L,
        2_000L,
        9_000L,
        9_000L,
        10_000L,
        10_000L
    };

    private final BrokerTopicStats brokerTopicStats = new BrokerTopicStats(false);
    private final MockTime time = new MockTime(NOW_MILLIS, 0);
    private final List<UnifiedLog> logs = new ArrayList<>();

    @TempDir
    Path root;

    @AfterEach
    public void tearDown() {
        for (UnifiedLog log : logs) {
            log.close();
        }
        brokerTopicStats.close();
    }

    @Test
    public void testNereusRetentionPlannerMatchesStockUnifiedLogOracle() throws Exception {
        int recordBytes = records(1_000L).sizeInBytes();
        long totalBytes = Math.multiplyExact(recordBytes, RECORD_TIMESTAMPS.length);

        assertOracle(
            new OracleCase(
                "time",
                -1,
                5_000,
                CLEANUP_POLICY_DELETE,
                RECORD_TIMESTAMPS.length,
                4));
        assertOracle(
            new OracleCase(
                "size",
                Math.multiplyExact(recordBytes, 4L),
                -1,
                CLEANUP_POLICY_DELETE,
                RECORD_TIMESTAMPS.length,
                4));
        assertOracle(
            new OracleCase(
                "combined",
                Math.subtractExact(totalBytes, Math.multiplyExact(recordBytes, 2L)),
                5_000,
                CLEANUP_POLICY_DELETE,
                RECORD_TIMESTAMPS.length,
                4));
        assertOracle(
            new OracleCase(
                "high-watermark",
                -1,
                0,
                CLEANUP_POLICY_DELETE,
                3,
                2));
        assertOracle(
            new OracleCase(
                "strict-time-equality",
                -1,
                9_000,
                CLEANUP_POLICY_DELETE,
                RECORD_TIMESTAMPS.length,
                0));
        assertOracle(
            new OracleCase(
                "compact-only",
                0,
                0,
                CLEANUP_POLICY_COMPACT,
                RECORD_TIMESTAMPS.length,
                0));
    }

    private void assertOracle(OracleCase oracle) throws Exception {
        Properties properties = new Properties();
        properties.setProperty(
            SEGMENT_BYTES_CONFIG,
            Integer.toString(MIN_SEGMENT_BYTES));
        properties.setProperty(
            RETENTION_BYTES_CONFIG,
            Long.toString(oracle.retentionBytes()));
        properties.setProperty(
            RETENTION_MS_CONFIG,
            Long.toString(oracle.retentionMs()));
        properties.setProperty(
            CLEANUP_POLICY_CONFIG,
            oracle.cleanupPolicy());
        LogConfig config = new LogConfig(properties);
        UnifiedLog stock = createLog(oracle.name(), config);
        for (int index = 0; index < RECORD_TIMESTAMPS.length; index++) {
            stock.appendAsLeader(records(RECORD_TIMESTAMPS[index]), 0);
            if (index < RECORD_TIMESTAMPS.length - 2 && index % 2 == 1) {
                stock.roll();
            }
        }
        assertEquals(4, stock.numberOfSegments(), oracle.name());
        stock.updateHighWatermark(oracle.highWatermark());

        KafkaRetentionPlanner.Snapshot snapshot =
            snapshot(stock, config, oracle.highWatermark());
        KafkaRetentionPlanner.Plan planned =
            new KafkaRetentionPlanner().plan(snapshot);
        assertEquals(
            oracle.expectedLogStartOffset(),
            planned.candidateLogStartOffset(),
            oracle.name() + " expected product boundary");

        int deleted = stock.deleteOldSegments();
        assertEquals(
            planned.selectedSegmentCount(),
            deleted,
            oracle.name() + " deleted segment count");
        assertEquals(
            planned.candidateLogStartOffset(),
            stock.logStartOffset(),
            oracle.name() + " stock/product log-start boundary");
    }

    private KafkaRetentionPlanner.Snapshot snapshot(
        UnifiedLog stock,
        LogConfig config,
        long highWatermark
    ) throws IOException {
        int cleanupFlags = config.delete
            ? LogConfigHistoryEntry.CLEANUP_DELETE_FLAG
            : LogConfigHistoryEntry.CLEANUP_COMPACT_FLAG;
        if (config.delete && config.compact) {
            cleanupFlags =
                LogConfigHistoryEntry.CLEANUP_DELETE_FLAG
                    | LogConfigHistoryEntry.CLEANUP_COMPACT_FLAG;
        }
        LogConfigHistoryEntry history =
            LogConfigHistoryEntry.create(
                7,
                0,
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
                cleanupFlags);
        List<LogSegment> stockSegments =
            stock.logSegments().stream()
                .sorted(Comparator.comparingLong(LogSegment::baseOffset))
                .toList();
        List<VirtualSegment> virtualSegments =
            new ArrayList<>(stockSegments.size());
        long cumulativeBytes = 0;
        for (int index = 0; index < stockSegments.size(); index++) {
            LogSegment segment = stockSegments.get(index);
            long endOffset =
                index + 1 < stockSegments.size()
                    ? stockSegments.get(index + 1).baseOffset()
                    : stock.logEndOffset();
            long firstBytes = cumulativeBytes;
            cumulativeBytes = Math.addExact(cumulativeBytes, segment.size());
            boolean active = index + 1 == stockSegments.size();
            long createdAt = index * 10L + 1;
            virtualSegments.add(
                new VirtualSegment(
                    segment.baseOffset(),
                    endOffset,
                    index,
                    createdAt,
                    active ? 0 : createdAt + 1,
                    0,
                    segment.largestTimestamp(),
                    endOffset - 1,
                    segment.size(),
                    firstBytes,
                    cumulativeBytes,
                    history.configDigest(),
                    index == 0 ? RollReason.INITIAL : RollReason.SIZE,
                    active ? SegmentState.ACTIVE : SegmentState.CLOSED));
        }
        KafkaVirtualSegmentState state =
            new KafkaVirtualSegmentState(
                stock.logStartOffset(),
                stock.logEndOffset(),
                virtualSegments,
                List.of(history));
        return new KafkaRetentionPlanner.Snapshot(
            state,
            KafkaRetentionPlanner.Policy.from(history),
            highWatermark,
            highWatermark,
            time.milliseconds());
    }

    private UnifiedLog createLog(String name, LogConfig config) throws IOException {
        File directory =
            Files.createDirectory(root.resolve(name + "-0")).toFile();
        UnifiedLog log =
            UnifiedLog.create(
                directory,
                config,
                0L,
                0L,
                time.scheduler,
                brokerTopicStats,
                time,
                3_600_000,
                new ProducerStateManagerConfig(3_600_000, false),
                PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS,
                new LogDirFailureChannel(10),
                true,
                Optional.<Uuid>empty(),
                new ConcurrentHashMap<>(),
                false,
                LogOffsetsListener.NO_OP_OFFSETS_LISTENER);
        logs.add(log);
        return log;
    }

    private static MemoryRecords records(long timestamp) {
        return MemoryRecords.withRecords(
            Compression.NONE,
            new SimpleRecord(
                timestamp,
                "retention-key".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                "retention-value".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    private record OracleCase(
        String name,
        long retentionBytes,
        long retentionMs,
        String cleanupPolicy,
        long highWatermark,
        long expectedLogStartOffset
    ) {
    }
}
