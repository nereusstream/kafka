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
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.log.CleanedTransactionMetadata;
import org.apache.kafka.storage.internals.log.Cleaner;
import org.apache.kafka.storage.internals.log.CleanerStats;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.OffsetMap;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.internals.utils.Throttler;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.AbortedTransactionRange;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.MarkerDecision;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.Snapshot;
import com.nereusstream.kafka.compaction.KafkaCompactionRowMapper;
import com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1;
import com.nereusstream.kafka.compaction.KafkaCompactionTwoPassExecutor;
import com.nereusstream.kafka.compaction.KafkaCompactionTwoPassExecutor.Limits;
import com.nereusstream.kafka.compaction.KafkaTopicCompactionCodecV1;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.MarkerStatus.DELETE_ELIGIBLE;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.MarkerStatus.RETAIN_REQUIRED;
import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaCompactionOracleTest {
    private static final long NOW_MILLIS = 10_000L;
    private static final long DELETE_RETENTION_MILLIS = 1_000L;
    private static final int PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS = 600_000;
    private static final String SEGMENT_BYTES_CONFIG = "segment.bytes";
    private static final String DELETE_RETENTION_MS_CONFIG = "delete.retention.ms";
    private static final String CLEANUP_POLICY_CONFIG = "cleanup.policy";
    private static final String CLEANUP_POLICY_COMPACT = "compact";

    private final BrokerTopicStats brokerTopicStats = new BrokerTopicStats(false);
    private final MockTime time = new MockTime(NOW_MILLIS, 0);
    private final List<UnifiedLog> logs = new ArrayList<>();

    @TempDir
    Path root;

    @AfterEach
    void tearDown() {
        for (UnifiedLog log : logs) {
            log.close();
        }
        brokerTopicStats.close();
    }

    @Test
    void matchesStockCleanerForKeyedNullTombstoneCompressionAndNewerTailWinner()
            throws Exception {
        List<InputRecord> input =
            List.of(
                new InputRecord("a", "old-a", Compression.gzip().build()),
                new InputRecord(null, "unkeyed", Compression.NONE),
                new InputRecord("b", "old-b", Compression.gzip().build()),
                new InputRecord("b", "latest-b", Compression.NONE),
                new InputRecord("deleted", null, Compression.gzip().build()),
                new InputRecord("a", "tail-a", Compression.NONE),
                new InputRecord("tail-only", "tail", Compression.NONE));

        List<MemoryRecords> encoded = new ArrayList<>();
        for (int offset = 0; offset < input.size(); offset++) {
            InputRecord record = input.get(offset);
            encoded.add(
                MemoryRecords.withRecords(
                    offset,
                    record.compression(),
                    0,
                    new SimpleRecord(
                        1_000L + offset,
                        bytes(record.key()),
                        bytes(record.value()))));
        }

        OracleResult result =
            assertOracle(
                "key-null-tombstone-tail",
                encoded,
                5,
                snapshot(5, encoded.size()));

        assertEquals(List.of(3L, 4L), result.stockOffsets());
        assertEquals(result.stock(), result.nereus());
        assertEquals(
            OptionalLong.of(NOW_MILLIS + DELETE_RETENTION_MILLIS),
            result.nereus().get(1).deleteHorizonMillis());
    }

    @Test
    void matchesStockCleanerForCommittedAbortedAndControlRecords()
            throws Exception {
        List<MemoryRecords> encoded =
            List.of(
                MemoryRecords.withRecords(
                    0,
                    Compression.NONE,
                    0,
                    new SimpleRecord(2_000, bytes("x"), bytes("old-x"))),
                MemoryRecords.withTransactionalRecords(
                    1,
                    Compression.gzip().build(),
                    1,
                    (short) 0,
                    0,
                    0,
                    new SimpleRecord(2_001, bytes("a"), bytes("committed-a"))),
                MemoryRecords.withEndTransactionMarker(
                    2,
                    2_002,
                    0,
                    1,
                    (short) 0,
                    new EndTransactionMarker(ControlRecordType.COMMIT, 0)),
                MemoryRecords.withTransactionalRecords(
                    3,
                    Compression.gzip().build(),
                    2,
                    (short) 0,
                    0,
                    0,
                    new SimpleRecord(2_003, bytes("x"), bytes("aborted-x"))),
                MemoryRecords.withEndTransactionMarker(
                    4,
                    2_004,
                    0,
                    2,
                    (short) 0,
                    new EndTransactionMarker(ControlRecordType.ABORT, 0)),
                MemoryRecords.withRecords(
                    5,
                    Compression.NONE,
                    0,
                    new SimpleRecord(2_005, bytes("x"), bytes("tail-x"))));
        Snapshot snapshot =
            new Snapshot(
                new OffsetRange(0, 5),
                new OffsetRange(0, 6),
                6,
                NOW_MILLIS,
                DELETE_RETENTION_MILLIS,
                1 << 20,
                1 << 20,
                1 << 20,
                List.of(new AbortedTransactionRange(2, 3, 4)),
                List.of(),
                List.of(
                    new MarkerDecision(2, RETAIN_REQUIRED),
                    new MarkerDecision(4, RETAIN_REQUIRED)));

        OracleResult result =
            assertOracle("transactions-and-markers", encoded, 5, snapshot);

        assertEquals(List.of(1L, 2L, 4L), result.stockOffsets());
        assertEquals(result.stock(), result.nereus());
    }

    @Test
    void matchesStockCleanerWhenTombstoneAndEmptyAbortMarkerHorizonsExpire()
            throws Exception {
        List<MemoryRecords> encoded =
            List.of(
                KafkaCompactionOracleSupport.tombstoneWithDeleteHorizon(
                    0,
                    3_000,
                    bytes("deleted"),
                    NOW_MILLIS),
                KafkaCompactionOracleSupport.markerWithDeleteHorizon(
                    1,
                    3_001,
                    4,
                    (short) 0,
                    new EndTransactionMarker(ControlRecordType.ABORT, 0),
                    NOW_MILLIS),
                MemoryRecords.withRecords(
                    2,
                    Compression.NONE,
                    0,
                    new SimpleRecord(3_002, bytes("tail"), bytes("value"))));
        Snapshot snapshot =
            new Snapshot(
                new OffsetRange(0, 2),
                new OffsetRange(0, 3),
                3,
                NOW_MILLIS,
                DELETE_RETENTION_MILLIS,
                1 << 20,
                1 << 20,
                1 << 20,
                List.of(),
                List.of(),
                List.of(new MarkerDecision(1, DELETE_ELIGIBLE)));

        OracleResult result =
            assertOracle("expired-delete-horizons", encoded, 2, snapshot);

        assertEquals(List.of(), result.stockOffsets());
        assertEquals(result.stock(), result.nereus());
    }

    @Test
    void matchesStockCleanerWhenFilteringAnIdempotentBatch()
            throws Exception {
        List<MemoryRecords> encoded =
            List.of(
                MemoryRecords.withIdempotentRecords(
                    0,
                    Compression.gzip().build(),
                    9,
                    (short) 2,
                    100,
                    0,
                    new SimpleRecord(4_000, bytes("a"), bytes("old-a")),
                    new SimpleRecord(4_001, bytes("b"), bytes("keep-b"))),
                MemoryRecords.withRecords(
                    2,
                    Compression.NONE,
                    0,
                    new SimpleRecord(4_002, bytes("a"), bytes("tail-a"))));

        OracleResult result =
            assertOracle(
                "idempotent-batch",
                encoded,
                2,
                snapshot(2, 3));

        assertEquals(List.of(1L), result.stockOffsets());
        assertEquals(result.stock(), result.nereus());
    }

    private OracleResult assertOracle(
        String name,
        List<MemoryRecords> encoded,
        int outputEnd,
        Snapshot snapshot
    ) throws Exception {
        List<ReadBatch> batches = new ArrayList<>(encoded.size());
        for (int index = 0; index < encoded.size(); index++) {
            MemoryRecords records = encoded.get(index);
            RecordBatch batch = records.batches().iterator().next();
            batches.add(
                KafkaCompactionOracleSupport.readBatch(
                    new OffsetRange(batch.baseOffset(), batch.lastOffset() + 1),
                    bytes(records),
                    name + "-" + index));
        }

        UnifiedLog stock = createLog(name);
        for (int index = 0; index < batches.size(); index++) {
            stock.appendAsFollower(
                MemoryRecords.readableRecords(ByteBuffer.wrap(batches.get(index).payload())),
                0);
            if (batches.get(index).range().endOffset() == outputEnd) {
                stock.roll();
            }
        }
        stock.updateHighWatermark(stock.logEndOffset());
        List<LogSegment> segments = stock.logSegments(0, outputEnd);
        OffsetMap offsetMap = KafkaCompactionOracleSupport.offsetMap(10_000);
        CleanerStats stats = new CleanerStats(time);
        Cleaner cleaner =
            new Cleaner(
                0,
                offsetMap,
                1 << 20,
                1 << 20,
                0.75,
                new Throttler(
                    Double.MAX_VALUE,
                    300,
                    "nereus-compaction-oracle-" + UUID.randomUUID(),
                    "bytes",
                    time),
                time,
                ignored -> {
                });
        long horizonEnd = snapshot.decisionHorizon().endOffset();
        cleaner.buildOffsetMap(stock, 0, horizonEnd, offsetMap, stats);
        cleaner.cleanSegments(
            stock,
            segments,
            offsetMap,
            NOW_MILLIS,
            stats,
            new CleanedTransactionMetadata(),
            -1,
            horizonEnd);

        List<ReadBatch> outputBatches =
            batches.stream()
                .filter(batch -> batch.range().endOffset() <= outputEnd)
                .toList();
        KafkaCompactionTwoPassExecutor.Result nereus =
            new KafkaCompactionTwoPassExecutor(
                    new KafkaTopicCompactionCodecV1(),
                    new KafkaCompactionStrategyV1(),
                    new KafkaCompactionRowMapper(),
                    new Limits(100, 100, 16L << 20))
                .prepare(
                    snapshot,
                    KafkaCompactionOracleSupport.sourceSet(batches),
                    batches)
                .rewrite(
                    KafkaCompactionOracleSupport.sourceSet(outputBatches),
                    outputBatches,
                    false);

        List<RecordFingerprint> stockRecords = stockRecords(stock, outputEnd);
        List<RecordFingerprint> nereusRecords =
            nereus.rows().stream()
                .flatMap(
                    row ->
                        fingerprints(
                                MemoryRecords.readableRecords(row.exactPayload()).batches())
                            .stream())
                .sorted(Comparator.comparingLong(RecordFingerprint::offset))
                .toList();
        return new OracleResult(stockRecords, nereusRecords);
    }

    private Snapshot snapshot(long outputEnd, long horizonEnd) {
        return new Snapshot(
            new OffsetRange(0, outputEnd),
            new OffsetRange(0, horizonEnd),
            horizonEnd,
            NOW_MILLIS,
            DELETE_RETENTION_MILLIS,
            1 << 20,
            1 << 20,
            1 << 20,
            List.of(),
            List.of(),
            List.of());
    }

    private UnifiedLog createLog(String name) throws IOException {
        Properties properties = new Properties();
        properties.setProperty(SEGMENT_BYTES_CONFIG, Integer.toString(1 << 20));
        properties.setProperty(
            DELETE_RETENTION_MS_CONFIG,
            Long.toString(DELETE_RETENTION_MILLIS));
        properties.setProperty(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT);
        File directory =
            Files.createDirectory(root.resolve(name + "-0")).toFile();
        UnifiedLog log =
            UnifiedLog.create(
                directory,
                new LogConfig(properties),
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

    private static List<RecordFingerprint> stockRecords(
        UnifiedLog log,
        long outputEnd
    ) {
        return log.logSegments().stream()
            .flatMap(segment -> fingerprints(segment.log().batches()).stream())
            .filter(record -> record.offset() < outputEnd)
            .sorted(Comparator.comparingLong(RecordFingerprint::offset))
            .toList();
    }

    private static List<RecordFingerprint> fingerprints(
        Iterable<? extends RecordBatch> batches
    ) {
        ArrayList<RecordFingerprint> result = new ArrayList<>();
        for (RecordBatch batch : batches) {
            for (org.apache.kafka.common.record.Record record : batch) {
                result.add(
                    new RecordFingerprint(
                        record.offset(),
                        base64(record.key()),
                        base64(record.value()),
                        record.timestamp(),
                        batch.compressionType().name(),
                        batch.deleteHorizonMs(),
                        batch.isTransactional(),
                        batch.isControlBatch(),
                        batch.producerId(),
                        batch.producerEpoch(),
                        record.sequence(),
                        batch.partitionLeaderEpoch()));
            }
        }
        return List.copyOf(result);
    }

    private static byte[] bytes(MemoryRecords records) {
        ByteBuffer buffer = records.buffer().duplicate();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private static byte[] bytes(String value) {
        return value == null
            ? null
            : value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static String base64(ByteBuffer value) {
        if (value == null) {
            return null;
        }
        ByteBuffer copy = value.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    private record InputRecord(
        String key,
        String value,
        Compression compression
    ) {
    }

    private record RecordFingerprint(
        long offset,
        String key,
        String value,
        long timestamp,
        String compression,
        OptionalLong deleteHorizonMillis,
        boolean transactional,
        boolean control,
        long producerId,
        short producerEpoch,
        int sequence,
        int partitionLeaderEpoch
    ) {
    }

    private record OracleResult(
        List<RecordFingerprint> stock,
        List<RecordFingerprint> nereus
    ) {
        List<Long> stockOffsets() {
            return stock.stream().map(RecordFingerprint::offset).toList();
        }
    }

}
