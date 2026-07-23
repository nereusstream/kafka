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

import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.storage.internals.log.AsyncOffsetReadFutureHolder;
import org.apache.kafka.storage.internals.log.OffsetResultHolder;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.partition.KafkaListOffsetQuery;
import com.nereusstream.kafka.partition.KafkaListOffsetResult;
import com.nereusstream.kafka.partition.KafkaListOffsetsRequest;
import com.nereusstream.kafka.partition.KafkaStableSnapshot;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NereusListOffsetsBridgeTest {
    private static final KafkaStableSnapshot SNAPSHOT = KafkaStableSnapshot.nonTransactional(10, 20, 3);
    private static final NereusListOffsetsScanConfig CONFIG = new NereusListOffsetsScanConfig(
            100, 1_048_576, 4_096, 1_048_576, 10, Duration.ofSeconds(5));

    @Test
    void returnsCompletedSentinelsImmediatelyAndPassesExactBounds() {
        AtomicReference<KafkaListOffsetsRequest> captured = new AtomicReference<>();
        NereusListOffsetsBridge bridge = new NereusListOffsetsBridge(request -> {
            captured.set(request);
            return CompletableFuture.completedFuture(Optional.of(result(request.query())));
        }, CONFIG);

        OffsetResultHolder holder = bridge.fetchOffsetByTimestamp(
                ListOffsetsRequest.EARLIEST_TIMESTAMP, 7, () -> { });

        FileRecords.TimestampAndOffset value = holder.timestampAndOffsetOpt().orElseThrow();
        assertEquals(RecordBatch.NO_TIMESTAMP, value.timestamp);
        assertEquals(10, value.offset);
        assertEquals(Optional.of(7), value.leaderEpoch);
        assertTrue(holder.futureHolderOpt().isEmpty());
        assertEquals(KafkaListOffsetQuery.EARLIEST, captured.get().query());
        assertEquals(OptionalLong.empty(), captured.get().targetTimestampMillis());
        assertEquals(7, captured.get().expectedLeaderEpoch());
        assertEquals(CONFIG.maxScanRecords(), captured.get().maxScanRecords());
        assertEquals(CONFIG.maxScanBytes(), captured.get().maxScanBytes());
        assertEquals(CONFIG.readTargetBytes(), captured.get().readTargetBytes());
        assertEquals(CONFIG.hardMaxReadBytes(), captured.get().hardMaxReadBytes());
        assertEquals(CONFIG.maxReadOperations(), captured.get().maxReadOperations());
        assertEquals(CONFIG.timeout(), captured.get().timeout());
    }

    @Test
    void exposesAsyncTimestampResultAndWakesDelayedOperationOnce() {
        CompletableFuture<Optional<KafkaListOffsetResult>> source = new CompletableFuture<>();
        AtomicReference<KafkaListOffsetsRequest> captured = new AtomicReference<>();
        AtomicInteger wakeups = new AtomicInteger();
        NereusListOffsetsBridge bridge = new NereusListOffsetsBridge(request -> {
            captured.set(request);
            return source;
        }, CONFIG);

        OffsetResultHolder holder = bridge.fetchOffsetByTimestamp(1_500, 7, wakeups::incrementAndGet);
        AsyncOffsetReadFutureHolder<OffsetResultHolder.FileRecordsOrError> future =
                holder.futureHolderOpt().orElseThrow();

        assertTrue(holder.timestampAndOffsetOpt().isEmpty());
        assertFalse(future.taskFuture().isDone());
        assertEquals(KafkaListOffsetQuery.TIMESTAMP, captured.get().query());
        assertEquals(OptionalLong.of(1_500), captured.get().targetTimestampMillis());

        source.complete(Optional.of(new KafkaListOffsetResult(
                KafkaListOffsetQuery.TIMESTAMP,
                OptionalLong.of(2_000),
                12,
                OptionalInt.of(7),
                SNAPSHOT)));

        OffsetResultHolder.FileRecordsOrError completed = future.taskFuture().join();
        FileRecords.TimestampAndOffset value = completed.timestampAndOffset().orElseThrow();
        assertTrue(completed.exception().isEmpty());
        assertEquals(2_000, value.timestamp);
        assertEquals(12, value.offset);
        assertEquals(Optional.of(7), value.leaderEpoch);
        assertTrue(future.jobFuture().isDone());
        assertEquals(1, wakeups.get());
    }

    @Test
    void exposesEmptyAndFailedAsyncResultsAsTerminalKafkaResults() {
        CompletableFuture<Optional<KafkaListOffsetResult>> emptySource = new CompletableFuture<>();
        AtomicInteger emptyWakeups = new AtomicInteger();
        OffsetResultHolder emptyHolder = bridge(emptySource).fetchOffsetByTimestamp(
                ListOffsetsRequest.MAX_TIMESTAMP, 7, emptyWakeups::incrementAndGet);

        emptySource.complete(Optional.empty());

        OffsetResultHolder.FileRecordsOrError empty = emptyHolder.futureHolderOpt()
                .orElseThrow().taskFuture().join();
        assertTrue(empty.exception().isEmpty());
        assertTrue(empty.timestampAndOffset().isEmpty());
        assertEquals(1, emptyWakeups.get());

        CompletableFuture<Optional<KafkaListOffsetResult>> failedSource = new CompletableFuture<>();
        AtomicInteger failedWakeups = new AtomicInteger();
        OffsetResultHolder failedHolder = bridge(failedSource).fetchOffsetByTimestamp(
                ListOffsetsRequest.LATEST_TIMESTAMP, 7, failedWakeups::incrementAndGet);

        failedSource.completeExceptionally(new NereusException(ErrorCode.TIMEOUT, true, "timed out"));

        OffsetResultHolder.FileRecordsOrError failed = failedHolder.futureHolderOpt()
                .orElseThrow().taskFuture().join();
        assertInstanceOf(TimeoutException.class, failed.exception().orElseThrow());
        assertTrue(failed.timestampAndOffset().isEmpty());
        assertEquals(1, failedWakeups.get());
    }

    @Test
    void completesTaskEvenWhenLookupViolatesItsNullResultContract() {
        CompletableFuture<Optional<KafkaListOffsetResult>> source = new CompletableFuture<>();
        AtomicInteger wakeups = new AtomicInteger();
        OffsetResultHolder holder = bridge(source).fetchOffsetByTimestamp(
                ListOffsetsRequest.LATEST_TIMESTAMP, 7, wakeups::incrementAndGet);

        source.complete(null);

        OffsetResultHolder.FileRecordsOrError completed = holder.futureHolderOpt()
                .orElseThrow().taskFuture().join();
        assertInstanceOf(KafkaStorageException.class, completed.exception().orElseThrow());
        assertTrue(completed.timestampAndOffset().isEmpty());
        assertEquals(1, wakeups.get());
    }

    @Test
    void cancellingKafkaJobCancelsNereusLookupAndWakesCompletionPath() {
        CompletableFuture<Optional<KafkaListOffsetResult>> source = new CompletableFuture<>();
        AtomicInteger wakeups = new AtomicInteger();
        AsyncOffsetReadFutureHolder<OffsetResultHolder.FileRecordsOrError> future = bridge(source)
                .fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, 7, wakeups::incrementAndGet)
                .futureHolderOpt().orElseThrow();

        assertTrue(future.jobFuture().cancel(false));

        assertTrue(source.isCancelled());
        assertInstanceOf(TimeoutException.class,
                future.taskFuture().join().exception().orElseThrow());
        assertEquals(1, wakeups.get());
    }

    @Test
    void rejectsKafkaSentinelsThatNereusDoesNotSupport() {
        NereusListOffsetsBridge bridge = new NereusListOffsetsBridge(
                request -> CompletableFuture.completedFuture(Optional.empty()), CONFIG);

        assertThrows(UnsupportedForMessageFormatException.class, () ->
                bridge.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, 7, () -> { }));
        assertThrows(UnsupportedForMessageFormatException.class, () ->
                bridge.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIERED_TIMESTAMP, 7, () -> { }));
        assertThrows(UnsupportedForMessageFormatException.class, () ->
                bridge.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP, 7, () -> { }));
    }

    private static NereusListOffsetsBridge bridge(
            CompletableFuture<Optional<KafkaListOffsetResult>> source
    ) {
        return new NereusListOffsetsBridge(request -> source, CONFIG);
    }

    private static KafkaListOffsetResult result(KafkaListOffsetQuery query) {
        long offset = query == KafkaListOffsetQuery.EARLIEST ? 10 : 20;
        OptionalLong timestamp = query == KafkaListOffsetQuery.TIMESTAMP
                || query == KafkaListOffsetQuery.MAX_TIMESTAMP
                ? OptionalLong.of(2_000)
                : OptionalLong.empty();
        if (timestamp.isPresent() && offset == SNAPSHOT.stableEndOffset()) {
            offset--;
        }
        return new KafkaListOffsetResult(query, timestamp, offset, OptionalInt.of(7), SNAPSHOT);
    }
}
