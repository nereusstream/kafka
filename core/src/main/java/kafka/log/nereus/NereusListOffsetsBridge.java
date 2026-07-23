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

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.storage.internals.log.AsyncOffsetReadFutureHolder;
import org.apache.kafka.storage.internals.log.OffsetResultHolder;

import com.nereusstream.kafka.partition.KafkaListOffsetQuery;
import com.nereusstream.kafka.partition.KafkaListOffsetResult;
import com.nereusstream.kafka.partition.KafkaListOffsetsRequest;
import com.nereusstream.kafka.partition.KafkaListOffsetsResolver;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/** Adapts asynchronous exact Nereus ListOffsets results to Kafka's existing delayed-offset holder contract. */
public final class NereusListOffsetsBridge {
    @FunctionalInterface
    interface Lookup {
        CompletableFuture<Optional<KafkaListOffsetResult>> resolve(KafkaListOffsetsRequest request);
    }

    private final Lookup lookup;
    private final NereusListOffsetsScanConfig config;

    public NereusListOffsetsBridge(
            KafkaListOffsetsResolver resolver,
            NereusListOffsetsScanConfig config
    ) {
        this(Objects.requireNonNull(resolver, "resolver")::resolve, config);
    }

    NereusListOffsetsBridge(Lookup lookup, NereusListOffsetsScanConfig config) {
        this.lookup = Objects.requireNonNull(lookup, "lookup");
        this.config = Objects.requireNonNull(config, "config");
    }

    public OffsetResultHolder fetchOffsetByTimestamp(
            long targetTimestamp,
            int expectedLeaderEpoch,
            Runnable completionWakeup
    ) {
        Objects.requireNonNull(completionWakeup, "completionWakeup");
        KafkaListOffsetQuery query = query(targetTimestamp);
        OptionalLong target = query == KafkaListOffsetQuery.TIMESTAMP
                ? OptionalLong.of(targetTimestamp)
                : OptionalLong.empty();
        CompletableFuture<Optional<KafkaListOffsetResult>> result;
        try {
            result = Objects.requireNonNull(
                    lookup.resolve(config.request(query, target, expectedLeaderEpoch)),
                    "Nereus ListOffsets lookup returned a null future");
        } catch (Throwable failure) {
            throw NereusKafkaExceptionMapper.map(failure);
        }

        if (result.isDone()) {
            try {
                return new OffsetResultHolder(result.join().map(NereusListOffsetsBridge::toKafka));
            } catch (Throwable failure) {
                throw NereusKafkaExceptionMapper.map(failure);
            }
        }
        return async(result, completionWakeup);
    }

    private static OffsetResultHolder async(
            CompletableFuture<Optional<KafkaListOffsetResult>> result,
            Runnable completionWakeup
    ) {
        CompletableFuture<Void> jobFuture = new CompletableFuture<>();
        CompletableFuture<OffsetResultHolder.FileRecordsOrError> taskFuture = new CompletableFuture<>();
        jobFuture.whenComplete((ignored, failure) -> {
            if (jobFuture.isCancelled()) {
                result.cancel(false);
            }
        });
        result.whenComplete((value, failure) -> {
            try {
                if (failure == null) {
                    Optional<FileRecords.TimestampAndOffset> timestampAndOffset = Objects.requireNonNull(
                            value, "Nereus ListOffsets lookup returned a null result")
                            .map(NereusListOffsetsBridge::toKafka);
                    taskFuture.complete(new OffsetResultHolder.FileRecordsOrError(
                            Optional.empty(), timestampAndOffset));
                } else {
                    taskFuture.complete(failure(failure));
                }
            } catch (Throwable conversionFailure) {
                taskFuture.complete(failure(conversionFailure));
            } finally {
                jobFuture.complete(null);
                try {
                    completionWakeup.run();
                } catch (Throwable ignored) {
                    // The completed task remains observable by the delayed-operation polling path.
                }
            }
        });
        AsyncOffsetReadFutureHolder<OffsetResultHolder.FileRecordsOrError> async =
                new AsyncOffsetReadFutureHolder<>(jobFuture, taskFuture);
        return new OffsetResultHolder(Optional.empty(), Optional.of(async));
    }

    private static OffsetResultHolder.FileRecordsOrError failure(Throwable failure) {
        ApiException mapped = NereusKafkaExceptionMapper.map(failure);
        return new OffsetResultHolder.FileRecordsOrError(
                Optional.of(mapped), Optional.empty());
    }

    private static FileRecords.TimestampAndOffset toKafka(KafkaListOffsetResult result) {
        long timestamp = result.timestampMillis().orElse(RecordBatch.NO_TIMESTAMP);
        Optional<Integer> leaderEpoch = result.leaderEpoch().isPresent()
                ? Optional.of(result.leaderEpoch().orElseThrow())
                : Optional.empty();
        return new FileRecords.TimestampAndOffset(timestamp, result.offset(), leaderEpoch);
    }

    private static KafkaListOffsetQuery query(long targetTimestamp) {
        if (targetTimestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP) {
            return KafkaListOffsetQuery.EARLIEST;
        }
        if (targetTimestamp == ListOffsetsRequest.LATEST_TIMESTAMP) {
            return KafkaListOffsetQuery.LATEST;
        }
        if (targetTimestamp == ListOffsetsRequest.MAX_TIMESTAMP) {
            return KafkaListOffsetQuery.MAX_TIMESTAMP;
        }
        if (targetTimestamp >= 0) {
            return KafkaListOffsetQuery.TIMESTAMP;
        }
        throw Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT.exception(
                "Nereus storage does not expose local/tiered ListOffsets sentinels");
    }
}
