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
import org.apache.kafka.storage.internals.log.AsyncOffsetReadFutureHolder;
import org.apache.kafka.storage.internals.log.LeaderEpochAwareOffsetLookup;
import org.apache.kafka.storage.internals.log.OffsetResultHolder;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Uses the current recovered shared prefix; stock delayed ListOffsets owns completion wakeups. */
public final class NereusListOffsetsBridge implements LeaderEpochAwareOffsetLookup {
    private final NereusUnifiedLog log;

    public NereusListOffsetsBridge(NereusUnifiedLog log) {
        this.log = java.util.Objects.requireNonNull(log);
    }

    @Override
    public OffsetResultHolder fetchOffsetByTimestamp(long timestamp, int expectedEpoch, Runnable wakeup) {
        var task = new CompletableFuture<OffsetResultHolder.FileRecordsOrError>();
        var job = new CompletableFuture<Void>();
        try {
            log.lookupTimestamp(timestamp, expectedEpoch).whenComplete((result, failure) -> {
                try {
                    task.complete(
                            failure == null
                                    ? new OffsetResultHolder.FileRecordsOrError(Optional.empty(), result)
                                    : failed(failure));
                } finally {
                    job.complete(null);
                    wakeup.run();
                }
            });
        } catch (Throwable failure) {
            task.complete(failed(failure));
            job.complete(null);
        }
        return new OffsetResultHolder(Optional.empty(), Optional.of(new AsyncOffsetReadFutureHolder<>(job, task)));
    }

    private static OffsetResultHolder.FileRecordsOrError failed(Throwable failure) {
        ApiException mapped = NereusKafkaExceptionMapper.map(failure);
        return new OffsetResultHolder.FileRecordsOrError(Optional.of(mapped), Optional.empty());
    }
}
