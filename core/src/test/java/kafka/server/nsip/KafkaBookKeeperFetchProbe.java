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
package kafka.server.nsip;

import kafka.server.storage.BrokerStorageFetchExecutor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.storage.internals.log.LogReadResult;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import scala.Function1;
import scala.Tuple2;
import scala.collection.Seq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A single Fetch request remains pending at LSO until the transaction commits. */
final class KafkaBookKeeperFetchProbe {
    private static final short FETCH_VERSION = 12;
    private static final int MAX_WAIT_MS = 20_000;

    private final CompletableFuture<LogReadResult> firstRead = new CompletableFuture<>();
    private final ConcurrentLinkedQueue<String> observedWaves = new ConcurrentLinkedQueue<>();

    BrokerStorageFetchExecutor observe(BrokerStorageFetchExecutor nativeExecutor) {
        return new BrokerStorageFetchExecutor() {
            @Override
            public CompletionStage<Seq<Tuple2<TopicIdPartition, LogReadResult>>> submit(
                    FetchParams params,
                    Seq<Tuple2<TopicIdPartition, FetchRequest.PartitionData>> partitions,
                    Function1<Object, Seq<Tuple2<TopicIdPartition, LogReadResult>>> read) {
                return nativeExecutor.submit(params, partitions, initial -> {
                    if (params.maxWaitMs == MAX_WAIT_MS) observedWaves.add("read started");
                    var response = read.apply(initial);
                    if (params.maxWaitMs == MAX_WAIT_MS) {
                        var result = response.head()._2();
                        observedWaves.add("HW=" + result.highWatermark() + " LSO=" + result.lastStableOffset()
                                + " bytes=" + result.info().records.sizeInBytes() + " " + result.error());
                        if (Boolean.TRUE.equals(initial)) firstRead.complete(result);
                    }
                    return response;
                });
            }

            @Override
            public CompletionStage<Void> drained() {
                return nativeExecutor.drained();
            }

            @Override
            public void close() throws Exception {
                nativeExecutor.close();
            }
        };
    }

    void commitAndVerifyFetch(
            KafkaClusterTestKit cluster,
            int broker,
            Uuid topicId,
            TopicPartition partition,
            KafkaProducer<String, String> producer,
            List<String> expected) throws Exception {
        var fetch = FetchRequest.Builder.forConsumer(FETCH_VERSION, MAX_WAIT_MS, 1,
                        Map.of(partition, new FetchRequest.PartitionData(
                                topicId, 0, FetchRequest.INVALID_LOG_START_OFFSET, 1 << 20, Optional.empty())))
                .isolationLevel(IsolationLevel.READ_COMMITTED)
                .build(FETCH_VERSION);
        long start = System.nanoTime();
        try (var wire = new NativeKafkaRequest(cluster, broker, fetch)) {
            var initial = firstRead.get(5, TimeUnit.SECONDS);
            assertEquals(Errors.NONE, initial.error());
            assertEquals(expected.size(), initial.highWatermark());
            assertEquals(0, initial.lastStableOffset().orElseThrow());
            assertEquals(0, initial.info().records.sizeInBytes());
            assertThrows(SocketTimeoutException.class, () -> wire.receive(250),
                    "read_committed Fetch returned while the transaction was open");
            producer.commitTransaction();
            // The first and only request must return before Kafka's delayed Fetch timer fires.
            final FetchResponse response;
            try {
                response = (FetchResponse) wire.receive(5_000);
            } catch (SocketTimeoutException timeout) {
                throw new AssertionError("Fetch did not wake after transaction commit: " + observedWaves, timeout);
            }
            assertTrue(System.nanoTime() - start < TimeUnit.MILLISECONDS.toNanos(MAX_WAIT_MS),
                    "Fetch returned only when its wait expired");
            var result = response.responseData(Map.of(topicId, partition.topic()), FETCH_VERSION).get(partition);
            assertEquals(Errors.NONE.code(), result.errorCode());
            assertEquals(expected.size() + 1, result.highWatermark());
            assertEquals(result.highWatermark(), result.lastStableOffset());
            var offsets = new ArrayList<Long>();
            var actual = new ArrayList<String>();
            for (var batch : FetchResponse.recordsOrFail(result).batches()) {
                if (batch.isControlBatch()) {
                    continue;
                }
                for (var record : batch) {
                    offsets.add(record.offset());
                    actual.add(java.nio.charset.StandardCharsets.UTF_8.decode(record.value()).toString());
                }
            }
            assertEquals(expected, actual);
            assertEquals(java.util.stream.LongStream.range(0, expected.size()).boxed().toList(), offsets);
        }
    }
}
