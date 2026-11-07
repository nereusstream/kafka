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

import kafka.log.nereus.NereusUnifiedLog;
import kafka.server.storage.BrokerStorageFetchExecutor;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
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
import org.apache.kafka.test.TestUtils;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    private static final int IDLE_WAIT_MS = 1_500;
    private static final int DELETE_WAIT_MS = 18_000;
    private static final String PEER_TOPIC = "native-fetch-peer";

    private final CompletableFuture<LogReadResult> firstRead = new CompletableFuture<>();
    private final ConcurrentLinkedQueue<String> observedWaves = new ConcurrentLinkedQueue<>();
    private final IdleProbe idleProbe = new IdleProbe();
    private final IdleProbe deletionProbe = new IdleProbe();

    BrokerStorageFetchExecutor observe(BrokerStorageFetchExecutor nativeExecutor) {
        return new BrokerStorageFetchExecutor() {
            @Override
            public CompletionStage<Seq<Tuple2<TopicIdPartition, LogReadResult>>> submit(
                    FetchParams params,
                    Seq<Tuple2<TopicIdPartition, FetchRequest.PartitionData>> partitions,
                    Function1<Object, Seq<Tuple2<TopicIdPartition, LogReadResult>>> read) {
                return nativeExecutor.submit(params, partitions, initial -> {
                    boolean first = Boolean.TRUE.equals(initial);
                    if (params.maxWaitMs == MAX_WAIT_MS) observedWaves.add("read started");
                    var response = read.apply(initial);
                    if (params.maxWaitMs == MAX_WAIT_MS) {
                        var result = response.head()._2();
                        observedWaves.add("HW=" + result.highWatermark() + " LSO=" + result.lastStableOffset()
                                + " bytes=" + result.info().records.sizeInBytes() + " " + result.error());
                        if (first) firstRead.complete(result);
                    }
                    if (params.maxWaitMs == IDLE_WAIT_MS) idleProbe.record(first);
                    if (params.maxWaitMs == DELETE_WAIT_MS) deletionProbe.record(first);
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

    void verifyIdleAndDeletion(
            KafkaClusterTestKit cluster, Admin admin, int broker, Uuid topicId, TopicPartition primary, long end)
            throws Exception {
        var peer = new TopicPartition(PEER_TOPIC, 0);
        var peerId = admin.createTopics(List.of(new NewTopic(PEER_TOPIC, Map.of(0, List.of(broker)))))
                .topicId(PEER_TOPIC).get(30, TimeUnit.SECONDS);
        TestUtils.waitForCondition(() -> {
            var local = cluster.brokers().get(broker).replicaManager().onlinePartition(peer);
            return local.isDefined() && local.get().log().isDefined()
                    && ((NereusUnifiedLog) local.get().log().get()).nereusWritable(local.get().getLeaderEpoch());
        }, "Second native Fetch partition did not become ready");
        var partitions = new LinkedHashMap<TopicPartition, FetchRequest.PartitionData>();
        partitions.put(primary, fetchPartition(topicId, end));
        partitions.put(peer, fetchPartition(peerId, 0));
        var names = Map.of(topicId, primary.topic(), peerId, peer.topic());

        try (var wire = new NativeKafkaRequest(cluster, broker, fetchRequest(partitions, IDLE_WAIT_MS, 1))) {
            idleProbe.initial.get(5, TimeUnit.SECONDS);
            assertThrows(SocketTimeoutException.class, () -> wire.receive(250),
                    "Empty multi-partition Fetch returned before its deadline");
            assertEquals(1, idleProbe.waves.get(), "Idle Fetch polled without a publication event");
            var response = ((FetchResponse) wire.receive(5_000)).responseData(names, FETCH_VERSION);
            assertEquals(2, idleProbe.waves.get(), "Idle Fetch must have one initial and one deadline wave");
            assertEquals(2, response.size());
            assertEmpty(response.get(primary), end);
            assertEmpty(response.get(peer), 0);
        }

        try (var wire = new NativeKafkaRequest(cluster, broker, fetchRequest(partitions, DELETE_WAIT_MS, 1))) {
            deletionProbe.initial.get(5, TimeUnit.SECONDS);
            assertThrows(SocketTimeoutException.class, () -> wire.receive(250),
                    "Multi-partition Fetch did not wait on both frontiers");
            admin.deleteTopics(List.of(PEER_TOPIC)).all().get(5, TimeUnit.SECONDS);
            var response = ((FetchResponse) wire.receive(5_000)).responseData(names, FETCH_VERSION);
            assertEquals(2, response.size());
            assertEmpty(response.get(primary), end);
            var error = Errors.forCode(response.get(peer).errorCode());
            assertTrue(error == Errors.UNKNOWN_TOPIC_OR_PARTITION || error == Errors.UNKNOWN_TOPIC_ID
                            || error == Errors.NOT_LEADER_OR_FOLLOWER,
                    "Deleted Fetch partition returned " + error);
            assertEquals(0, FetchResponse.recordsSize(response.get(peer)));
        }
    }

    private static void assertEmpty(
            org.apache.kafka.common.message.FetchResponseData.PartitionData result, long end) {
        assertEquals(Errors.NONE.code(), result.errorCode());
        assertEquals(end, result.highWatermark());
        assertEquals(end, result.lastStableOffset());
        assertEquals(0, FetchResponse.recordsSize(result));
    }

    void commitAndVerifyFetch(
            KafkaClusterTestKit cluster,
            int broker,
            Uuid topicId,
            TopicPartition partition,
            KafkaProducer<String, String> producer,
            List<String> expected) throws Exception {
        var fetch = fetchRequest(topicId, partition, 0, MAX_WAIT_MS, 1);
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

    static void verifyAbortedWireResponse(
            KafkaClusterTestKit cluster, int broker, Uuid topicId, TopicPartition partition,
            long firstAbortedOffset, long markerOffset) throws Exception {
        var fetch = fetchRequest(topicId, partition, firstAbortedOffset, 0, 0);
        try (var wire = new NativeKafkaRequest(cluster, broker, fetch)) {
            var response = (FetchResponse) wire.receive(30_000);
            var result = response.responseData(Map.of(topicId, partition.topic()), FETCH_VERSION).get(partition);
            assertEquals(Errors.NONE.code(), result.errorCode());
            assertEquals(markerOffset + 1, result.highWatermark());
            assertEquals(result.highWatermark(), result.lastStableOffset());
            assertEquals(1, result.abortedTransactions().size());
            var aborted = result.abortedTransactions().get(0);
            assertEquals(firstAbortedOffset, aborted.firstOffset());
            long nextOffset = firstAbortedOffset;
            for (var batch : FetchResponse.recordsOrFail(result).batches()) {
                assertEquals(nextOffset, batch.baseOffset());
                assertEquals(aborted.producerId(), batch.producerId());
                assertTrue(batch.isTransactional());
                assertEquals(batch.baseOffset() == markerOffset, batch.isControlBatch());
                nextOffset = batch.lastOffset() + 1;
            }
            assertEquals(markerOffset + 1, nextOffset);
        }
    }

    private static FetchRequest fetchRequest(
            Uuid topicId, TopicPartition partition, long offset, int maxWait, int minBytes) {
        return fetchRequest(Map.of(partition, fetchPartition(topicId, offset)), maxWait, minBytes);
    }

    private static FetchRequest.PartitionData fetchPartition(Uuid topicId, long offset) {
        return new FetchRequest.PartitionData(topicId, offset, FetchRequest.INVALID_LOG_START_OFFSET,
                1 << 20, Optional.empty());
    }

    private static FetchRequest fetchRequest(
            Map<TopicPartition, FetchRequest.PartitionData> partitions, int maxWait, int minBytes) {
        return FetchRequest.Builder.forConsumer(FETCH_VERSION, maxWait, minBytes, partitions)
                .isolationLevel(IsolationLevel.READ_COMMITTED)
                .build(FETCH_VERSION);
    }

    private static final class IdleProbe {
        private final CompletableFuture<Void> initial = new CompletableFuture<>();
        private final AtomicInteger waves = new AtomicInteger();

        private void record(boolean first) {
            waves.incrementAndGet();
            if (first) initial.complete(null);
        }
    }
}
