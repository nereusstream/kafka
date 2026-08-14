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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerResourceGuard;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GuardedSenderTest {
    private static final String TOPIC = "guarded-topic";
    private static final Uuid TOPIC_ID = new Uuid(41L, 42L);
    private final MockTime time = new MockTime();
    private final ProducerMetadata metadata = new ProducerMetadata(0, 0, Long.MAX_VALUE, 60_000,
            new LogContext(), new ClusterResourceListeners(), time);
    private MockClient client;
    private Metrics metrics;
    private RecordAccumulator accumulator;
    private Sender sender;

    @BeforeEach
    void setUp() {
        client = new MockClient(time, metadata);
        client.setNodeApiVersions(NodeApiVersions.create(ApiKeys.PRODUCE.id, (short) 0, (short) 13));
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1,
                Collections.singletonMap(TOPIC, 1), Collections.singletonMap(TOPIC, TOPIC_ID)));
        metadata.add(TOPIC, time.milliseconds());

        metrics = new Metrics();
        BufferPool bufferPool = new BufferPool(1024 * 1024, 16 * 1024, metrics, time, "guarded-test");
        accumulator = new RecordAccumulator(new LogContext(), 16 * 1024, Compression.NONE, 0,
                0L, 0L, 10_000, metrics, "guarded-test", time, null, bufferPool);
        sender = newSender(1);
    }

    @AfterEach
    void tearDown() {
        metrics.close();
    }

    @Test
    void pinsProduceV13AndCompletesWithBoundEvidence() throws Exception {
        Cluster cluster = metadata.fetch();
        ProducerResourceGuard guard = new ProducerResourceGuard(cluster.clusterResource().clusterId(), TOPIC,
                TOPIC_ID, 0);
        byte[] value = new byte[]{1, 2, 3};
        AtomicReference<Object> completionEvidence = new AtomicReference<>();
        AtomicReference<RecordMetadata> completionMetadata = new AtomicReference<>();
        AtomicReference<Object> completionFailure = new AtomicReference<>();
        GuardedCompletion completion = new GuardedCompletion() {
            @Override
            public ProducerResourceGuard guard() {
                return guard;
            }

            @Override
            public void complete(RecordMetadata recordMetadata, Object evidence) {
                completionMetadata.set(recordMetadata);
                completionEvidence.set(evidence);
            }

            @Override
            public void fail(Object failure) {
                completionFailure.set(failure);
            }
        };

        accumulator.append(TOPIC, 0, 123L, null, value, new Header[0], null,
                1_000, time.milliseconds(), cluster, guard, completion,
                GuardedEvidenceUtils.sha256(ByteBuffer.wrap(value)));

        sender.runOnce();
        sender.runOnce();
        assertEquals(1, client.requests().size());
        ProduceRequest request = (ProduceRequest) client.requests().peek().requestBuilder().build();
        assertEquals(13, request.version());
        assertEquals(TOPIC_ID, request.data().topicData().iterator().next().topicId());

        ProduceResponseData.TopicProduceResponse topicResponse = new ProduceResponseData.TopicProduceResponse()
                .setName("")
                .setTopicId(TOPIC_ID);
        topicResponse.partitionResponses().add(new ProduceResponseData.PartitionProduceResponse()
                .setIndex(0)
                .setErrorCode((short) 0)
                .setBaseOffset(77L)
                .setLogAppendTimeMs(88L)
                .setCurrentLeader(new ProduceResponseData.LeaderIdAndEpoch().setLeaderEpoch(9)));
        client.respond(new ProduceResponse(new ProduceResponseData().setResponses(
                new ProduceResponseData.TopicProduceResponseCollection(Collections.singletonList(topicResponse)))));
        sender.runOnce();

        assertNotNull(completionMetadata.get());
        assertEquals(77L, completionMetadata.get().offset());
        assertNotNull(completionEvidence.get());
        assertEquals(null, completionFailure.get());
        GuardedCompletion.Evidence evidence = assertInstanceOf(GuardedCompletion.Evidence.class,
                completionEvidence.get());
        assertEquals(13, evidence.requestContext.requestVersion);
        assertEquals(77L, evidence.baseOffset);
        assertEquals(OptionalInt.of(9), evidence.responseLeaderEpoch);
        assertEquals(0, evidence.selectedBatchRecordIndex);
        assertEquals(1, evidence.selectedBatchRecordCount);
        assertArrayEquals(GuardedEvidenceUtils.sha256(ByteBuffer.wrap(value)), evidence.selectedRecordValueSha256);
        assertEquals(32, evidence.requestContext.produceRequestBodySha256.length);
        assertEquals(32, evidence.produceResponseBodySha256.length);
    }

    @Test
    void guardedAndOrdinaryRecordsNeverShareAnAccumulatorBatch() throws Exception {
        Cluster cluster = metadata.fetch();
        ProducerResourceGuard guard = new ProducerResourceGuard(cluster.clusterResource().clusterId(), TOPIC,
                TOPIC_ID, 0);
        byte[] value = new byte[]{9};
        accumulator.append(TOPIC, 0, 1L, null, value, new Header[0], null,
                1_000, time.milliseconds(), cluster);
        accumulator.append(TOPIC, 0, 2L, null, value, new Header[0], null,
                1_000, time.milliseconds(), cluster, guard, completionFor(guard), new byte[32]);

        assertEquals(2, accumulator.getDeque(new TopicPartition(TOPIC, 0)).size());
        assertEquals(null, accumulator.getDeque(new TopicPartition(TOPIC, 0)).peekFirst().resourceGuard());
        assertEquals(guard, accumulator.getDeque(new TopicPartition(TOPIC, 0)).peekLast().resourceGuard());
    }

    @Test
    void unknownTopicIdIsTheOnlyDefinitiveBrokerRejection() throws Exception {
        Cluster cluster = metadata.fetch();
        ProducerResourceGuard guard = new ProducerResourceGuard(cluster.clusterResource().clusterId(), TOPIC,
                TOPIC_ID, 0);
        AtomicReference<Object> failure = new AtomicReference<>();
        accumulator.append(TOPIC, 0, 123L, null, new byte[]{1}, new Header[0], null,
                1_000, time.milliseconds(), cluster, guard, completionFor(guard, null, failure),
                GuardedEvidenceUtils.sha256(ByteBuffer.wrap(new byte[]{1})));

        sender.runOnce();
        sender.runOnce();
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1,
                Collections.singletonMap(TOPIC, 1), Collections.singletonMap(TOPIC, new Uuid(51L, 52L))));
        client.respond(produceResponse(TOPIC_ID, Errors.UNKNOWN_TOPIC_ID.code(), -1L, -1L));
        sender.runOnce();

        GuardedCompletion.Failure result = assertInstanceOf(GuardedCompletion.Failure.class, failure.get());
        assertEquals(org.apache.kafka.clients.producer.ResourceGuardFailureReason.AUTHENTICATED_BROKER_REJECTION,
                result.reason);
        assertTrue(result.definitelyNotPersisted);
    }

    @Test
    void nonAllowlistedBrokerRejectionIsNotDefinitive() throws Exception {
        Cluster cluster = metadata.fetch();
        ProducerResourceGuard guard = new ProducerResourceGuard(cluster.clusterResource().clusterId(), TOPIC,
                TOPIC_ID, 0);
        AtomicReference<Object> failure = new AtomicReference<>();
        accumulator.append(TOPIC, 0, 123L, null, new byte[]{1}, new Header[0], null,
                1_000, time.milliseconds(), cluster, guard, completionFor(guard, null, failure),
                GuardedEvidenceUtils.sha256(ByteBuffer.wrap(new byte[]{1})));

        sender.runOnce();
        sender.runOnce();
        client.respond(produceResponse(TOPIC_ID, Errors.TOPIC_AUTHORIZATION_FAILED.code(), -1L, -1L));
        sender.runOnce();

        GuardedCompletion.Failure result = assertInstanceOf(GuardedCompletion.Failure.class, failure.get());
        assertEquals(org.apache.kafka.clients.producer.ResourceGuardFailureReason.AUTHENTICATED_BROKER_REJECTION,
                result.reason);
        assertFalse(result.definitelyNotPersisted);
    }

    @Test
    void disconnectCompletesAsAmbiguousAndCannotBecomeDefinitive() throws Exception {
        Cluster cluster = metadata.fetch();
        ProducerResourceGuard guard = new ProducerResourceGuard(cluster.clusterResource().clusterId(), TOPIC,
                TOPIC_ID, 0);
        AtomicReference<Object> failure = new AtomicReference<>();
        accumulator.append(TOPIC, 0, 123L, null, new byte[]{1}, new Header[0], null,
                1_000, time.milliseconds(), cluster, guard, completionFor(guard, null, failure),
                GuardedEvidenceUtils.sha256(ByteBuffer.wrap(new byte[]{1})));

        sender.runOnce();
        sender.runOnce();
        client.respond(produceResponse(TOPIC_ID, (short) 0, 77L, 88L), true);
        sender.runOnce();

        GuardedCompletion.Failure result = assertInstanceOf(GuardedCompletion.Failure.class, failure.get());
        assertEquals(org.apache.kafka.clients.producer.ResourceGuardFailureReason.AMBIGUOUS_PRIOR_ATTEMPT,
                result.reason);
        assertFalse(result.definitelyNotPersisted);
    }

    @Test
    void leaderErrorRetriesWithTheSameExpectedTopicId() throws Exception {
        Cluster cluster = metadata.fetch();
        ProducerResourceGuard guard = new ProducerResourceGuard(cluster.clusterResource().clusterId(), TOPIC,
                TOPIC_ID, 0);
        AtomicReference<RecordMetadata> completion = new AtomicReference<>();
        AtomicReference<Object> failure = new AtomicReference<>();
        accumulator.append(TOPIC, 0, 123L, null, new byte[]{1}, new Header[0], null,
                1_000, time.milliseconds(), cluster, guard, completionFor(guard, completion, failure),
                GuardedEvidenceUtils.sha256(ByteBuffer.wrap(new byte[]{1})));

        sender.runOnce();
        sender.runOnce();
        client.respond(produceResponse(TOPIC_ID, Errors.NOT_LEADER_OR_FOLLOWER.code(), -1L, -1L));
        sender.runOnce();
        sender.runOnce();
        assertEquals(1, client.requests().size());
        ProduceRequest retry = (ProduceRequest) client.requests().peek().requestBuilder().build();
        assertEquals(13, retry.version());
        assertEquals(TOPIC_ID, retry.data().topicData().iterator().next().topicId());

        client.respond(produceResponse(TOPIC_ID, (short) 0, 77L, 88L));
        sender.runOnce();
        assertNotNull(completion.get());
        assertEquals(null, failure.get());
    }

    private Sender newSender(final int retries) {
        return new Sender(new LogContext(), client, metadata, accumulator, false,
                1024 * 1024, (short) -1, retries, new SenderMetricsRegistry(metrics), time,
                5_000, 0L, null);
    }

    private static ProduceResponse produceResponse(final Uuid topicId, final short errorCode,
                                                   final long baseOffset, final long logAppendTimeMs) {
        ProduceResponseData.TopicProduceResponse topicResponse = new ProduceResponseData.TopicProduceResponse()
                .setName("").setTopicId(topicId);
        topicResponse.partitionResponses().add(new ProduceResponseData.PartitionProduceResponse()
                .setIndex(0).setErrorCode(errorCode).setBaseOffset(baseOffset)
                .setLogAppendTimeMs(logAppendTimeMs));
        return new ProduceResponse(new ProduceResponseData().setResponses(
                new ProduceResponseData.TopicProduceResponseCollection(Collections.singletonList(topicResponse))));
    }

    private static GuardedCompletion completionFor(final ProducerResourceGuard guard) {
        return completionFor(guard, null, null);
    }

    private static GuardedCompletion completionFor(final ProducerResourceGuard guard,
                                                   final AtomicReference<RecordMetadata> completionMetadata,
                                                   final AtomicReference<Object> completionFailure) {
        return new GuardedCompletion() {
            @Override
            public ProducerResourceGuard guard() {
                return guard;
            }

            @Override
            public void complete(final RecordMetadata metadata, final Object evidence) {
                if (completionMetadata != null)
                    completionMetadata.set(metadata);
            }

            @Override
            public void fail(final Object failure) {
                if (completionFailure != null)
                    completionFailure.set(failure);
            }
        };
    }
}
