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
package org.apache.kafka.controller;

import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.metadata.nereus.MetadataRecordBatchSizer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Set;

import static org.apache.kafka.common.protocol.Errors.NONE;
import static org.apache.kafka.common.protocol.Errors.POLICY_VIOLATION;
import static org.apache.kafka.controller.ControllerRequestContextUtil.anonymousContextFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(40)
class NereusCreateTopicsAdmissionTest {
    @Test
    void testExactByteAdmissionBoundary() {
        CreateTopicsRequestData request = singleTopicRequest("byte-boundary", 1);

        ReplicationControlManagerTest.ReplicationControlTestContext probe = context();
        ControllerResult<CreateTopicsResponseData> probeResult = create(probe, request);
        int exactBytes = MetadataRecordBatchSizer.sizeInBytes(probeResult.records());
        assertTrue(exactBytes > 1);

        ReplicationControlManagerTest.ReplicationControlTestContext exact = context(exactBytes);
        ControllerResult<CreateTopicsResponseData> exactResult = create(exact, request);
        assertEquals(NONE.code(), exactResult.response().topics().find("byte-boundary").errorCode());

        ReplicationControlManagerTest.ReplicationControlTestContext below = context(exactBytes - 1);
        ControllerResult<CreateTopicsResponseData> belowResult = create(below, request);
        assertEquals(POLICY_VIOLATION.code(),
            belowResult.response().topics().find("byte-boundary").errorCode());
        assertTrue(belowResult.records().isEmpty());
    }

    @Test
    void testAtomicRecordBoundaryIncludesAggregate() {
        ReplicationControlManagerTest.ReplicationControlTestContext exact = context();
        CreatableTopicResult exactResult = exact.createTestTopic(
            "record-boundary-exact", 9_998, (short) 1, NONE.code());
        assertEquals(9_998, exactResult.numPartitions());

        ReplicationControlManagerTest.ReplicationControlTestContext overflow = context();
        overflow.createTestTopic(
            "record-boundary-overflow", 9_999, (short) 1, POLICY_VIOLATION.code());
    }

    @Test
    void testResponseLossRetryConvergesAfterReplay() {
        ReplicationControlManagerTest.ReplicationControlTestContext ctx = context();
        CreateTopicsRequestData request = singleTopicRequest("response-loss-retry", 1);

        ControllerResult<CreateTopicsResponseData> first = create(ctx, request);
        assertEquals(NONE.code(), first.response().topics().find("response-loss-retry").errorCode());
        assertEquals(3, first.records().size());
        ctx.replay(first.records());

        ControllerResult<CreateTopicsResponseData> retry = create(ctx, request);
        assertEquals(Errors.TOPIC_ALREADY_EXISTS.code(),
            retry.response().topics().find("response-loss-retry").errorCode());
        assertTrue(retry.records().isEmpty());
        assertTrue(ctx.replicationControl.getTopic(
            first.response().topics().find("response-loss-retry").topicId()).nereusAggregate().isPresent());
    }

    private static ReplicationControlManagerTest.ReplicationControlTestContext context() {
        return prepare(new ReplicationControlManagerTest.ReplicationControlTestContext.Builder());
    }

    private static ReplicationControlManagerTest.ReplicationControlTestContext context(int maxBatchSizeBytes) {
        return prepare(new ReplicationControlManagerTest.ReplicationControlTestContext.Builder()
            .setMaxBatchSizeBytes(maxBatchSizeBytes));
    }

    private static ReplicationControlManagerTest.ReplicationControlTestContext prepare(
        ReplicationControlManagerTest.ReplicationControlTestContext.Builder builder
    ) {
        ReplicationControlManagerTest.ReplicationControlTestContext ctx = builder
            .setIsNereusStorageEnabled(true)
            .build();
        ctx.registerBrokers(0);
        ctx.unfenceBrokers(0);
        return ctx;
    }

    private static ControllerResult<CreateTopicsResponseData> create(
        ReplicationControlManagerTest.ReplicationControlTestContext ctx,
        CreateTopicsRequestData request
    ) {
        return ctx.replicationControl.createTopics(
            anonymousContextFor(ApiKeys.CREATE_TOPICS), request, Set.of());
    }

    private static CreateTopicsRequestData singleTopicRequest(String topicName, int partitions) {
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        request.topics().add(new CreatableTopic()
            .setName(topicName)
            .setNumPartitions(partitions)
            .setReplicationFactor((short) 1));
        return request;
    }
}
