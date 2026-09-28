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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignablePartition;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignableTopic;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.Errors.NONE;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NereusOwnerRelocationTest {
    @Test
    public void testNereusBookKeeperRelocatesSoleReplicaAfterTwoOwnerFailures() {
        ReplicationControlManagerTest.ReplicationControlTestContext ctx = new ReplicationControlManagerTest.ReplicationControlTestContext.Builder()
            .setIsNereusStorageEnabled(true).build();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        Uuid topicId = ctx.createTestTopic("cold-owner", new int[][] {new int[] {0}},
            Map.of("nereus.storage.profile", "BOOKKEEPER_WAL_ONLY"), NONE.code()).topicId();
        var aggregate = ctx.replicationControl.getTopic(topicId).nereusAggregate().orElseThrow();
        PartitionRegistration initial = ctx.replicationControl.getPartition(topicId, 0);

        List<ApiMessageAndVersion> firstFailure = new ArrayList<>();
        ctx.replicationControl.handleBrokerFenced(0, firstFailure);
        ctx.replay(firstFailure);
        PartitionRegistration first = ctx.replicationControl.getPartition(topicId, 0);
        assertNereusColdAssignment(first, 1);
        assertTrue(first.leaderEpoch > initial.leaderEpoch);

        List<ApiMessageAndVersion> secondFailure = new ArrayList<>();
        ctx.replicationControl.handleBrokerFenced(1, secondFailure);
        ctx.replay(secondFailure);
        PartitionRegistration second = ctx.replicationControl.getPartition(topicId, 0);
        assertNereusColdAssignment(second, 2);
        assertTrue(second.leaderEpoch > first.leaderEpoch);
        assertEquals(aggregate, ctx.replicationControl.getTopic(topicId).nereusAggregate().orElseThrow());
    }

    @Test
    public void testNereusBookKeeperWaitsForEligibleReplacementAndReassignsWithoutFollower() {
        ReplicationControlManagerTest.ReplicationControlTestContext ctx = new ReplicationControlManagerTest.ReplicationControlTestContext.Builder()
            .setIsNereusStorageEnabled(true).build();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1);
        ctx.inControlledShutdownBrokers(1);
        Uuid topicId = ctx.createTestTopic("cold-owner", new int[][] {new int[] {0}},
            Map.of("nereus.storage.profile", "BOOKKEEPER_WAL_ONLY"), NONE.code()).topicId();
        List<ApiMessageAndVersion> failure = new ArrayList<>();
        ctx.replicationControl.handleBrokerFenced(0, failure);
        ctx.replay(failure);
        assertEquals(NO_LEADER, ctx.replicationControl.getPartition(topicId, 0).leader);
        assertArrayEquals(new int[] {0}, ctx.replicationControl.getPartition(topicId, 0).replicas);

        ctx.unfenceBrokers(2);
        assertNereusColdAssignment(ctx.replicationControl.getPartition(topicId, 0), 2);
        ctx.unfenceBrokers(0);
        ControllerResult<AlterPartitionReassignmentsResponseData> reassignment =
            ctx.replicationControl.alterPartitionReassignments(
                new AlterPartitionReassignmentsRequestData().setTopics(List.of(
                    new ReassignableTopic().setName("cold-owner").setPartitions(List.of(
                        new ReassignablePartition().setPartitionIndex(0).setReplicas(List.of(0)))))));
        assertEquals(NONE.code(), reassignment.response().responses().get(0).partitions().get(0).errorCode());
        ctx.replay(reassignment.records());
        assertNereusColdAssignment(ctx.replicationControl.getPartition(topicId, 0), 0);
        assertTrue(ctx.replicationControl.listPartitionReassignments(null, Long.MAX_VALUE).topics().isEmpty());
    }

    private static void assertNereusColdAssignment(PartitionRegistration partition, int owner) {
        assertEquals(owner, partition.leader);
        assertArrayEquals(new int[] {owner}, partition.replicas);
        assertArrayEquals(new int[] {owner}, partition.isr);
        assertArrayEquals(new int[0], partition.addingReplicas);
        assertArrayEquals(new int[0], partition.removingReplicas);
        assertEquals(LeaderRecoveryState.RECOVERING, partition.leaderRecoveryState);
    }

}
