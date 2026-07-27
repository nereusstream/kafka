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

package kafka.server.nereus

import com.nereusstream.api.{ErrorCode, NereusException}
import com.nereusstream.kafka.partition.KafkaPartitionIdentity
import com.nereusstream.kafka.retention.KafkaPartitionMaintenance
import kafka.cluster.Partition
import kafka.log.nereus.NereusUnifiedLog
import kafka.server.ReplicaManager
import org.junit.jupiter.api.Assertions.{assertEquals, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, verify, when}

import java.util
import java.util.concurrent.CompletionException

class NereusKafkaOwnedPartitionSourceBridgeTest {

  @Test
  def testLateBindingAndExactMaintenanceRegistration(): Unit = {
    val bridge = new NereusKafkaOwnedPartitionSourceBridge
    val unbound = assertThrows(classOf[CompletionException], () =>
      bridge.snapshot(10).toCompletableFuture.join())
    assertTrue(unbound.getCause.isInstanceOf[NereusException])
    assertEquals(
      ErrorCode.METADATA_UNAVAILABLE,
      unbound.getCause.asInstanceOf[NereusException].code())

    val replicaManager = mock(classOf[ReplicaManager])
    val partition = mock(classOf[Partition])
    val log = mock(classOf[NereusUnifiedLog])
    val authority = mock(classOf[NereusUnifiedLog.MaintenanceAuthority])
    val hooks = mock(classOf[KafkaPartitionMaintenance.Hooks])
    val identity = mock(classOf[KafkaPartitionIdentity])
    when(replicaManager.nereusOnlineLeaderPartitions(10))
      .thenReturn(util.List.of(partition))
    when(partition.leaderLogIfLocal).thenReturn(Some(log))
    when(partition.getLeaderEpoch).thenReturn(7)
    when(partition.nereusMaintenanceAuthority(log)).thenReturn(authority)
    when(log.nereusIdentity).thenReturn(identity)
    when(log.maintenanceHooks(7, authority)).thenReturn(hooks)

    bridge.bind(replicaManager)
    bridge.bind(replicaManager)
    val registrations = bridge.snapshot(10).toCompletableFuture.join()

    assertEquals(1, registrations.size())
    assertSame(identity, registrations.get(0).identity())
    assertEquals(7, registrations.get(0).leaderEpoch())
    assertSame(hooks, registrations.get(0).hooks())
    verify(replicaManager).nereusOnlineLeaderPartitions(10)

    assertThrows(classOf[IllegalStateException], () =>
      bridge.bind(mock(classOf[ReplicaManager])))
  }
}
