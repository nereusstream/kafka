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
import com.nereusstream.kafka.compaction.{
  KafkaCompactionPartitionPass,
  KafkaCompactionRuntime,
  KafkaCompactionScheduler
}
import com.nereusstream.kafka.partition.KafkaPartitionIdentity
import com.nereusstream.kafka.retention.KafkaPartitionMaintenance
import kafka.cluster.Partition
import kafka.log.nereus.NereusUnifiedLog
import kafka.server.ReplicaManager
import org.apache.kafka.server.config.NereusKafkaStorageConfig
import org.junit.jupiter.api.Assertions.{assertEquals, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyInt, same}
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
    val compactionProvider =
      mock(classOf[KafkaCompactionPartitionPass.CaptureProvider])
    val identity = mock(classOf[KafkaPartitionIdentity])
    when(replicaManager.nereusOnlineLeaderPartitions(10))
      .thenReturn(util.List.of(partition))
    when(partition.leaderLogIfLocal).thenReturn(Some(log))
    when(partition.getLeaderEpoch).thenReturn(7)
    when(partition.nereusMaintenanceAuthority(log)).thenReturn(authority)
    when(log.nereusIdentity).thenReturn(identity)
    when(identity.observedTopicName()).thenReturn("__consumer_offsets")
    when(log.maintenanceHooks(7, authority)).thenReturn(hooks)
    when(log.compactionCaptureProvider(
      anyInt(),
      same(authority),
      any(classOf[NereusUnifiedLog.CompactionConfiguration])))
      .thenReturn(compactionProvider)

    bridge.bind(replicaManager)
    bridge.bind(replicaManager)
    val registrations = bridge.snapshot(10).toCompletableFuture.join()

    assertEquals(1, registrations.size())
    assertSame(identity, registrations.get(0).identity())
    assertEquals(7, registrations.get(0).leaderEpoch())
    assertSame(hooks, registrations.get(0).hooks())
    verify(replicaManager).nereusOnlineLeaderPartitions(10)

    val triggers = new KafkaCompactionScheduler.TriggerBatch(
      util.Set.of(KafkaCompactionScheduler.Trigger.STARTUP),
      KafkaCompactionScheduler.Trigger.STARTUP)
    val unconfigured = assertThrows(classOf[CompletionException], () =>
      bridge.snapshot(triggers, 10).toCompletableFuture.join())
    assertEquals(
      ErrorCode.METADATA_UNAVAILABLE,
      unconfigured.getCause.asInstanceOf[NereusException].code())

    val storage = mock(classOf[NereusKafkaStorageConfig])
    val compaction = mock(classOf[NereusKafkaStorageConfig.RetentionCompaction])
    val lifecycle = mock(classOf[NereusKafkaStorageConfig.Lifecycle])
    when(storage.retentionCompaction()).thenReturn(compaction)
    when(storage.lifecycle()).thenReturn(lifecycle)
    when(compaction.compactionTaskMaxRecords()).thenReturn(1_000_000L)
    when(compaction.compactionTaskMaxSourceBytes()).thenReturn(256L * 1024 * 1024)
    when(compaction.compactionKeyMaxBytes()).thenReturn(1024 * 1024)
    when(compaction.compactionSpillMaxBytes()).thenReturn(64L * 1024 * 1024)
    when(lifecycle.recoveryChunkRecords()).thenReturn(8_192)

    bridge.configureCompaction(storage, "build-a")
    bridge.configureCompaction(storage, "build-a")
    val compactionRegistrations =
      bridge.snapshot(triggers, 10).toCompletableFuture.join()

    assertEquals(1, compactionRegistrations.size())
    assertSame(identity, compactionRegistrations.get(0).identity())
    assertEquals(7, compactionRegistrations.get(0).leaderEpoch())
    assertEquals(
      KafkaCompactionRuntime.WorkClass.INTERNAL,
      compactionRegistrations.get(0).workClass())
    assertSame(
      compactionProvider,
      compactionRegistrations.get(0).captureProvider())
    val configuration =
      ArgumentCaptor.forClass(classOf[NereusUnifiedLog.CompactionConfiguration])
    verify(log).compactionCaptureProvider(
      anyInt(),
      same(authority),
      configuration.capture())
    assertEquals(
      "kafka-log-cleaner-v1",
      configuration.getValue.outputPolicy().topicCompaction().orElseThrow().strategyId())
    assertEquals(
      "KCK2",
      configuration.getValue.outputPolicy().topicCompaction().orElseThrow().keyCodecId())
    assertEquals(1_000_000L, configuration.getValue.maxDecodedRecords())
    assertEquals(1024 * 1024, configuration.getValue.maxKeyBytes())
    assertEquals(
      64L * 1024 * 1024,
      configuration.getValue.maxInMemoryKeyBytes())
    assertEquals("build-a", configuration.getValue.writeSettings().writerBuild())

    when(compaction.compactionKeyMaxBytes()).thenReturn(512 * 1024)
    assertThrows(classOf[IllegalStateException], () =>
      bridge.configureCompaction(storage, "build-a"))
    assertThrows(classOf[IllegalStateException], () =>
      bridge.bind(mock(classOf[ReplicaManager])))
  }
}
