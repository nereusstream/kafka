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

package kafka.log.nereus

import com.nereusstream.api.{ErrorCode, NereusException, StorageProfile}
import com.nereusstream.kafka.partition.{KafkaPartitionIdentity, KafkaPartitionLeaderOpenRequest, KafkaPartitionStorage}
import kafka.cluster.Partition
import kafka.server.ReplicaManager
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.metadata.{PartitionChangeRecord, PartitionRecord, RemoveTopicRecord, TopicRecord}
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong, same}
import org.mockito.Mockito.{mock, never, verify, when}

import java.time.Duration
import java.util.concurrent.{CompletableFuture, CompletionException}
import scala.collection.mutable

class NereusTopicDeltaLifecycleTest {
  private val brokerId = 1
  private val timeout = Duration.ofSeconds(5)
  private val profile = StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT

  @Test
  def testNewLeaderPublicationMarksExactEpochPendingSynchronously(): Unit = {
    val topicId = Uuid.randomUuid()
    val partition = mock(classOf[Partition])
    when(partition.topicId).thenReturn(Some(topicId))
    val lifecycle = newLifecycle(
      mock(classOf[ReplicaManager]),
      mock(classOf[NereusListOffsetsLifecycle]),
      brokerEpoch = 9)

    lifecycle.onLeaderStatePublished(partition, topicId, 5)

    verify(partition).beginLeaderEpochAwareOffsetLookup(5)
    assertThrows(classOf[NereusException], () =>
      lifecycle.onLeaderStatePublished(partition, Uuid.randomUuid(), 5))
  }

  @Test
  def testLeaderCallbackWaitsForExactRecoveredStorageInstallation(): Unit = {
    val topicId = Uuid.randomUuid()
    val topicPartition = new TopicPartition("events", 0)
    val (delta, image) = createLeader(topicPartition, topicId, leaderEpoch = 5, metadataOffset = 10)
    val partition = mock(classOf[Partition])
    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.onlinePartition(topicPartition)).thenReturn(Some(partition))
    val partitionLifecycle = mock(classOf[NereusListOffsetsLifecycle])
    val openedStorage = new CompletableFuture[KafkaPartitionStorage]
    when(partitionLifecycle.openLeader(
      any(classOf[Partition]),
      any(classOf[KafkaPartitionLeaderOpenRequest])))
      .thenReturn(openedStorage)
    val lifecycle = newLifecycle(replicaManager, partitionLifecycle, brokerEpoch = 9)
    val ready = mutable.ArrayBuffer.empty[(TopicPartition, Int)]

    val applied = lifecycle.applyAfterReplicaManager(
      delta.topicsDelta(),
      image,
      (tp, epoch) => ready += tp -> epoch,
      (_, _) => ())

    assertFalse(applied.isDone)
    assertTrue(ready.isEmpty)
    val requestCaptor = ArgumentCaptor.forClass(classOf[KafkaPartitionLeaderOpenRequest])
    verify(partitionLifecycle).openLeader(same(partition), requestCaptor.capture())
    val request = requestCaptor.getValue
    assertEquals("kraft-cluster", request.identity().kafkaClusterId())
    assertEquals(topicId.toString, request.identity().topicId())
    assertEquals(topicPartition.topic(), request.identity().observedTopicName())
    assertEquals(5, request.leaderEpoch())
    assertEquals(9, request.brokerEpoch())
    assertEquals(10, request.metadataOffset())
    assertEquals(profile, request.storageProfile())

    val storage = mock(classOf[KafkaPartitionStorage])
    openedStorage.complete(storage)

    applied.join()
    assertEquals(Seq(topicPartition -> 5), ready.toSeq)
  }

  @Test
  def testFollowerResignationUsesNewObservedLeaderEpoch(): Unit = {
    val topicId = Uuid.randomUuid()
    val topicPartition = new TopicPartition("events", 0)
    val (initialDelta, initialImage) = createLeader(
      topicPartition, topicId, leaderEpoch = 5, metadataOffset = 10)
    assertTrue(initialDelta.topicsDelta().localChanges(brokerId).leaders().containsKey(topicPartition))
    val delta = new MetadataDelta(initialImage)
    delta.replay(new PartitionChangeRecord()
      .setTopicId(topicId)
      .setPartitionId(0)
      .setLeader(2))
    val image = delta.apply(new MetadataProvenance(11, 1, 1000, true))
    val partitionLifecycle = mock(classOf[NereusListOffsetsLifecycle])
    val resigned = new CompletableFuture[Void]
    when(partitionLifecycle.resign(
      any(classOf[KafkaPartitionIdentity]),
      anyInt(),
      any(classOf[Duration])))
      .thenReturn(resigned)
    val lifecycle = newLifecycle(mock(classOf[ReplicaManager]), partitionLifecycle, brokerEpoch = 9)
    val callbacks = mutable.ArrayBuffer.empty[(TopicPartition, Option[Int])]

    val applied = lifecycle.applyAfterReplicaManager(
      delta.topicsDelta(), image, (_, _) => (), (tp, epoch) => callbacks += tp -> epoch)

    assertTrue(callbacks.isEmpty)
    resigned.complete(null)
    applied.join()
    assertEquals(Seq(topicPartition -> Some(6)), callbacks.toSeq)
  }

  @Test
  def testDeleteUsesPreviousTopicIdAndReportsEpochlessResignation(): Unit = {
    val topicId = Uuid.randomUuid()
    val topicPartition = new TopicPartition("events", 0)
    val (_, initialImage) = createLeader(topicPartition, topicId, leaderEpoch = 5, metadataOffset = 10)
    val delta = new MetadataDelta(initialImage)
    delta.replay(new RemoveTopicRecord().setTopicId(topicId))
    val image = delta.apply(new MetadataProvenance(11, 1, 1000, true))
    val partitionLifecycle = mock(classOf[NereusListOffsetsLifecycle])
    val deleted = new CompletableFuture[Void]
    when(partitionLifecycle.delete(
      any(classOf[KafkaPartitionIdentity]),
      anyLong(),
      any(classOf[Duration])))
      .thenReturn(deleted)
    val lifecycle = newLifecycle(mock(classOf[ReplicaManager]), partitionLifecycle, brokerEpoch = 9)
    val callbacks = mutable.ArrayBuffer.empty[(TopicPartition, Option[Int])]

    val applied = lifecycle.applyAfterReplicaManager(
      delta.topicsDelta(), image, (_, _) => (), (tp, epoch) => callbacks += tp -> epoch)

    val identityCaptor = ArgumentCaptor.forClass(classOf[KafkaPartitionIdentity])
    verify(partitionLifecycle).delete(identityCaptor.capture(), org.mockito.ArgumentMatchers.eq(11L), same(timeout))
    assertEquals(topicId.toString, identityCaptor.getValue.topicId())
    assertTrue(callbacks.isEmpty)
    deleted.complete(null)
    applied.join()
    assertEquals(Seq(topicPartition -> None), callbacks.toSeq)
  }

  @Test
  def testDeleteThenSameNameRecreationIsSerializedPerPartition(): Unit = {
    val oldTopicId = Uuid.randomUuid()
    val newTopicId = Uuid.randomUuid()
    val topicPartition = new TopicPartition("events", 0)
    val (_, initialImage) = createLeader(topicPartition, oldTopicId, leaderEpoch = 5, metadataOffset = 10)
    val delta = new MetadataDelta(initialImage)
    delta.replay(new RemoveTopicRecord().setTopicId(oldTopicId))
    delta.replay(new TopicRecord().setName(topicPartition.topic()).setTopicId(newTopicId))
    delta.replay(new PartitionRecord()
      .setTopicId(newTopicId)
      .setPartitionId(0)
      .setLeader(brokerId)
      .setLeaderEpoch(0)
      .setPartitionEpoch(0)
      .setReplicas(java.util.List.of(brokerId))
      .setIsr(java.util.List.of(brokerId)))
    val image = delta.apply(new MetadataProvenance(11, 1, 1000, true))
    val partition = mock(classOf[Partition])
    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.onlinePartition(topicPartition)).thenReturn(Some(partition))
    val partitionLifecycle = mock(classOf[NereusListOffsetsLifecycle])
    val deleted = new CompletableFuture[Void]
    val opened = new CompletableFuture[KafkaPartitionStorage]
    when(partitionLifecycle.delete(
      any(classOf[KafkaPartitionIdentity]), anyLong(), any(classOf[Duration])))
      .thenReturn(deleted)
    when(partitionLifecycle.openLeader(
      any(classOf[Partition]), any(classOf[KafkaPartitionLeaderOpenRequest])))
      .thenReturn(opened)
    val lifecycle = newLifecycle(replicaManager, partitionLifecycle, brokerEpoch = 9)
    val events = mutable.ArrayBuffer.empty[String]

    val applied = lifecycle.applyAfterReplicaManager(
      delta.topicsDelta(),
      image,
      (_, _) => events += "leader-ready",
      (_, _) => events += "resigned")

    verify(partitionLifecycle, never()).openLeader(
      any(classOf[Partition]), any(classOf[KafkaPartitionLeaderOpenRequest]))
    deleted.complete(null)
    assertEquals(Seq("resigned"), events.toSeq)
    verify(partitionLifecycle).openLeader(
      same(partition), any(classOf[KafkaPartitionLeaderOpenRequest]))
    opened.complete(mock(classOf[KafkaPartitionStorage]))

    applied.join()
    assertEquals(Seq("resigned", "leader-ready"), events.toSeq)
  }

  @Test
  def testFailedOperationDoesNotPublishAReadyCallback(): Unit = {
    val topicId = Uuid.randomUuid()
    val topicPartition = new TopicPartition("events", 0)
    val (delta, image) = createLeader(topicPartition, topicId, leaderEpoch = 5, metadataOffset = 10)
    val partition = mock(classOf[Partition])
    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.onlinePartition(topicPartition)).thenReturn(Some(partition))
    val partitionLifecycle = mock(classOf[NereusListOffsetsLifecycle])
    when(partitionLifecycle.openLeader(
      any(classOf[Partition]), any(classOf[KafkaPartitionLeaderOpenRequest])))
      .thenReturn(CompletableFuture.failedFuture(new NereusException(
        ErrorCode.FENCED_APPEND, false, "fenced")))
    val lifecycle = newLifecycle(replicaManager, partitionLifecycle, brokerEpoch = 9)
    val ready = mutable.ArrayBuffer.empty[TopicPartition]

    val applied = lifecycle.applyAfterReplicaManager(
      delta.topicsDelta(), image, (tp, _) => ready += tp, (_, _) => ())

    assertThrows(classOf[CompletionException], () => applied.join())
    assertTrue(ready.isEmpty)
    verify(partition).cancelLeaderEpochAwareOffsetLookup(5)
  }

  @Test
  def testBrokerEpochSupplierFailureCancelsPreparedLeader(): Unit = {
    val topicId = Uuid.randomUuid()
    val topicPartition = new TopicPartition("events", 0)
    val (delta, image) = createLeader(topicPartition, topicId, leaderEpoch = 5, metadataOffset = 10)
    val partition = mock(classOf[Partition])
    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.onlinePartition(topicPartition)).thenReturn(Some(partition))
    val partitionLifecycle = mock(classOf[NereusListOffsetsLifecycle])
    val lifecycle = new NereusTopicDeltaLifecycle(
      "kraft-cluster",
      brokerId,
      () => throw new IllegalStateException("broker epoch unavailable"),
      profile,
      timeout,
      replicaManager,
      partitionLifecycle)

    val applied = lifecycle.applyAfterReplicaManager(
      delta.topicsDelta(), image, (_, _) => (), (_, _) => ())

    val failure = assertThrows(classOf[CompletionException], () => applied.join())
    assertEquals("broker epoch unavailable", failure.getCause.getMessage)
    verify(partition).cancelLeaderEpochAwareOffsetLookup(5)
    verify(partitionLifecycle, never()).openLeader(
      any(classOf[Partition]), any(classOf[KafkaPartitionLeaderOpenRequest]))
  }

  private def newLifecycle(
    replicaManager: ReplicaManager,
    partitionLifecycle: NereusListOffsetsLifecycle,
    brokerEpoch: Long
  ): NereusTopicDeltaLifecycle = new NereusTopicDeltaLifecycle(
    "kraft-cluster",
    brokerId,
    () => brokerEpoch,
    profile,
    timeout,
    replicaManager,
    partitionLifecycle)

  private def createLeader(
    topicPartition: TopicPartition,
    topicId: Uuid,
    leaderEpoch: Int,
    metadataOffset: Long
  ): (MetadataDelta, MetadataImage) = {
    val delta = new MetadataDelta(MetadataImage.EMPTY)
    delta.replay(new TopicRecord().setName(topicPartition.topic()).setTopicId(topicId))
    delta.replay(new PartitionRecord()
      .setTopicId(topicId)
      .setPartitionId(topicPartition.partition())
      .setLeader(brokerId)
      .setLeaderEpoch(leaderEpoch)
      .setPartitionEpoch(0)
      .setReplicas(java.util.List.of(brokerId))
      .setIsr(java.util.List.of(brokerId)))
    delta -> delta.apply(new MetadataProvenance(metadataOffset, 1, 1000, true))
  }
}
