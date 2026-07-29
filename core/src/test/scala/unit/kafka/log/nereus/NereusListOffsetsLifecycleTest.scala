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
import com.nereusstream.kafka.partition.{KafkaPartitionIdentity, KafkaPartitionLeaderOpenRequest, KafkaPartitionState, KafkaPartitionStorage, KafkaPartitionStorageManager}
import kafka.cluster.Partition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.FencedLeaderEpochException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.storage.internals.log.LeaderEpochAwareOffsetLookup
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito.{doAnswer, doThrow, mock, never, verify, when}

import java.time.Duration
import java.util.Optional
import java.util.concurrent.{CompletableFuture, CompletionException}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

class NereusListOffsetsLifecycleTest {
  private val timeout = Duration.ofSeconds(5)
  private val scanConfig = new NereusListOffsetsScanConfig(
    100,
    1024 * 1024,
    4096,
    1024 * 1024,
    10,
    timeout)

  @Test
  def testInstallsOnlyAfterRecoveredStorageAndRemovesBeforeResign(): Unit = {
    val events = mutable.ArrayBuffer.empty[String]
    val topicId = Uuid.randomUuid()
    val partition = partitionAt(topicId, new AtomicInteger(5), events)
    val manager = new RecordingStorageManager(events)
    val lifecycle = new NereusListOffsetsLifecycle(manager, scanConfig)
    val request = leaderRequest(topicId, 5)

    val opened = lifecycle.openLeader(partition, request)
    val duplicate = lifecycle.openLeader(partition, request)

    assertSame(opened, duplicate)
    assertFalse(opened.isDone)
    assertFalse(opened.cancel(false))
    assertEquals(Seq("partition-begin-5", "manager-open-5"), events.toSeq)

    val storage = storageFor(request)
    manager.completeOpen(5, storage)

    assertSame(storage, opened.join())
    assertEquals(1, lifecycle.installedPartitions)
    assertEquals(Seq("partition-begin-5", "manager-open-5", "partition-install-5"), events.toSeq)
    verify(storage, never()).probeMandatoryCompactedRead(any(classOf[Duration]))

    lifecycle.resign(partition, request.identity(), 5, timeout).join()

    assertEquals(0, lifecycle.installedPartitions)
    assertEquals(Seq(
      "partition-begin-5",
      "manager-open-5",
      "partition-install-5",
      "partition-remove-5",
      "manager-resign-5"), events.toSeq)
  }

  @Test
  def testInternalCoordinatorOpenFailsClosedWhenMandatoryCompactedProbeFails(): Unit = {
    val events = mutable.ArrayBuffer.empty[String]
    val topicId = Uuid.randomUuid()
    val topic = Topic.TRANSACTION_STATE_TOPIC_NAME
    val partition = partitionAt(topic, topicId, new AtomicInteger(5), events)
    val manager = new RecordingStorageManager(events)
    val lifecycle = new NereusListOffsetsLifecycle(manager, scanConfig)
    val request = leaderRequest(topic, topicId, 5)
    val probe = new CompletableFuture[Void]
    val storage = storageFor(request)
    when(storage.probeMandatoryCompactedRead(timeout)).thenReturn(probe)

    val opened = lifecycle.openLeader(partition, request)
    manager.completeOpen(5, storage)

    assertFalse(opened.isDone)
    assertEquals(Seq("partition-begin-5", "manager-open-5"), events.toSeq)
    verify(storage).probeMandatoryCompactedRead(timeout)

    probe.completeExceptionally(new NereusException(
      ErrorCode.OBJECT_NOT_FOUND,
      true,
      "all same-view NTC2 candidates are unavailable"))

    assertFailureCode(opened, ErrorCode.OBJECT_NOT_FOUND)
    assertEquals(0, lifecycle.installedPartitions)
    assertEquals(Seq(
      "partition-begin-5",
      "manager-open-5",
      "partition-cancel-5",
      "manager-resign-5"), events.toSeq)
  }

  @Test
  def testInternalCoordinatorInstallsOnlyAfterMandatoryCompactedProbeSucceeds(): Unit = {
    val events = mutable.ArrayBuffer.empty[String]
    val topicId = Uuid.randomUuid()
    val topic = Topic.GROUP_METADATA_TOPIC_NAME
    val partition = partitionAt(topic, topicId, new AtomicInteger(5), events)
    val manager = new RecordingStorageManager(events)
    val lifecycle = new NereusListOffsetsLifecycle(manager, scanConfig)
    val request = leaderRequest(topic, topicId, 5)
    val probe = new CompletableFuture[Void]
    val storage = storageFor(request)
    when(storage.probeMandatoryCompactedRead(timeout)).thenReturn(probe)

    val opened = lifecycle.openLeader(partition, request)
    manager.completeOpen(5, storage)

    assertFalse(opened.isDone)
    assertEquals(0, lifecycle.installedPartitions)
    probe.complete(null)

    assertSame(storage, opened.join())
    assertEquals(1, lifecycle.installedPartitions)
    assertEquals(Seq(
      "partition-begin-5",
      "manager-open-5",
      "partition-install-5"), events.toSeq)
  }

  @Test
  def testLateOldOpenCannotInstallOrRemoveNewLookup(): Unit = {
    val events = mutable.ArrayBuffer.empty[String]
    val topicId = Uuid.randomUuid()
    val leaderEpoch = new AtomicInteger(5)
    val partition = partitionAt(topicId, leaderEpoch, events)
    val manager = new RecordingStorageManager(events)
    val lifecycle = new NereusListOffsetsLifecycle(manager, scanConfig)
    val oldRequest = leaderRequest(topicId, 5)
    val newRequest = leaderRequest(topicId, 6)

    val oldOpen = lifecycle.openLeader(partition, oldRequest)
    leaderEpoch.set(6)
    val newOpen = lifecycle.openLeader(partition, newRequest)

    assertFailureCode(oldOpen, ErrorCode.FENCED_APPEND)
    manager.completeOpen(5, storageFor(oldRequest))
    manager.completeOpen(6, storageFor(newRequest))

    assertTrue(newOpen.isDone)
    assertEquals(1, lifecycle.installedPartitions)
    assertEquals(Seq(
      "partition-begin-5",
      "manager-open-5",
      "partition-begin-6",
      "partition-cancel-5",
      "manager-open-6",
      "manager-resign-5",
      "partition-install-6"), events.toSeq)
  }

  @Test
  def testInstallationFailureResignsRecoveredStorageBeforeFailingOpen(): Unit = {
    val events = mutable.ArrayBuffer.empty[String]
    val topicId = Uuid.randomUuid()
    val partition = partitionAt(topicId, new AtomicInteger(5), events)
    doThrow(new FencedLeaderEpochException("advanced"))
      .when(partition).installLeaderEpochAwareOffsetLookup(anyInt(), any(classOf[LeaderEpochAwareOffsetLookup]))
    val manager = new RecordingStorageManager(events)
    val lifecycle = new NereusListOffsetsLifecycle(manager, scanConfig)
    val request = leaderRequest(topicId, 5)

    val opened = lifecycle.openLeader(partition, request)
    manager.completeOpen(5, storageFor(request))

    assertThrows(classOf[CompletionException], () => opened.join())
    assertEquals(0, lifecycle.installedPartitions)
    assertEquals(Seq(
      "partition-begin-5",
      "manager-open-5",
      "partition-cancel-5",
      "manager-resign-5"), events.toSeq)
  }

  @Test
  def testRequiresStockLeaderEpochPublicationBeforeStorageOpen(): Unit = {
    val events = mutable.ArrayBuffer.empty[String]
    val topicId = Uuid.randomUuid()
    val partition = partitionAt(topicId, new AtomicInteger(4), events)
    val manager = new RecordingStorageManager(events)
    val lifecycle = new NereusListOffsetsLifecycle(manager, scanConfig)

    assertThrows(classOf[FencedLeaderEpochException], () =>
      lifecycle.openLeader(partition, leaderRequest(topicId, 5)))
    assertTrue(events.isEmpty)
  }

  @Test
  def testRejectsStorageOutsideRecoveredLeaderAuthority(): Unit = {
    val events = mutable.ArrayBuffer.empty[String]
    val topicId = Uuid.randomUuid()
    val partition = partitionAt(topicId, new AtomicInteger(5), events)
    val manager = new RecordingStorageManager(events)
    val lifecycle = new NereusListOffsetsLifecycle(manager, scanConfig)
    val request = leaderRequest(topicId, 5)
    val invalid = storageFor(request)
    when(invalid.leaderEpoch()).thenReturn(4)

    val opened = lifecycle.openLeader(partition, request)
    manager.completeOpen(5, invalid)

    assertFailureCode(opened, ErrorCode.METADATA_INVARIANT_VIOLATION)
    assertEquals(Seq(
      "partition-begin-5",
      "manager-open-5",
      "partition-cancel-5",
      "manager-resign-5"), events.toSeq)
  }

  @Test
  def testStaleResignCannotRemoveNewerLookup(): Unit = {
    val events = mutable.ArrayBuffer.empty[String]
    val topicId = Uuid.randomUuid()
    val partition = partitionAt(topicId, new AtomicInteger(6), events)
    val manager = new RecordingStorageManager(events)
    val lifecycle = new NereusListOffsetsLifecycle(manager, scanConfig)
    val request = leaderRequest(topicId, 6)
    val opened = lifecycle.openLeader(partition, request)
    manager.completeOpen(6, storageFor(request))
    opened.join()

    lifecycle.resign(partition, request.identity(), 5, timeout).join()
    assertEquals(1, lifecycle.installedPartitions)
    assertFalse(events.contains("partition-remove-6"))

    lifecycle.resign(partition, request.identity(), 6, timeout).join()
    assertEquals(0, lifecycle.installedPartitions)
    assertEquals(Seq(
      "partition-begin-6",
      "manager-open-6",
      "partition-install-6",
      "manager-resign-5",
      "partition-remove-6",
      "manager-resign-6"), events.toSeq)
  }

  @Test
  def testDeleteAndShutdownRevokeLookupsBeforeManagerLifecycle(): Unit = {
    val events = mutable.ArrayBuffer.empty[String]
    val firstTopicId = Uuid.randomUuid()
    val firstPartition = partitionAt(firstTopicId, new AtomicInteger(5), events)
    val manager = new RecordingStorageManager(events)
    val lifecycle = new NereusListOffsetsLifecycle(manager, scanConfig)
    val firstRequest = leaderRequest(firstTopicId, 5)
    val firstOpen = lifecycle.openLeader(firstPartition, firstRequest)
    manager.completeOpen(5, storageFor(firstRequest))
    firstOpen.join()

    lifecycle.delete(firstPartition, firstRequest.identity(), 11, timeout).join()

    assertTrue(events.indexOf("partition-remove-5") < events.indexOf("manager-delete-11"))

    val secondTopicId = Uuid.randomUuid()
    val secondPartition = partitionAt(secondTopicId, new AtomicInteger(7), events)
    val secondRequest = leaderRequest(secondTopicId, 7)
    val secondOpen = lifecycle.openLeader(secondPartition, secondRequest)
    manager.completeOpen(7, storageFor(secondRequest))
    secondOpen.join()

    lifecycle.beginDrain()

    assertTrue(events.contains("partition-remove-7"))
    assertFalse(events.contains("manager-shutdown"))
    assertEquals(0, lifecycle.installedPartitions)
    assertFailureCode(lifecycle.openLeader(secondPartition, secondRequest), ErrorCode.STORAGE_CLOSED)

    val firstShutdown = lifecycle.shutdown()
    val duplicateShutdown = lifecycle.shutdown()
    assertSame(firstShutdown, duplicateShutdown)
    firstShutdown.join()
    assertTrue(events.indexOf("partition-remove-7") < events.indexOf("manager-shutdown"))
    assertEquals(1, events.count(_ == "manager-shutdown"))
  }

  private def partitionAt(
    topicId: Uuid,
    leaderEpoch: AtomicInteger,
    events: mutable.ArrayBuffer[String]
  ): Partition = partitionAt("events", topicId, leaderEpoch, events)

  private def partitionAt(
    topic: String,
    topicId: Uuid,
    leaderEpoch: AtomicInteger,
    events: mutable.ArrayBuffer[String]
  ): Partition = {
    val partition = mock(classOf[Partition])
    when(partition.topic).thenReturn(topic)
    when(partition.partitionId).thenReturn(0)
    when(partition.topicId).thenReturn(Some(topicId))
    when(partition.topicPartition).thenReturn(new org.apache.kafka.common.TopicPartition(topic, 0))
    when(partition.isLeader).thenReturn(true)
    when(partition.getLeaderEpoch).thenAnswer(_ => leaderEpoch.get())
    when(partition.localLogOrException).thenReturn(mock(classOf[NereusUnifiedLog]))
    doAnswer(invocation => {
      events += s"partition-begin-${invocation.getArgument[Int](0)}"
      null
    }).when(partition).beginLeaderEpochAwareOffsetLookup(anyInt())
    doAnswer(invocation => {
      events += s"partition-install-${invocation.getArgument[Int](0)}"
      null
    }).when(partition).installLeaderEpochAwareOffsetLookup(
      anyInt(), any(classOf[LeaderEpochAwareOffsetLookup]))
    doAnswer(invocation => {
      events += s"partition-remove-${invocation.getArgument[Int](0)}"
      null
    }).when(partition).removeLeaderEpochAwareOffsetLookup(
      anyInt(), any(classOf[LeaderEpochAwareOffsetLookup]))
    doAnswer(invocation => {
      events += s"partition-cancel-${invocation.getArgument[Int](0)}"
      null
    }).when(partition).cancelLeaderEpochAwareOffsetLookup(anyInt())
    partition
  }

  private def leaderRequest(topicId: Uuid, leaderEpoch: Int): KafkaPartitionLeaderOpenRequest =
    leaderRequest("events", topicId, leaderEpoch)

  private def leaderRequest(
    topic: String,
    topicId: Uuid,
    leaderEpoch: Int
  ): KafkaPartitionLeaderOpenRequest =
    new KafkaPartitionLeaderOpenRequest(
      new KafkaPartitionIdentity("kraft", topicId.toString, 0, topic),
      1,
      leaderEpoch,
      9,
      StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT,
      11,
      timeout)

  private def storageFor(request: KafkaPartitionLeaderOpenRequest): KafkaPartitionStorage = {
    val storage = mock(classOf[KafkaPartitionStorage])
    when(storage.identity()).thenReturn(request.identity())
    when(storage.leaderEpoch()).thenReturn(request.leaderEpoch())
    when(storage.storageProfile()).thenReturn(request.storageProfile())
    when(storage.state()).thenReturn(KafkaPartitionState.LEADER_WRITABLE)
    storage
  }

  private def assertFailureCode(completion: CompletableFuture[_], expected: ErrorCode): Unit = {
    val failure = assertThrows(classOf[CompletionException], () => completion.join())
    assertTrue(failure.getCause.isInstanceOf[NereusException])
    assertEquals(expected, failure.getCause.asInstanceOf[NereusException].code())
  }

  private final class RecordingStorageManager(events: mutable.ArrayBuffer[String])
    extends KafkaPartitionStorageManager {
    private val opens = mutable.HashMap.empty[Int, CompletableFuture[KafkaPartitionStorage]]

    override def openLeader(
      request: KafkaPartitionLeaderOpenRequest
    ): CompletableFuture[KafkaPartitionStorage] = {
      events += s"manager-open-${request.leaderEpoch()}"
      val result = new CompletableFuture[KafkaPartitionStorage]
      opens.put(request.leaderEpoch(), result)
      result
    }

    def completeOpen(leaderEpoch: Int, storage: KafkaPartitionStorage): Unit =
      opens(leaderEpoch).complete(storage)

    override def resign(
      identity: KafkaPartitionIdentity,
      observedLeaderEpoch: Int,
      timeout: Duration
    ): CompletableFuture[Void] = {
      events += s"manager-resign-$observedLeaderEpoch"
      CompletableFuture.completedFuture(null)
    }

    override def delete(
      identity: KafkaPartitionIdentity,
      metadataOffset: Long,
      timeout: Duration
    ): CompletableFuture[Void] = {
      events += s"manager-delete-$metadataOffset"
      CompletableFuture.completedFuture(null)
    }

    override def current(identity: KafkaPartitionIdentity): Optional[KafkaPartitionStorage] = Optional.empty()

    override def shutdown(): CompletableFuture[Void] = {
      events += "manager-shutdown"
      CompletableFuture.completedFuture(null)
    }

    override def close(): Unit = {}
  }
}
