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

import kafka.log.nereus.NereusListOffsetsLifecycle
import kafka.utils.TestUtils
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.server.config.NereusKafkaStorageConfig
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams}
import org.apache.kafka.storage.internals.log.{FetchDataInfo, LogOffsetMetadata, LogReadResult}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}

import java.time.Duration
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import java.util.concurrent.{CompletionException, CopyOnWriteArrayList, Executors, TimeUnit}
import java.util.{Optional, OptionalLong}
import scala.collection.Seq

class NereusBrokerStorageFetchExecutorTest {
  @Test
  def testEventRereadUsesStockWaveOnBoundedWorkerAndDrainsAfterClose(): Unit = {
    val scheduler = Executors.newSingleThreadScheduledExecutor()
    val fixture = storageFixture()
    val executor = new NereusBrokerStorageFetchExecutor(
      fetchConfig(threads = 1, queueCapacity = 2),
      4,
      fixture.lifecycle,
      scheduler)
    val reads = new AtomicInteger
    val readThread = new AtomicReference[String]
    try {
      val completion = executor.submit(
        fetchParams(maxWaitMs = 5000, minBytes = 1),
        Seq(fixture.partition -> partitionData(fixture.topicId)),
        initial => {
          readThread.set(Thread.currentThread().getName)
          val attempt = reads.incrementAndGet()
          assertEquals(attempt == 1, initial)
          Seq(fixture.partition -> successResult(
            if (attempt == 1) MemoryRecords.EMPTY
            else MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("ready".getBytes))))
        }).toCompletableFuture

      TestUtils.waitUntilTrue(
        () => reads.get() == 1 && fixture.listener.get() != null,
        "initial Fetch wave did not subscribe and enter waiting")
      assertFalse(completion.isDone)

      fixture.listener.get().run()

      val result = completion.get(5, TimeUnit.SECONDS)
      assertEquals(2, reads.get())
      assertEquals(1, result.size)
      assertTrue(result.head._2.info.records.sizeInBytes > 0)
      assertTrue(readThread.get().startsWith("nereus-kafka-fetch-4"))
      assertTrue(fixture.subscriptionClosed.get())

      executor.close()
      executor.drained.toCompletableFuture.get(5, TimeUnit.SECONDS)
    } finally {
      executor.close()
      scheduler.shutdownNow()
    }
  }

  @Test
  def testLogicalCapacityRejectsBeforeThirdReadAndAcceptedRequestsReachDeadline(): Unit = {
    val scheduler = Executors.newSingleThreadScheduledExecutor()
    val fixture = storageFixture()
    val executor = new NereusBrokerStorageFetchExecutor(
      fetchConfig(threads = 1, queueCapacity = 1),
      5,
      fixture.lifecycle,
      scheduler)
    val reads = new AtomicInteger
    val responseReady = new AtomicBoolean
    try {
      def submit() = executor.submit(
        fetchParams(maxWaitMs = 30000, minBytes = 1),
        Seq(fixture.partition -> partitionData(fixture.topicId)),
        _ => {
          reads.incrementAndGet()
          val records =
            if (responseReady.get()) MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("ready".getBytes))
            else MemoryRecords.EMPTY
          Seq(fixture.partition -> successResult(records))
        }).toCompletableFuture

      val first = submit()
      val second = submit()
      TestUtils.waitUntilTrue(() => reads.get() == 2, "two admitted Fetch operations did not read")

      val rejected = submit()
      val failure = assertThrows(classOf[CompletionException], () => rejected.join())
      assertTrue(failure.getCause.isInstanceOf[
        org.apache.kafka.common.errors.ThrottlingQuotaExceededException])
      assertEquals(2, reads.get())

      responseReady.set(true)
      fixture.listeners.forEach(_.run())
      executor.close()
      first.get(5, TimeUnit.SECONDS)
      second.get(5, TimeUnit.SECONDS)
      executor.drained.toCompletableFuture.get(5, TimeUnit.SECONDS)
      assertEquals(4, reads.get())
    } finally {
      executor.close()
      scheduler.shutdownNow()
    }
  }

  @Test
  def testByteCapacityRejectsBeforeReadAndIsReleasedOnCompletion(): Unit = {
    val scheduler = Executors.newSingleThreadScheduledExecutor()
    val fixture = storageFixture()
    val executor = new NereusBrokerStorageFetchExecutor(
      fetchConfig(threads = 2, queueCapacity = 2, inflightBytes = 1024 * 1024),
      6, fixture.lifecycle, scheduler)
    val reads = new AtomicInteger
    val ready = new AtomicBoolean
    try {
      def submit() = executor.submit(
        fetchParams(maxWaitMs = 30000, minBytes = 1),
        Seq(fixture.partition -> partitionData(fixture.topicId)),
        _ => {
          reads.incrementAndGet()
          Seq(fixture.partition -> successResult(
            if (ready.get()) MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("ready".getBytes))
            else MemoryRecords.EMPTY))
        }).toCompletableFuture
      val first = submit()
      TestUtils.waitUntilTrue(() => reads.get() == 1, "first Fetch did not consume its byte admission")
      val rejected = assertThrows(classOf[CompletionException], () => submit().join())
      assertTrue(rejected.getCause.isInstanceOf[org.apache.kafka.common.errors.ThrottlingQuotaExceededException])
      assertEquals(1, reads.get())
      ready.set(true)
      fixture.listener.get().run()
      first.get(5, TimeUnit.SECONDS)
      submit().get(5, TimeUnit.SECONDS)
      assertEquals(3, reads.get())
      executor.close()
      executor.drained.toCompletableFuture.get(5, TimeUnit.SECONDS)
    } finally {
      executor.close()
      scheduler.shutdownNow()
    }
  }

  private def storageFixture(): StorageFixture = {
    val topicId = Uuid.randomUuid()
    val partition = new TopicIdPartition(topicId, new TopicPartition("topic", 0))
    val lifecycle = mock(classOf[NereusListOffsetsLifecycle])
    val listener = new AtomicReference[Runnable]
    val listeners = new CopyOnWriteArrayList[Runnable]
    val subscriptionClosed = new AtomicBoolean
    when(lifecycle.subscribe(any(classOf[TopicIdPartition]), any(classOf[Runnable]))).thenAnswer { invocation =>
      val exactListener = invocation.getArgument(1, classOf[Runnable])
      listener.set(exactListener); listeners.add(exactListener)
      (() => { listeners.remove(exactListener); subscriptionClosed.set(true); () }): AutoCloseable
    }
    StorageFixture(topicId, partition, lifecycle, listener, listeners, subscriptionClosed)
  }

  private def successResult(records: MemoryRecords): LogReadResult =
    new LogReadResult(
      new FetchDataInfo(new LogOffsetMetadata(0L), records),
      Optional.empty(),
      1L,
      0L,
      1L,
      0L,
      0L,
      OptionalLong.of(1L),
      Errors.NONE)

  private def fetchParams(maxWaitMs: Long, minBytes: Int): FetchParams =
    new FetchParams(
      FetchRequest.ORDINARY_CONSUMER_ID,
      -1L,
      maxWaitMs,
      minBytes,
      1024 * 1024,
      FetchIsolation.HIGH_WATERMARK,
      Optional.empty())

  private def partitionData(topicId: Uuid): PartitionData =
    new PartitionData(
      topicId,
      0L,
      0L,
      1024 * 1024,
      Optional.empty(),
      Optional.empty())

  private def fetchConfig(
    threads: Int,
    queueCapacity: Int,
    inflightBytes: Long = 8L * 1024 * 1024
  ): NereusKafkaStorageConfig.Fetch =
    new NereusKafkaStorageConfig.Fetch(
      Duration.ofSeconds(5),
      threads,
      queueCapacity,
      inflightBytes,
      1024 * 1024,
      Math.min(4L * 1024 * 1024, inflightBytes),
      4)

  private case class StorageFixture(
    topicId: Uuid,
    partition: TopicIdPartition,
    lifecycle: NereusListOffsetsLifecycle,
    listener: AtomicReference[Runnable],
    listeners: CopyOnWriteArrayList[Runnable],
    subscriptionClosed: AtomicBoolean)
}
