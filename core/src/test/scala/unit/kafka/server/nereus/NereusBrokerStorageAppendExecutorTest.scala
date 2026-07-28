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

import kafka.server.LogAppendResult
import org.apache.kafka.common.{TopicIdPartition, Uuid}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.server.config.NereusKafkaStorageConfig
import org.apache.kafka.storage.internals.log.LogAppendInfo
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertFalse, assertThrows}
import org.junit.jupiter.api.Test

import java.nio.ByteBuffer
import java.time.Duration
import java.util.concurrent.{CopyOnWriteArrayList, CountDownLatch, TimeUnit}
import scala.jdk.CollectionConverters._

class NereusBrokerStorageAppendExecutorTest {
  @Test
  def testCopiesBeforeReturnAndPreservesPerPartitionOrderThroughDrain(): Unit = {
    val executor = new NereusBrokerStorageAppendExecutor(appendConfig(requestBytes = 1024 * 1024), 7)
    val partition = new TopicIdPartition(Uuid.randomUuid(), 0, "ordered")
    val firstStarted = new CountDownLatch(1)
    val releaseFirst = new CountDownLatch(1)
    val observed = new CopyOnWriteArrayList[Int]
    val firstRecords = records("first")
    val secondRecords = records("second")
    val expectedSecond = exactBytes(secondRecords)

    val first = executor.submit(partition, firstRecords, owned => {
      assertFalse(owned.buffer().isReadOnly)
      firstStarted.countDown()
      assertEquals(true, releaseFirst.await(5, TimeUnit.SECONDS))
      observed.add(1)
      success
    }).toCompletableFuture
    assertEquals(true, firstStarted.await(5, TimeUnit.SECONDS))

    val second = executor.submit(partition, secondRecords, owned => {
      assertArrayEquals(expectedSecond, exactBytes(owned))
      observed.add(2)
      success
    }).toCompletableFuture

    val source = secondRecords.buffer()
    source.put(source.position(), (source.get(source.position()) ^ 0x01).toByte)
    executor.close()
    val drained = executor.drained.toCompletableFuture
    assertFalse(drained.isDone)

    releaseFirst.countDown()
    first.get(5, TimeUnit.SECONDS)
    second.get(5, TimeUnit.SECONDS)
    drained.get(5, TimeUnit.SECONDS)
    assertEquals(Seq(1, 2), observed.asScala.toSeq)
  }

  @Test
  def testRejectsOversizedRequestBeforeSubmit(): Unit = {
    val executor = new NereusBrokerStorageAppendExecutor(appendConfig(requestBytes = 1), 7)
    try {
      assertThrows(
        classOf[ThrottlingQuotaExceededException],
        () => executor.validateRequest(List(records("too-large"))))
    } finally {
      executor.close()
      executor.drained.toCompletableFuture.get(5, TimeUnit.SECONDS)
    }
  }

  private def appendConfig(requestBytes: Long): NereusKafkaStorageConfig.Append =
    new NereusKafkaStorageConfig.Append(
      Duration.ofSeconds(5),
      2,
      4,
      2 * 1024 * 1024,
      requestBytes,
      Duration.ofSeconds(30),
      Duration.ofSeconds(5),
      3)

  private def records(value: String): MemoryRecords =
    MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(value.getBytes))

  private def exactBytes(records: MemoryRecords): Array[Byte] = {
    val source: ByteBuffer = records.buffer().duplicate()
    val bytes = new Array[Byte](source.remaining())
    source.get(bytes)
    bytes
  }

  private def success: LogAppendResult =
    LogAppendResult(LogAppendInfo.UNKNOWN_LOG_APPEND_INFO, None, hasCustomErrorMessage = false)
}
