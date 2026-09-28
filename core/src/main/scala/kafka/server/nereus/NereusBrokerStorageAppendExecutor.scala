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
import kafka.server.storage.BrokerStorageAppendExecutor
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.server.config.NereusKafkaStorageConfig

import java.nio.ByteBuffer
import java.util.concurrent.{ArrayBlockingQueue, CompletableFuture, CompletionStage, ThreadPoolExecutor, TimeUnit}
import scala.collection.mutable

/** Bounded ownership of request bytes and per-partition FIFO invocation of the stock append work. */
final class NereusBrokerStorageAppendExecutor(config: NereusKafkaStorageConfig.Append, brokerId: Int)
  extends BrokerStorageAppendExecutor {
  private val guard = new Object
  private val maximum = Math.addExact(config.executorThreads(), config.executorQueueCapacity())
  private val tails = mutable.HashMap.empty[TopicIdPartition, CompletableFuture[LogAppendResult]]
  private val completed = new CompletableFuture[Void]
  private val workers = new ThreadPoolExecutor(config.executorThreads(), config.executorThreads(), 0L, TimeUnit.MILLISECONDS,
    new ArrayBlockingQueue[Runnable](maximum), task => {
      val thread = new Thread(task, s"nereus-kafka-append-$brokerId"); thread.setDaemon(true); thread
    }, new ThreadPoolExecutor.AbortPolicy)
  private var outstanding = 0
  private var bytes = 0L
  private var closed = false

  override def validateRequest(entries: Iterable[MemoryRecords]): Unit = {
    val count = entries.foldLeft(0L)((total, records) => Math.addExact(total, records.sizeInBytes.toLong))
    if (count > config.requestBytes()) throw rejected("Produce request exceeds byte capacity")
  }
  override def submit(partition: TopicIdPartition, records: MemoryRecords,
                      append: MemoryRecords => LogAppendResult): CompletionStage[LogAppendResult] = {
    val result = new CompletableFuture[LogAppendResult]
    val length = records.sizeInBytes.toLong
    val (previous, owned) = guard.synchronized {
      if (closed || outstanding >= maximum || length > config.inflightBytes() - bytes) {
        return CompletableFuture.failedFuture(rejected("native append executor capacity exhausted"))
      }
      val copy = ByteBuffer.allocate(records.sizeInBytes)
      copy.put(records.buffer().duplicate()); copy.flip()
      outstanding += 1; bytes += length
      val predecessor = tails.get(partition)
      tails.put(partition, result)
      (predecessor, MemoryRecords.readableRecords(copy))
    }
    def terminate(value: LogAppendResult, failure: Throwable): Unit = {
      guard.synchronized {
        outstanding -= 1; bytes -= length
        if (tails.get(partition).contains(result)) tails.remove(partition)
        if (closed && outstanding == 0) { workers.shutdown(); completed.complete(null) }
      }
      if (failure == null) result.complete(value) else result.completeExceptionally(failure)
    }
    def run(): Unit = {
      try workers.execute(() => {
        try terminate(append(owned), null) catch { case failure: Throwable => terminate(null, failure) }
      }) catch { case failure: Throwable => terminate(null, failure) }
    }
    previous match { case Some(before) => before.whenComplete((_, _) => run()); case None => run() }
    result.copy()
  }
  override def drained: CompletionStage[Void] = completed.copy()
  override def close(): Unit = guard.synchronized {
    closed = true
    if (outstanding == 0) { workers.shutdown(); completed.complete(null) }
  }
  private def rejected(message: String) = new ThrottlingQuotaExceededException(message)
}
