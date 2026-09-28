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
import kafka.server.storage.BrokerStorageFetchExecutor
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.server.config.NereusKafkaStorageConfig
import org.apache.kafka.server.storage.log.FetchParams
import org.apache.kafka.storage.internals.log.LogReadResult

import java.util.concurrent.{ArrayBlockingQueue, CompletableFuture, CompletionStage, ScheduledExecutorService, ScheduledFuture, ThreadPoolExecutor, TimeUnit}
import scala.collection.Seq
import scala.collection.mutable

/** One event-driven stock Fetch wave at a time, with bounded request admission and a final deadline read. */
final class NereusBrokerStorageFetchExecutor(config: NereusKafkaStorageConfig.Fetch, brokerId: Int,
    lifecycle: NereusListOffsetsLifecycle, scheduler: ScheduledExecutorService) extends BrokerStorageFetchExecutor {
  private val guard = new Object
  private val maximum = Math.addExact(config.executorThreads(), config.executorQueueCapacity())
  private val operations = mutable.Set.empty[Operation]
  private val drainedRoot = new CompletableFuture[Void]
  private val workers = new ThreadPoolExecutor(config.executorThreads(), config.executorThreads(), 0L, TimeUnit.MILLISECONDS,
    new ArrayBlockingQueue[Runnable](maximum), task => {
      val thread = new Thread(task, s"nereus-kafka-fetch-$brokerId"); thread.setDaemon(true); thread
    }, new ThreadPoolExecutor.AbortPolicy)
  private var closed = false
  @volatile private var ownedBytes = 0L

  override def submit(params: FetchParams, infos: Seq[(TopicIdPartition, PartitionData)],
      read: Boolean => Seq[(TopicIdPartition, LogReadResult)]): CompletionStage[Seq[(TopicIdPartition, LogReadResult)]] = {
    if (infos.isEmpty) return CompletableFuture.completedFuture(Seq.empty)
    val byteBudget = Math.max(Math.min(config.maxResponseBytes(), Math.max(0, params.maxBytes).toLong), config.maxEntryBytes())
    val operation = new Operation(params, infos.toVector, read, byteBudget)
    guard.synchronized {
      if (closed || operations.size >= maximum || byteBudget > config.inflightBytes() - ownedBytes) return CompletableFuture.failedFuture(new ThrottlingQuotaExceededException("native Fetch capacity exhausted"))
      operations += operation
      ownedBytes = Math.addExact(ownedBytes, byteBudget)
    }
    operation.start()
    operation.result.copy()
  }
  override def drained: CompletionStage[Void] = drainedRoot.copy()
  override def close(): Unit = {
    val pending = guard.synchronized { closed = true; operations.toVector }
    pending.foreach(_.deadline())
    guard.synchronized { shutdownIfDrained() }
  }
  private def shutdownIfDrained(): Unit = if (closed && operations.isEmpty) { workers.shutdown(); drainedRoot.complete(null) }
  private final class Operation(params: FetchParams, infos: Seq[(TopicIdPartition, PartitionData)],
      read: Boolean => Seq[(TopicIdPartition, LogReadResult)], byteBudget: Long) {
    val result = new CompletableFuture[Seq[(TopicIdPartition, LogReadResult)]]
    private val lock = new Object
    private val subscriptions = mutable.ArrayBuffer.empty[AutoCloseable]
    private var timer: ScheduledFuture[_] = _
    private var running = false
    private var signaled = false
    private var expired = params.maxWaitMs <= 0
    private var waves = 0
    private var done = false
    private var started = false
    def start(): Unit = {
      try {
        infos.foreach { case (identity, _) => subscriptions += lifecycle.subscribe(identity, () => wakeup()) }
        lock.synchronized {
          started = true
          timer = scheduler.schedule((() => deadline()): Runnable, Math.max(0L, params.maxWaitMs), TimeUnit.MILLISECONDS)
          queue()
        }
      } catch { case failure: Throwable => finish(null, failure) }
    }
    def wakeup(): Unit = lock.synchronized { signaled = true; if (started && !running && !done) queue() }
    def deadline(): Unit = lock.synchronized { expired = true; if (started && !running && !done) queue() }
    private def queue(): Unit = {
      if (done || running || (!expired && waves > config.operationMaxRereads())) return
      running = true; signaled = false
      val initial = waves == 0
      val finalWave = expired
      waves += 1
      try workers.execute(() => {
        try {
          val response = read(initial).toVector
          if (response.size != infos.size || !response.iterator.zip(infos.iterator).forall { case ((actual, _), (expected, _)) => actual == expected }) {
            throw new IllegalStateException("native Fetch wave changed partition order")
          }
          val size = response.foldLeft(0L) { case (total, (_, value)) => Math.addExact(total, value.info.records.sizeInBytes.toLong) }
          if (size > byteBudget) throw new IllegalStateException("native Fetch wave exceeded its admitted byte budget")
          val terminal = response.exists { case (_, value) => value.error != Errors.NONE || value.divergingEpoch.isPresent ||
            value.preferredReadReplica.isPresent || value.info.delayedRemoteStorageFetch.isPresent }
          lock.synchronized {
            running = false
            if (finalWave || terminal || size >= params.minBytes) finish(response, null)
            else if (expired || signaled) queue()
          }
        } catch { case failure: Throwable => finish(null, failure) }
      }) catch { case failure: Throwable => finish(null, failure) }
    }
    private def finish(value: Seq[(TopicIdPartition, LogReadResult)], failure: Throwable): Unit = {
      val terminal = lock.synchronized { if (done) false else { done = true; if (timer != null) timer.cancel(false); true } }
      if (!terminal) return
      subscriptions.foreach(_.close())
      guard.synchronized { operations -= this; ownedBytes = Math.subtractExact(ownedBytes, byteBudget); shutdownIfDrained() }
      if (failure == null) result.complete(value) else result.completeExceptionally(failure)
    }
  }
}
