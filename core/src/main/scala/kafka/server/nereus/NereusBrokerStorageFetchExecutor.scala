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
import com.nereusstream.kafka.fetch.{KafkaFetchWaveOperation, KafkaFetchWaveSource}
import com.nereusstream.kafka.partition.{KafkaPartitionEventSubscription, KafkaPartitionIdentity, KafkaPartitionStorageManager}
import kafka.log.nereus.NereusKafkaExceptionMapper
import kafka.server.storage.BrokerStorageFetchExecutor
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.server.config.NereusKafkaStorageConfig
import org.apache.kafka.server.storage.log.FetchParams
import org.apache.kafka.storage.internals.log.LogReadResult

import java.time.Duration
import java.util.Objects
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ArrayBlockingQueue, CompletableFuture, CompletionStage, ScheduledExecutorService, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import java.util.function.{Predicate, ToIntFunction}
import scala.collection.Seq
import scala.collection.mutable

/**
 * Product-backed whole-request Fetch handoff.
 *
 * The stock read wave remains the single owner of leader-epoch, divergence, follower-state, quota and request-order byte
 * semantics. This class owns only bounded admission, partition event subscriptions and operation lifecycle.
 */
final class NereusBrokerStorageFetchExecutor(
  config: NereusKafkaStorageConfig.Fetch,
  kafkaClusterId: String,
  brokerId: Int,
  storageManager: KafkaPartitionStorageManager,
  deadlineScheduler: ScheduledExecutorService
) extends BrokerStorageFetchExecutor {
  Objects.requireNonNull(config, "config")
  Objects.requireNonNull(kafkaClusterId, "kafkaClusterId")
  Objects.requireNonNull(storageManager, "storageManager")
  Objects.requireNonNull(deadlineScheduler, "deadlineScheduler")

  private val guard = new Object
  private val maxOutstanding = Math.addExact(config.executorThreads(), config.executorQueueCapacity())
  private val operations = mutable.Set.empty[KafkaFetchWaveOperation[Seq[(TopicIdPartition, LogReadResult)]]]
  // Logical admission already bounds runners to maxOutstanding. Keeping that many physical queue slots prevents a burst of
  // signals for already-admitted waiting operations from being rejected before idle workers can dequeue their control tasks.
  private val readExecutor = new TrackingExecutor(
    config.executorThreads(),
    maxOutstanding,
    threadFactory(s"nereus-kafka-fetch-read-$brokerId"))
  private val callbackExecutor = new TrackingExecutor(
    1,
    maxOutstanding,
    threadFactory(s"nereus-kafka-fetch-callback-$brokerId"))
  private val drainedRoot = CompletableFuture.allOf(readExecutor.terminatedFuture, callbackExecutor.terminatedFuture)
  private var closed = false

  override def submit(
    params: FetchParams,
    suppliedFetchInfos: Seq[(TopicIdPartition, PartitionData)],
    readWave: Boolean => Seq[(TopicIdPartition, LogReadResult)]
  ): CompletionStage[Seq[(TopicIdPartition, LogReadResult)]] = {
    Objects.requireNonNull(params, "params")
    Objects.requireNonNull(suppliedFetchInfos, "fetchInfos")
    Objects.requireNonNull(readWave, "read")
    val fetchInfos = suppliedFetchInfos.toVector
    if (fetchInfos.isEmpty) {
      return CompletableFuture.completedFuture(Seq.empty)
    }

    val operation = new KafkaFetchWaveOperation[Seq[(TopicIdPartition, LogReadResult)]](
      source(fetchInfos, readWave),
      params.minBytes,
      Duration.ofMillis(Math.max(0L, params.maxWaitMs)),
      config.operationMaxRereads(),
      responseBytes,
      forceComplete,
      readExecutor,
      callbackExecutor,
      deadlineScheduler)
    guard.synchronized {
      if (closed) return failed(ErrorCode.STORAGE_CLOSED, "Kafka Fetch executor is closed")
      if (operations.size >= maxOutstanding) {
        return failed(ErrorCode.BACKPRESSURE_REJECTED, "Kafka Fetch executor logical capacity is exhausted")
      }
      operations += operation
    }

    val result = new CompletableFuture[Seq[(TopicIdPartition, LogReadResult)]]
    operation.start().whenComplete { (terminal, failure) =>
      try {
        if (failure == null) {
          result.complete(terminal.response())
        } else {
          result.completeExceptionally(NereusKafkaExceptionMapper.map(failure))
        }
      } finally {
        guard.synchronized {
          operations -= operation
          shutdownIfDrained()
        }
      }
    }
    result.copy()
  }

  override def drained: CompletionStage[Void] = drainedRoot.thenApply(_ => null)

  override def close(): Unit = guard.synchronized {
    closed = true
    shutdownIfDrained()
  }

  private def source(
    fetchInfos: Seq[(TopicIdPartition, PartitionData)],
    readWave: Boolean => Seq[(TopicIdPartition, LogReadResult)]
  ): KafkaFetchWaveSource[Seq[(TopicIdPartition, LogReadResult)]] =
    new KafkaFetchWaveSource[Seq[(TopicIdPartition, LogReadResult)]] {
      override def read(initialWave: Boolean): CompletionStage[Seq[(TopicIdPartition, LogReadResult)]] = {
        val results = Objects.requireNonNull(
          readWave(initialWave),
          "stock Kafka Fetch read wave").toVector
        if (results.size != fetchInfos.size ||
          !results.iterator.zip(fetchInfos.iterator).forall { case ((actual, _), (expected, _)) => actual == expected }) {
          throw new IllegalStateException("stock Kafka Fetch wave changed request partition order or cardinality")
        }
        CompletableFuture.completedFuture(results)
      }

      override def subscribe(wakeup: Runnable): AutoCloseable = {
        Objects.requireNonNull(wakeup, "wakeup")
        val subscriptions = mutable.ArrayBuffer.empty[KafkaPartitionEventSubscription]
        try {
          fetchInfos.foreach { case (partition, _) =>
            val identity = new KafkaPartitionIdentity(
              kafkaClusterId,
              partition.topicId.toString,
              partition.partition,
              partition.topic)
            storageManager.current(identity).ifPresent { storage =>
              subscriptions += storage.subscribe(_ => wakeup.run())
            }
          }
        } catch {
          case failure: Throwable =>
            subscriptions.reverseIterator.foreach(subscription => closeQuietly(subscription))
            throw failure
        }
        new AutoCloseable {
          override def close(): Unit =
            subscriptions.reverseIterator.foreach(subscription => closeQuietly(subscription))
        }
      }
    }

  private def shutdownIfDrained(): Unit = {
    if (closed && operations.isEmpty) {
      readExecutor.shutdown()
      callbackExecutor.shutdown()
    }
  }

  private def failed(
    code: ErrorCode,
    message: String
  ): CompletionStage[Seq[(TopicIdPartition, LogReadResult)]] =
    CompletableFuture.failedFuture(NereusKafkaExceptionMapper.map(
      new NereusException(code, code == ErrorCode.BACKPRESSURE_REJECTED, message)))

  private def threadFactory(prefix: String): ThreadFactory = {
    val threadId = new AtomicInteger
    task => {
      val thread = new Thread(task, s"$prefix-${threadId.incrementAndGet()}")
      thread.setDaemon(true)
      thread
    }
  }

  private def closeQuietly(subscription: KafkaPartitionEventSubscription): Unit = {
    try subscription.close()
    catch {
      case _: Throwable =>
    }
  }

  private val responseBytes = new ToIntFunction[Seq[(TopicIdPartition, LogReadResult)]] {
    override def applyAsInt(results: Seq[(TopicIdPartition, LogReadResult)]): Int =
      results.foldLeft(0) { case (total, (_, result)) =>
        Math.addExact(total, result.info.records.sizeInBytes)
      }
  }

  private val forceComplete = new Predicate[Seq[(TopicIdPartition, LogReadResult)]] {
    override def test(results: Seq[(TopicIdPartition, LogReadResult)]): Boolean =
      results.exists { case (_, result) =>
        result.error != Errors.NONE ||
          result.divergingEpoch.isPresent ||
          result.preferredReadReplica.isPresent ||
          result.info.delayedRemoteStorageFetch.isPresent
      }
  }

  private final class TrackingExecutor(
    threads: Int,
    queueCapacity: Int,
    factory: ThreadFactory
  ) extends ThreadPoolExecutor(
    threads,
    threads,
    0L,
    TimeUnit.MILLISECONDS,
    new ArrayBlockingQueue[Runnable](queueCapacity),
    factory,
    new ThreadPoolExecutor.AbortPolicy) {
    val terminatedFuture = new CompletableFuture[Void]

    override protected def terminated(): Unit = terminatedFuture.complete(null)
  }
}
