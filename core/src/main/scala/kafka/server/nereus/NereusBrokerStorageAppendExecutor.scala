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

import com.nereusstream.api.{AppendOutcome, ErrorCode, NereusException}
import com.nereusstream.kafka.runtime.{KafkaBoundedAppendExecutor, KafkaByteBudget}
import kafka.log.nereus.NereusKafkaExceptionMapper
import kafka.server.LogAppendResult
import kafka.server.storage.BrokerStorageAppendExecutor
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.server.config.NereusKafkaStorageConfig

import java.util.Objects
import java.util.concurrent.{CompletableFuture, CompletionStage}

/** Product-backed append handoff with request-wide byte validation and per-partition FIFO execution. */
final class NereusBrokerStorageAppendExecutor(
  config: NereusKafkaStorageConfig.Append,
  brokerId: Int
) extends BrokerStorageAppendExecutor {
  Objects.requireNonNull(config, "config")

  private val requestBytes = config.requestBytes()
  private val delegate = new KafkaBoundedAppendExecutor(
    config.executorThreads(),
    config.executorQueueCapacity(),
    new KafkaByteBudget(config.inflightBytes()),
    s"nereus-kafka-append-$brokerId")

  override def validateRequest(entries: Iterable[MemoryRecords]): Unit = {
    Objects.requireNonNull(entries, "entries")
    var total = 0L
    entries.foreach { records =>
      Objects.requireNonNull(records, "records")
      try {
        total = Math.addExact(total, records.sizeInBytes.toLong)
      } catch {
        case _: ArithmeticException => rejectRequest(Long.MaxValue)
      }
    }
    if (total > requestBytes) rejectRequest(total)
  }

  override def submit(
    partition: TopicIdPartition,
    records: MemoryRecords,
    append: MemoryRecords => LogAppendResult
  ): CompletionStage[LogAppendResult] = {
    Objects.requireNonNull(partition, "partition")
    Objects.requireNonNull(records, "records")
    Objects.requireNonNull(append, "append")
    val result = new CompletableFuture[LogAppendResult]
    val submitted = delegate.submit(
      partition,
      records.buffer().duplicate(),
      owned => append(MemoryRecords.readableRecords(owned)))
    submitted.whenComplete { (value, failure) =>
      if (failure == null) result.complete(value)
      else result.completeExceptionally(NereusKafkaExceptionMapper.map(failure))
    }
    result
  }

  override def drained: CompletionStage[Void] = delegate.drainedFuture()

  override def close(): Unit = delegate.close()

  private def rejectRequest(actualBytes: Long): Nothing = {
    throw NereusKafkaExceptionMapper.map(new NereusException(
      ErrorCode.BACKPRESSURE_REJECTED,
      true,
      s"Kafka Produce request owns $actualBytes bytes, exceeding the configured limit $requestBytes",
      AppendOutcome.KNOWN_NOT_COMMITTED))
  }
}
