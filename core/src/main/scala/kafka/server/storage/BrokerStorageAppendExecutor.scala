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

package kafka.server.storage

import kafka.server.LogAppendResult
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.record.MemoryRecords

import java.util.concurrent.CompletionStage

/**
 * Stock-owned, optional handoff for blocking authoritative-storage appends.
 *
 * Implementations must copy the exact remaining record bytes before submit returns, preserve FIFO execution for one
 * TopicIdPartition, reject before invoking append work, and continue admitted work even when a returned future is
 * cancelled. ReplicaManager uses RequestLocal.noCaching on executor threads.
 */
trait BrokerStorageAppendExecutor extends AutoCloseable {
  /** Validates request-wide limits before any partition is submitted. */
  def validateRequest(entries: Iterable[MemoryRecords]): Unit

  /** Submits one partition append. The supplied operation is invoked at most once with executor-owned records. */
  def submit(
    partition: TopicIdPartition,
    records: MemoryRecords,
    append: MemoryRecords => LogAppendResult
  ): CompletionStage[LogAppendResult]

  /** Completes after close has stopped admission and every admitted append has terminated. */
  def drained: CompletionStage[Void]
}
