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

import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.server.storage.log.FetchParams
import org.apache.kafka.storage.internals.log.LogReadResult

import java.util.concurrent.CompletionStage
import scala.collection.Seq

/**
 * Stock-owned, optional handoff for a complete Fetch request.
 *
 * Implementations must subscribe to partition change signals before the initial read, invoke the supplied stock read
 * wave only on bounded storage workers, keep at most one wave in flight for a request, and complete after an actual-byte
 * minBytes result, a stock terminal fact, or one final deadline wave. The boolean supplied to read is true only for the
 * initial wave; later calls are delayed-fetch-equivalent rereads.
 */
trait BrokerStorageFetchExecutor extends AutoCloseable {
  def submit(
    params: FetchParams,
    fetchInfos: Seq[(TopicIdPartition, PartitionData)],
    read: Boolean => Seq[(TopicIdPartition, LogReadResult)]
  ): CompletionStage[Seq[(TopicIdPartition, LogReadResult)]]

  /** Completes after close has stopped admission and every admitted Fetch callback has terminated. */
  def drained: CompletionStage[Void]
}
