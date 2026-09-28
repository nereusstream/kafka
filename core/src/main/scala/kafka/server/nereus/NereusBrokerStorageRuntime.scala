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

import kafka.log.UnifiedLogFactory
import kafka.log.nereus.{NereusListOffsetsLifecycle, NereusTopicDeltaLifecycle, NereusUnifiedLogFactory}
import kafka.server.ReplicaManager
import kafka.server.metadata.AsyncTopicDeltaLifecycle
import kafka.server.storage.{BrokerStorageAppendExecutor, BrokerStorageDrainReason, BrokerStorageFetchExecutor, BrokerStorageRuntime, BrokerStorageRuntimeContext}

import java.time.Duration
import java.util.concurrent.{CompletableFuture, CompletionStage, TimeUnit}

/** Owns the existing native request handoffs and one shared BK provider lifecycle. */
final class NereusBrokerStorageRuntime(context: BrokerStorageRuntimeContext, provider: NereusKafkaOwnedProviderRuntime)
  extends BrokerStorageRuntime {
  private val guard = new Object
  private val partitions = new NereusListOffsetsLifecycle(context, provider)
  private val append = new NereusBrokerStorageAppendExecutor(context.config.nereusKafkaStorageConfig.append(), context.config.brokerId)
  private val fetch = new NereusBrokerStorageFetchExecutor(context.config.nereusKafkaStorageConfig.fetch(), context.config.brokerId,
    partitions, context.scheduler.scheduledExecutorService())
  private val logs = new NereusUnifiedLogFactory(context)
  private var replicaManager: ReplicaManager = _
  private var lifecycle: NereusTopicDeltaLifecycle = _
  private var closed = false
  override def start(): CompletionStage[Void] = provider.start()
  override def unifiedLogFactory: UnifiedLogFactory = logs
  override def appendExecutor: Option[BrokerStorageAppendExecutor] = Some(append)
  override def fetchExecutor: Option[BrokerStorageFetchExecutor] = Some(fetch)
  override def asyncTopicDeltaLifecycle(manager: ReplicaManager): Option[AsyncTopicDeltaLifecycle] = guard.synchronized {
    if (closed) throw new IllegalStateException("native BK runtime is draining")
    if (lifecycle == null) {
      replicaManager = manager
      lifecycle = new NereusTopicDeltaLifecycle(context.clusterId, context.config.brokerId, context.brokerEpochSupplier,
        context.scheduler.scheduledExecutorService(), () => context.time.milliseconds(),
        context.config.nereusKafkaStorageConfig.lifecycle().recoveryTimeout(), manager, partitions)
    } else if (!(replicaManager eq manager)) throw new IllegalArgumentException("native BK runtime has another ReplicaManager")
    Some(lifecycle)
  }
  override def beginDrain(reason: BrokerStorageDrainReason): CompletionStage[Void] = {
    guard.synchronized { closed = true }
    partitions.beginDrain(); provider.fence(); append.close(); fetch.close()
    CompletableFuture.completedFuture(null)
  }
  override def awaitDrained(timeout: Duration): CompletionStage[Void] =
    CompletableFuture.allOf(append.drained.toCompletableFuture, fetch.drained.toCompletableFuture).orTimeout(timeout.toMillis, TimeUnit.MILLISECONDS)
  override def close(): Unit = {
    beginDrain(BrokerStorageDrainReason.BrokerShutdown)
    provider.close()
  }
}
