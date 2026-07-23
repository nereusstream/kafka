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

import kafka.log.UnifiedLogFactory
import kafka.server.ReplicaManager
import kafka.server.metadata.AsyncTopicDeltaLifecycle

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.server.config.NereusKafkaConfigs

import java.time.Duration
import java.util.concurrent.{CompletableFuture, CompletionStage}

/** Explicit factory injection point; no reflection or process-global provider registry is used. */
trait BrokerStorageRuntimeFactory {
  def create(context: BrokerStorageRuntimeContext): BrokerStorageRuntime
}

object BrokerStorageRuntimeFactory {
  val Disabled: BrokerStorageRuntimeFactory = context => {
    if (context.config.nereusKafkaStorageConfig.enabled()) {
      throw new ConfigException(
        NereusKafkaConfigs.ENABLED_CONFIG,
        true,
        "requires an explicitly installed BrokerStorageRuntimeFactory")
    }
    DisabledBrokerStorageRuntime
  }

  private object DisabledBrokerStorageRuntime extends BrokerStorageRuntime {
    private val completed = CompletableFuture.completedFuture[Void](null)

    override def start(): CompletionStage[Void] = completed

    override def unifiedLogFactory: UnifiedLogFactory = UnifiedLogFactory.Local

    override def appendExecutor: Option[BrokerStorageAppendExecutor] = None

    override def fetchExecutor: Option[BrokerStorageFetchExecutor] = None

    override def asyncTopicDeltaLifecycle(replicaManager: ReplicaManager): Option[AsyncTopicDeltaLifecycle] = {
      require(replicaManager != null, "replicaManager must be non-null")
      None
    }

    override def beginDrain(reason: BrokerStorageDrainReason): CompletionStage[Void] = {
      require(reason != null, "reason must be non-null")
      completed
    }

    override def awaitDrained(timeout: Duration): CompletionStage[Void] = {
      require(timeout != null && !timeout.isNegative && !timeout.isZero,
        "timeout must be positive")
      completed
    }

    override def close(): Unit = { }
  }
}
