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

import com.nereusstream.api.StorageProfile
import com.nereusstream.kafka.partition.KafkaPartitionStorageManager
import com.nereusstream.kafka.runtime.{DrainReason, NereusKafkaRuntime}
import kafka.log.nereus.{NereusListOffsetsLifecycle, NereusListOffsetsScanConfig, NereusTopicDeltaLifecycle}
import kafka.server.ReplicaManager
import kafka.server.metadata.AsyncTopicDeltaLifecycle
import kafka.server.storage.{BrokerStorageDrainReason, BrokerStorageRuntime, BrokerStorageRuntimeContext}
import org.apache.kafka.server.config.NereusKafkaStorageConfig

import java.time.Duration
import java.util.Objects
import java.util.concurrent.CompletionStage

/** Binds one product-owned runtime to the exact BrokerServer and ReplicaManager that consume it. */
final class NereusBrokerStorageRuntime(
  context: BrokerStorageRuntimeContext,
  delegate: NereusKafkaRuntime,
  scanConfig: NereusListOffsetsScanConfig
) extends BrokerStorageRuntime {
  Objects.requireNonNull(context, "context")
  Objects.requireNonNull(delegate, "delegate")
  Objects.requireNonNull(scanConfig, "scanConfig")

  private val guard = new Object
  private val storageManager: KafkaPartitionStorageManager = Objects.requireNonNull(
    delegate.partitionStorageManager(),
    "Nereus runtime partition manager")
  private var metadataLifecycle: MetadataLifecycle = _
  private var draining = false
  private var closed = false

  override def start(): CompletionStage[Void] = {
    guard.synchronized {
      if (draining || closed) {
        throw new IllegalStateException("Nereus broker storage runtime cannot start after drain or close")
      }
    }
    Objects.requireNonNull(delegate.start(), "Nereus runtime start future")
  }

  override def asyncTopicDeltaLifecycle(replicaManager: ReplicaManager): Option[AsyncTopicDeltaLifecycle] = {
    Objects.requireNonNull(replicaManager, "replicaManager")
    guard.synchronized {
      if (draining || closed) {
        throw new IllegalStateException("Nereus metadata lifecycle cannot be created after drain or close")
      }
      if (metadataLifecycle == null) {
        val partitionLifecycle = new NereusListOffsetsLifecycle(storageManager, scanConfig)
        val topicLifecycle = new NereusTopicDeltaLifecycle(
          context.clusterId,
          context.config.brokerId,
          context.brokerEpochSupplier,
          storageProfile(context.config.nereusKafkaStorageConfig.core().profile()),
          context.config.nereusKafkaStorageConfig.lifecycle().recoveryTimeout(),
          replicaManager,
          partitionLifecycle)
        metadataLifecycle = new MetadataLifecycle(replicaManager, partitionLifecycle, topicLifecycle)
      } else if (!(metadataLifecycle.replicaManager eq replicaManager)) {
        throw new IllegalArgumentException("Nereus broker runtime is already bound to a different ReplicaManager")
      }
      Some(metadataLifecycle.topicLifecycle)
    }
  }

  override def beginDrain(reason: BrokerStorageDrainReason): CompletionStage[Void] = {
    Objects.requireNonNull(reason, "reason")
    val partitionLifecycle = guard.synchronized {
      draining = true
      Option(metadataLifecycle).map(_.partitionLifecycle)
    }
    try {
      Objects.requireNonNull(delegate.beginDrain(drainReason(reason)), "Nereus runtime drain future")
    } finally {
      partitionLifecycle.foreach(_.beginDrain())
    }
  }

  override def awaitDrained(timeout: Duration): CompletionStage[Void] =
    Objects.requireNonNull(delegate.awaitDrained(timeout), "Nereus runtime drained future")

  override def close(): Unit = {
    val partitionLifecycle = guard.synchronized {
      if (closed) {
        return
      }
      draining = true
      closed = true
      Option(metadataLifecycle).map(_.partitionLifecycle)
    }
    partitionLifecycle.foreach(_.beginDrain())
    delegate.close()
  }

  private def drainReason(reason: BrokerStorageDrainReason): DrainReason = reason match {
    case BrokerStorageDrainReason.BrokerShutdown => DrainReason.BROKER_SHUTDOWN
    case BrokerStorageDrainReason.StartupFailure => DrainReason.STARTUP_FAILURE
    case BrokerStorageDrainReason.BrokerFenced => DrainReason.BROKER_FENCED
    case BrokerStorageDrainReason.OperatorRequest => DrainReason.OPERATOR_REQUEST
  }

  private def storageProfile(profile: NereusKafkaStorageConfig.Profile): StorageProfile = profile match {
    case NereusKafkaStorageConfig.Profile.OBJECT_WAL_SYNC_OBJECT => StorageProfile.OBJECT_WAL_SYNC_OBJECT
    case NereusKafkaStorageConfig.Profile.OBJECT_WAL_ASYNC_OBJECT => StorageProfile.OBJECT_WAL_ASYNC_OBJECT
    case NereusKafkaStorageConfig.Profile.BOOKKEEPER_WAL_ONLY => StorageProfile.BOOKKEEPER_WAL_ONLY
    case NereusKafkaStorageConfig.Profile.BOOKKEEPER_WAL_ASYNC_OBJECT => StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT
    case NereusKafkaStorageConfig.Profile.BOOKKEEPER_WAL_SYNC_OBJECT => StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT
  }

  private final class MetadataLifecycle(
    val replicaManager: ReplicaManager,
    val partitionLifecycle: NereusListOffsetsLifecycle,
    val topicLifecycle: NereusTopicDeltaLifecycle
  )
}
