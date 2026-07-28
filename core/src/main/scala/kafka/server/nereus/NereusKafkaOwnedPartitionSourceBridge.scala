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
import com.nereusstream.kafka.compaction.{
  KafkaCompactionPartitionPass,
  KafkaCompactionRuntime,
  KafkaCompactionScheduler,
  KafkaCompactionStrategyV1
}
import com.nereusstream.kafka.retention.KafkaPartitionMaintenanceRuntime
import com.nereusstream.materialization.{
  MaterializationPolicy,
  MaterializationPolicyFactory,
  TopicCompactionSpec
}
import com.nereusstream.objectstore.compacted.KafkaCompactionKeyEncodingV2
import kafka.log.nereus.NereusUnifiedLog
import kafka.server.ReplicaManager
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.server.config.NereusKafkaStorageConfig

import java.util
import java.util.Objects
import java.util.concurrent.{CompletableFuture, CompletionStage}
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._

/**
 * One-time bridge from pre-ReplicaManager product construction to current fork-owned leaders.
 */
final class NereusKafkaOwnedPartitionSourceBridge
  extends KafkaPartitionMaintenanceRuntime.OwnedPartitionSource
    with KafkaCompactionRuntime.OwnedPartitionSource {

  private val delegate = new AtomicReference[ReplicaManager]()
  private val compaction =
    new AtomicReference[NereusUnifiedLog.CompactionConfiguration]()

  def configureCompaction(
    storage: NereusKafkaStorageConfig,
    nereusBuild: String
  ): Unit = {
    val exactStorage = Objects.requireNonNull(storage, "storage")
    val configured = exactStorage.retentionCompaction()
    val exact = new NereusUnifiedLog.CompactionConfiguration(
      MaterializationPolicyFactory.kafkaTopicCompacted(
        new TopicCompactionSpec(
          KafkaCompactionStrategyV1.STRATEGY_ID,
          KafkaCompactionStrategyV1.STRATEGY_VERSION,
          KafkaCompactionKeyEncodingV2.ID),
        2,
        MaterializationPolicy.MAX_SOURCE_RANGES,
        Math.min(
          configured.compactionTaskMaxRecords(),
          MaterializationPolicy.MAX_RANGE_RECORDS),
        Math.min(
          configured.compactionTaskMaxSourceBytes(),
          MaterializationPolicy.MAX_TARGET_OBJECT_BYTES),
        Math.min(
          exactStorage.lifecycle().recoveryChunkRecords(),
          MaterializationPolicy.MAX_ROW_GROUP_RECORDS),
        "ZSTD"),
      configured.compactionTaskMaxRecords(),
      configured.compactionKeyMaxBytes(),
      Math.min(
        configured.compactionSpillMaxBytes(),
        configured.compactionTaskMaxSourceBytes()),
      new KafkaCompactionPartitionPass.WriteSettings(
        Objects.requireNonNull(nereusBuild, "nereusBuild"),
        false))
    val current = compaction.get()
    if (current == exact) {
      return
    }
    if (!compaction.compareAndSet(null, exact)) {
      throw new IllegalStateException(
        "Nereus Kafka compaction bridge is already configured")
    }
  }

  def bind(replicaManager: ReplicaManager): Unit = {
    val exact = Objects.requireNonNull(replicaManager, "replicaManager")
    val current = delegate.get()
    if (current eq exact) {
      return
    }
    if (!delegate.compareAndSet(null, exact)) {
      throw new IllegalStateException(
        "Nereus Kafka owned-partition source is already bound")
    }
  }

  def bound: Boolean = delegate.get() != null

  override def snapshot(
    maximumPartitions: Int
  ): CompletionStage[util.List[KafkaPartitionMaintenanceRuntime.OwnedPartition]] = {
    val replicaManager = delegate.get()
    if (replicaManager == null) {
      return CompletableFuture.failedFuture(new NereusException(
        ErrorCode.METADATA_UNAVAILABLE,
        true,
        "Kafka owned-partition source is not bound to ReplicaManager"))
    }
    try {
      val registrations = replicaManager
        .nereusOnlineLeaderPartitions(maximumPartitions)
        .asScala
        .map { partition =>
          partition.leaderLogIfLocal match {
            case Some(log: NereusUnifiedLog) =>
              val leaderEpoch = partition.getLeaderEpoch
              new KafkaPartitionMaintenanceRuntime.OwnedPartition(
                log.nereusIdentity,
                leaderEpoch,
                log.maintenanceHooks(
                  leaderEpoch,
                  partition.nereusMaintenanceAuthority(log)))
            case _ =>
              throw new IllegalStateException(
                "Nereus leader snapshot returned a non-Nereus log")
          }
        }
        .toList
        .asJava
      CompletableFuture.completedFuture(registrations)
    } catch {
      case failure: Throwable => CompletableFuture.failedFuture(failure)
    }
  }

  override def snapshot(
    triggers: KafkaCompactionScheduler.TriggerBatch,
    maximumPartitions: Int
  ): CompletionStage[util.List[KafkaCompactionRuntime.OwnedPartition]] = {
    Objects.requireNonNull(triggers, "triggers")
    val replicaManager = delegate.get()
    val configured = compaction.get()
    if (replicaManager == null || configured == null) {
      return CompletableFuture.failedFuture(new NereusException(
        ErrorCode.METADATA_UNAVAILABLE,
        true,
        "Kafka compaction source is not bound and configured"))
    }
    try {
      val registrations = replicaManager
        .nereusOnlineLeaderPartitions(maximumPartitions)
        .asScala
        .map { partition =>
          partition.leaderLogIfLocal match {
            case Some(log: NereusUnifiedLog) =>
              val leaderEpoch = partition.getLeaderEpoch
              val workClass =
                if (Topic.isInternal(log.nereusIdentity.observedTopicName())) {
                  KafkaCompactionRuntime.WorkClass.INTERNAL
                } else {
                  KafkaCompactionRuntime.WorkClass.USER
                }
              new KafkaCompactionRuntime.OwnedPartition(
                log.nereusIdentity,
                leaderEpoch,
                workClass,
                log.compactionCaptureProvider(
                  leaderEpoch,
                  partition.nereusMaintenanceAuthority(log),
                  configured))
            case _ =>
              throw new IllegalStateException(
                "Nereus leader snapshot returned a non-Nereus log")
          }
        }
        .toList
        .asJava
      CompletableFuture.completedFuture(registrations)
    } catch {
      case failure: Throwable => CompletableFuture.failedFuture(failure)
    }
  }
}
