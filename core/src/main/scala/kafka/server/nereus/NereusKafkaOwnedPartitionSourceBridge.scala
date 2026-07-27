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
import com.nereusstream.kafka.retention.KafkaPartitionMaintenanceRuntime
import kafka.log.nereus.NereusUnifiedLog
import kafka.server.ReplicaManager

import java.util
import java.util.Objects
import java.util.concurrent.{CompletableFuture, CompletionStage}
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._

/**
 * One-time bridge from pre-ReplicaManager product construction to current fork-owned leaders.
 */
final class NereusKafkaOwnedPartitionSourceBridge
  extends KafkaPartitionMaintenanceRuntime.OwnedPartitionSource {

  private val delegate = new AtomicReference[ReplicaManager]()

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
}
