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

package kafka.log.nereus

import com.nereusstream.api.{ErrorCode, NereusException}
import com.nereusstream.kafka.partition.KafkaPartitionIdentity
import kafka.log.{UnifiedLogFactory, UnifiedLogOpenContext}
import kafka.server.storage.BrokerStorageRuntimeContext

import org.apache.kafka.common.Uuid
import org.apache.kafka.storage.internals.log.UnifiedLog

import java.io.File
import java.util.Objects

/** Selects one ephemeral cache root and creates only fail-closed Nereus UnifiedLog shells. */
final class NereusUnifiedLogFactory(context: BrokerStorageRuntimeContext) extends UnifiedLogFactory {
  Objects.requireNonNull(context, "context")

  private val cacheRoot = context.config.nereusKafkaStorageConfig.core().cacheDir().orElseThrow()
    .resolve(context.config.brokerId.toString)
    .resolve("partition-logs")
    .toFile
    .getAbsoluteFile

  override def logDirectories(
    configuredLogDirectories: collection.Seq[File]
  ): collection.Seq[File] = Seq(cacheRoot)

  override def initialOfflineDirectories(
    configuredInitialOfflineDirectories: collection.Seq[File],
    selectedLogDirectories: collection.Seq[File]
  ): collection.Seq[File] = Seq.empty

  override def loadExistingLogs: Boolean = false

  override def scheduleLocalMaintenance: Boolean = false

  override def open(openContext: UnifiedLogOpenContext): UnifiedLog = {
    Objects.requireNonNull(openContext, "openContext")
    if (openContext.dir.getAbsoluteFile.getParentFile != cacheRoot) {
      throw invariant("Nereus log shell directory is outside the selected broker cache root")
    }
    if (openContext.isFuture) {
      throw new UnsupportedOperationException("Nereus authoritative storage does not support Kafka future logs")
    }
    if (openContext.logStartOffset != 0L || openContext.recoveryPoint != 0L) {
      throw invariant("Nereus log shells cannot recover from local Kafka checkpoints")
    }
    val topicId = openContext.topicId.orElseThrow(() =>
      invariant("Nereus log creation requires a non-zero KRaft topic ID"))
    if (topicId == Uuid.ZERO_UUID) {
      throw invariant("Nereus log creation requires a non-zero KRaft topic ID")
    }
    val topicPartition = UnifiedLog.parseTopicPartitionName(openContext.dir)
    val identity = new KafkaPartitionIdentity(
      context.clusterId,
      topicId.toString,
      topicPartition.partition,
      topicPartition.topic)
    NereusUnifiedLog.create(
      openContext.dir,
      openContext.config,
      openContext.scheduler,
      openContext.brokerTopicStats,
      openContext.time,
      openContext.maxTransactionTimeoutMs,
      openContext.producerStateManagerConfig,
      openContext.producerIdExpirationCheckIntervalMs,
      openContext.logDirFailureChannel,
      topicId,
      identity,
      context.config.nereusKafkaStorageConfig.append().timeout(),
      context.config.nereusKafkaStorageConfig.fetch().timeout(),
      Math.toIntExact(context.config.nereusKafkaStorageConfig.fetch().maxResponseBytes()),
      openContext.logOffsetsListener)
  }

  private def invariant(message: String): NereusException =
    new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message)
}
