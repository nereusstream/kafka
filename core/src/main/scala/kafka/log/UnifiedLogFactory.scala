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

package kafka.log

import org.apache.kafka.common.Uuid
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log.{LogConfig, LogDirFailureChannel, LogOffsetsListener, ProducerStateManagerConfig, UnifiedLog}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

import java.io.File
import java.util.Optional
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

/**
 * Stock-owned construction seam for partition logs. The local implementation is the exact stock behavior. Optional
 * authoritative-storage implementations are injected per BrokerServer and are never discovered from global state.
 */
trait UnifiedLogFactory {
  def logDirectories(configuredLogDirectories: collection.Seq[File]): collection.Seq[File] =
    configuredLogDirectories

  def initialOfflineDirectories(
    configuredInitialOfflineDirectories: collection.Seq[File],
    selectedLogDirectories: collection.Seq[File]
  ): collection.Seq[File] = configuredInitialOfflineDirectories

  def loadExistingLogs: Boolean = true

  def scheduleLocalMaintenance: Boolean = true

  def open(context: UnifiedLogOpenContext): UnifiedLog
}

final case class UnifiedLogOpenContext(
  dir: File,
  config: LogConfig,
  logStartOffset: Long,
  recoveryPoint: Long,
  scheduler: Scheduler,
  brokerTopicStats: BrokerTopicStats,
  time: Time,
  maxTransactionTimeoutMs: Int,
  producerStateManagerConfig: ProducerStateManagerConfig,
  producerIdExpirationCheckIntervalMs: Int,
  logDirFailureChannel: LogDirFailureChannel,
  lastShutdownClean: Boolean,
  topicId: Optional[Uuid],
  numRemainingSegments: ConcurrentMap[String, Integer],
  remoteStorageSystemEnable: Boolean,
  logOffsetsListener: LogOffsetsListener,
  isFuture: Boolean
)

object UnifiedLogFactory {
  val Local: UnifiedLogFactory = context => UnifiedLog.create(
    context.dir,
    context.config,
    context.logStartOffset,
    context.recoveryPoint,
    context.scheduler,
    context.brokerTopicStats,
    context.time,
    context.maxTransactionTimeoutMs,
    context.producerStateManagerConfig,
    context.producerIdExpirationCheckIntervalMs,
    context.logDirFailureChannel,
    context.lastShutdownClean,
    context.topicId,
    context.numRemainingSegments,
    context.remoteStorageSystemEnable,
    context.logOffsetsListener)

  def newSegmentCounter(): ConcurrentMap[String, Integer] =
    new ConcurrentHashMap[String, Integer]()
}
