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

package kafka.server

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.share.ShareCoordinatorConfig
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.server.ProcessRole
import org.apache.kafka.server.config.{NereusKafkaConfigs, NereusKafkaStorageConfig, ReplicationConfigs, ServerConfigs, ServerLogConfigs}
import org.apache.kafka.storage.internals.log.CleanerConfig

import java.nio.file.{Path, Paths}
import scala.jdk.CollectionConverters._

/** Pure cross-Kafka validation for the optional Nereus storage mode. */
private[server] object NereusKafkaConfigValidator {
  private val AutoMqElasticStreamEnabledConfig = "elasticstream.enable"

  def validate(config: KafkaConfig, storage: NereusKafkaStorageConfig): Unit = {
    if (!storage.enabled()) return

    requireKRaftRole(config)
    if (config.processRoles.contains(ProcessRole.BrokerRole)) {
      requireSingleReplicaSemantics(config)
      requireConflictingStorageDisabled(config)
      requireRequestLimits(config, storage)
    }
    requireDedicatedDirectories(config, storage)
  }

  private def requireKRaftRole(config: KafkaConfig): Unit = {
    requireCondition(
      config.processRoles.nonEmpty,
      NereusKafkaConfigs.ENABLED_CONFIG,
      true,
      "requires a KRaft broker or controller process role")
  }

  private def requireSingleReplicaSemantics(config: KafkaConfig): Unit = {
    requireExact(
      ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG,
      config.defaultReplicationFactor,
      1)
    requireExact(
      GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG,
      config.getShort(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG),
      1)
    requireExact(
      TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG,
      config.getShort(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG),
      1)
    requireExact(
      ShareCoordinatorConfig.STATE_TOPIC_REPLICATION_FACTOR_CONFIG,
      config.getShort(ShareCoordinatorConfig.STATE_TOPIC_REPLICATION_FACTOR_CONFIG),
      1)
    requireExact(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG, config.minInSyncReplicas, 1)
    requireExact(
      TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG,
      config.getInt(TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG),
      1)
    requireExact(
      ShareCoordinatorConfig.STATE_TOPIC_MIN_ISR_CONFIG,
      config.getShort(ShareCoordinatorConfig.STATE_TOPIC_MIN_ISR_CONFIG),
      1)
  }

  private def requireConflictingStorageDisabled(config: KafkaConfig): Unit = {
    requireCondition(
      !config.remoteLogManagerConfig.isRemoteStorageSystemEnabled,
      "remote.log.storage.system.enable",
      true,
      "must be false when Nereus Kafka storage is enabled")
    requireCondition(
      !config.getBoolean(CleanerConfig.LOG_CLEANER_ENABLE_PROP),
      CleanerConfig.LOG_CLEANER_ENABLE_PROP,
      true,
      "must be false because Nereus owns compaction")
    Option(config.originals.get(AutoMqElasticStreamEnabledConfig)).foreach { configured =>
      val enabled = configured.toString.trim.toLowerCase(java.util.Locale.ROOT) match {
        case "true" => true
        case "false" => false
        case _ => throw new ConfigException(
          AutoMqElasticStreamEnabledConfig,
          configured,
          "must be a boolean when Nereus Kafka storage is enabled")
      }
      requireCondition(
        !enabled,
        AutoMqElasticStreamEnabledConfig,
        configured,
        "must be false because AutoMQ and Nereus storage modes are mutually exclusive")
    }
  }

  private def requireRequestLimits(
    config: KafkaConfig,
    storage: NereusKafkaStorageConfig
  ): Unit = {
    requireCondition(
      config.messageMaxBytes <= NereusKafkaConfigs.MAX_KAFKA_ENTRY_BYTES,
      ServerConfigs.MESSAGE_MAX_BYTES_CONFIG,
      config.messageMaxBytes,
      s"cannot exceed the Nereus Kafka entry limit ${NereusKafkaConfigs.MAX_KAFKA_ENTRY_BYTES}")
    requireCondition(
      storage.append().requestBytes() >= config.socketRequestMaxBytes,
      NereusKafkaConfigs.APPEND_REQUEST_BYTES_CONFIG,
      storage.append().requestBytes(),
      s"must be at least ${SocketServerConfigs.SOCKET_REQUEST_MAX_BYTES_CONFIG}=${config.socketRequestMaxBytes}")
  }

  private def requireDedicatedDirectories(
    config: KafkaConfig,
    storage: NereusKafkaStorageConfig
  ): Unit = {
    val cacheDir = storage.core().cacheDir().orElseThrow()
    val spillDir = storage.retentionCompaction().compactionSpillDir().orElseThrow()
    config.logDirs.asScala.map(normalized).foreach { logDir =>
      requireDisjoint(NereusKafkaConfigs.CACHE_DIR_CONFIG, cacheDir, logDir)
      requireDisjoint(NereusKafkaConfigs.COMPACTION_SPILL_DIR_CONFIG, spillDir, logDir)
    }
  }

  private def normalized(path: String): Path = Paths.get(path).toAbsolutePath.normalize()

  private def requireDisjoint(name: String, path: Path, logDir: Path): Unit = {
    requireCondition(
      !path.startsWith(logDir) && !logDir.startsWith(path),
      name,
      path,
      s"must not overlap Kafka authoritative log directory $logDir")
  }

  private def requireExact(name: String, actual: Number, expected: Int): Unit = {
    requireCondition(
      actual.longValue() == expected,
      name,
      actual,
      s"must equal $expected when Nereus Kafka storage is enabled")
  }

  private def requireCondition(condition: Boolean, name: String, value: Any, message: String): Unit = {
    if (!condition) throw new ConfigException(name, value, message)
  }
}
