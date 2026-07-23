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

import kafka.utils.TestUtils
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.share.ShareCoordinatorConfig
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.server.config.{NereusKafkaConfigs, ReplicationConfigs, ServerConfigs, ServerLogConfigs}
import org.apache.kafka.storage.internals.log.CleanerConfig
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util.Properties

class NereusKafkaConfigValidatorTest {
  @Test
  def testDisabledModeDoesNotApplyNereusCrossKafkaRestrictions(): Unit = {
    val properties = TestUtils.createBrokerConfig(0)
    properties.put(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "3")
    properties.put(CleanerConfig.LOG_CLEANER_ENABLE_PROP, "true")

    val config = KafkaConfig.fromProps(properties, false)

    assertFalse(config.nereusKafkaStorageConfig.enabled())
  }

  @Test
  def testEnabledModePublishesValidatedTypedSnapshot(): Unit = {
    val config = KafkaConfig.fromProps(enabledProperties(), false)

    assertTrue(config.nereusKafkaStorageConfig.enabled())
    assertEquals("nereus-cluster", config.nereusKafkaStorageConfig.core().cluster().orElseThrow())
    assertEquals(1, config.defaultReplicationFactor)
    assertFalse(config.remoteLogManagerConfig.isRemoteStorageSystemEnabled)
  }

  @Test
  def testEnabledModeRejectsReplicationCleanerRemoteAndRequestConflicts(): Unit = {
    Seq[(String, Any, String)](
      (ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, 2,
        ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG),
      (GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, 2,
        GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG),
      (TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, 2,
        TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG),
      (ShareCoordinatorConfig.STATE_TOPIC_REPLICATION_FACTOR_CONFIG, 2,
        ShareCoordinatorConfig.STATE_TOPIC_REPLICATION_FACTOR_CONFIG),
      (ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG, 2,
        ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG),
      (TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, 2,
        TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG),
      (ShareCoordinatorConfig.STATE_TOPIC_MIN_ISR_CONFIG, 2,
        ShareCoordinatorConfig.STATE_TOPIC_MIN_ISR_CONFIG),
      (CleanerConfig.LOG_CLEANER_ENABLE_PROP, true, CleanerConfig.LOG_CLEANER_ENABLE_PROP),
      ("remote.log.storage.system.enable", true, "remote.log.storage.system.enable"),
      (ServerConfigs.MESSAGE_MAX_BYTES_CONFIG,
        NereusKafkaConfigs.MAX_KAFKA_ENTRY_BYTES + 1,
        ServerConfigs.MESSAGE_MAX_BYTES_CONFIG),
      (SocketServerConfigs.SOCKET_REQUEST_MAX_BYTES_CONFIG,
        NereusKafkaConfigs.APPEND_REQUEST_BYTES_DEFAULT + 1,
        NereusKafkaConfigs.APPEND_REQUEST_BYTES_CONFIG)
    ).foreach { case (name, value, expectedKey) =>
      assertInvalid(name, value, expectedKey)
    }
  }

  @Test
  def testEnabledModeRejectsAutoMqAndAuthoritativeDirectoryOverlap(): Unit = {
    assertInvalid("elasticstream.enable", true, "elasticstream.enable")
    assertInvalid("elasticstream.enable", "not-a-boolean", "elasticstream.enable")

    val properties = enabledProperties()
    properties.put(NereusKafkaConfigs.CACHE_DIR_CONFIG, "/tmp/nereus-kafka-stock-log/cache")
    val failure = assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(properties, false))
    assertTrue(failure.getMessage.contains(NereusKafkaConfigs.CACHE_DIR_CONFIG), failure.getMessage)
  }

  private def assertInvalid(name: String, value: Any, expectedKey: String): Unit = {
    val properties = enabledProperties()
    properties.put(name, value.toString)
    val failure = assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(properties, false))
    assertTrue(failure.getMessage.contains(expectedKey), failure.getMessage)
  }

  private def enabledProperties(): Properties = {
    val properties = TestUtils.createBrokerConfig(0)
    properties.put(ServerLogConfigs.LOG_DIRS_CONFIG, "/tmp/nereus-kafka-stock-log")
    properties.put(NereusKafkaConfigs.ENABLED_CONFIG, "true")
    properties.put(NereusKafkaConfigs.CLUSTER_CONFIG, "nereus-cluster")
    properties.put(NereusKafkaConfigs.OXIA_SERVICE_ADDRESS_CONFIG, "oxia://127.0.0.1:6648")
    properties.put(NereusKafkaConfigs.CACHE_DIR_CONFIG, "/tmp/nereus-kafka-cache")
    properties.put(NereusKafkaConfigs.BOOKKEEPER_METADATA_SERVICE_URI_CONFIG, "bk://127.0.0.1/ledgers")
    properties.put(NereusKafkaConfigs.OBJECT_PROVIDER_CONFIG, "s3")
    properties.put(NereusKafkaConfigs.OBJECT_BUCKET_CONFIG, "nereus-kafka")
    properties.put(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "1")
    properties.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
    properties.put(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
    properties.put(ShareCoordinatorConfig.STATE_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
    properties.put(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
    properties.put(TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, "1")
    properties.put(ShareCoordinatorConfig.STATE_TOPIC_MIN_ISR_CONFIG, "1")
    properties.put(CleanerConfig.LOG_CLEANER_ENABLE_PROP, "false")
    properties.put("remote.log.storage.system.enable", "false")
    properties
  }
}
