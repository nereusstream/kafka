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

import kafka.server.KafkaConfig
import kafka.utils.TestUtils

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.share.ShareCoordinatorConfig
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.metadata.KRaftMetadataCache
import org.apache.kafka.server.config.{NereusKafkaConfigs, ReplicationConfigs, ServerLogConfigs}
import org.apache.kafka.server.util.KafkaScheduler
import org.apache.kafka.storage.internals.log.CleanerConfig
import org.junit.jupiter.api.Assertions.{assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock

import java.time.Duration
import java.util.Properties

class BrokerStorageRuntimeFactoryTest {
  @Test
  def testDisabledFactoryIsAnInertStockRuntime(): Unit = {
    val runtime = BrokerStorageRuntimeFactory.Disabled.create(context(KafkaConfig.fromProps(
      TestUtils.createBrokerConfig(0), false)))

    assertTrue(runtime.start().toCompletableFuture.isDone)
    assertFalse(runtime.start().toCompletableFuture.isCompletedExceptionally)
    assertTrue(runtime.asyncTopicDeltaLifecycle(mock(classOf[kafka.server.ReplicaManager])).isEmpty)
    assertTrue(runtime.beginDrain(BrokerStorageDrainReason.BrokerShutdown).toCompletableFuture.isDone)
    assertTrue(runtime.awaitDrained(Duration.ofSeconds(1)).toCompletableFuture.isDone)
    runtime.close()
  }

  @Test
  def testEnabledModeFailsClosedWithoutAnExplicitRuntimeFactory(): Unit = {
    val failure = assertThrows(classOf[ConfigException], () =>
      BrokerStorageRuntimeFactory.Disabled.create(context(KafkaConfig.fromProps(
        enabledProperties(), false))))

    assertTrue(failure.getMessage.contains(NereusKafkaConfigs.ENABLED_CONFIG), failure.getMessage)
    assertTrue(failure.getMessage.contains("explicitly installed"), failure.getMessage)
  }

  @Test
  def testContextRejectsMissingBorrowedDependencies(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(0), false)
    assertThrows(classOf[NullPointerException], () => BrokerStorageRuntimeContext(
      config,
      "cluster-id",
      () => 1L,
      null,
      Time.SYSTEM,
      mock(classOf[Metrics]),
      mock(classOf[KafkaScheduler])))
  }

  private def context(config: KafkaConfig): BrokerStorageRuntimeContext = BrokerStorageRuntimeContext(
    config,
    "cluster-id",
    () => 1L,
    mock(classOf[KRaftMetadataCache]),
    Time.SYSTEM,
    mock(classOf[Metrics]),
    mock(classOf[KafkaScheduler]))

  private def enabledProperties(): Properties = {
    val properties = TestUtils.createBrokerConfig(0)
    properties.put(ServerLogConfigs.LOG_DIRS_CONFIG, "/tmp/nereus-kafka-runtime-stock-log")
    properties.put(NereusKafkaConfigs.ENABLED_CONFIG, "true")
    properties.put(NereusKafkaConfigs.CLUSTER_CONFIG, "nereus-cluster")
    properties.put(NereusKafkaConfigs.OXIA_SERVICE_ADDRESS_CONFIG, "oxia://127.0.0.1:6648")
    properties.put(NereusKafkaConfigs.CACHE_DIR_CONFIG, "/tmp/nereus-kafka-runtime-cache")
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
