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
import org.apache.kafka.common.test.{KafkaClusterTestKit, TestKitNodes}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.share.ShareCoordinatorConfig
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.metadata.KRaftMetadataCache
import org.apache.kafka.server.config.{NereusKafkaConfigs, ReplicationConfigs, ServerLogConfigs}
import org.apache.kafka.server.util.KafkaScheduler
import org.apache.kafka.storage.internals.log.CleanerConfig
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock

import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

class BrokerStorageRuntimeFactoryTest {
  @Test
  def testDisabledFactoryIsAnInertStockRuntime(): Unit = {
    val runtime = BrokerStorageRuntimeFactory.Disabled.create(context(KafkaConfig.fromProps(
      TestUtils.createBrokerConfig(0), false)))

    assertTrue(runtime.start().toCompletableFuture.isDone)
    assertFalse(runtime.start().toCompletableFuture.isCompletedExceptionally)
    assertTrue(runtime.appendExecutor.isEmpty)
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

  @Test
  def testBrokerShutdownRetainsFirstStorageCloseFailureAfterFinalRetry(): Unit = {
    val first = new IllegalStateException("first storage close failure")
    val second = new IllegalStateException("second storage close failure")
    val attempts = new AtomicInteger()
    val factory: BrokerStorageRuntimeFactory = context => {
      val stock = BrokerStorageRuntimeFactory.Disabled.create(context)
      new BrokerStorageRuntime {
        override def start() = stock.start()
        override def unifiedLogFactory = stock.unifiedLogFactory
        override def appendExecutor = stock.appendExecutor
        override def fetchExecutor = stock.fetchExecutor
        override def asyncTopicDeltaLifecycle(replicaManager: kafka.server.ReplicaManager) =
          stock.asyncTopicDeltaLifecycle(replicaManager)
        override def beginDrain(reason: BrokerStorageDrainReason) = stock.beginDrain(reason)
        override def awaitDrained(timeout: Duration) = stock.awaitDrained(timeout)
        override def close(): Unit = if (attempts.incrementAndGet() == 1) throw first else throw second
      }
    }
    val cluster = new KafkaClusterTestKit.Builder(new TestKitNodes.Builder()
      .setNumBrokerNodes(1)
      .setNumControllerNodes(1)
      .build())
      .setBrokerStorageRuntimeFactory(factory)
      .build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      val broker = cluster.brokers().values().iterator().next()
      val failure = assertThrows(classOf[IllegalStateException], () => broker.shutdown())
      assertSame(first, failure)
      assertEquals(2, attempts.get())
      assertEquals(1, failure.getSuppressed.length)
      assertSame(second, failure.getSuppressed.head)
      assertTrue(broker.isShutdown)
    } finally {
      cluster.close()
    }
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
    properties.put(NereusKafkaConfigs.PROFILE_CONFIG, "OBJECT_WAL_SYNC_OBJECT")
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
