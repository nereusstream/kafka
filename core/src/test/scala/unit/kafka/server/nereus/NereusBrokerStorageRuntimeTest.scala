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

import com.nereusstream.kafka.partition.KafkaPartitionStorageManager
import com.nereusstream.kafka.runtime.{DrainReason, NereusKafkaRuntime}
import kafka.log.nereus.NereusListOffsetsScanConfig
import kafka.server.{KafkaConfig, ReplicaManager}
import kafka.server.storage.{BrokerStorageDrainReason, BrokerStorageRuntimeContext}
import kafka.utils.TestUtils

import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.share.ShareCoordinatorConfig
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.metadata.KRaftMetadataCache
import org.apache.kafka.server.config.{NereusKafkaConfigs, ReplicationConfigs, ServerLogConfigs}
import org.apache.kafka.server.util.KafkaScheduler
import org.apache.kafka.storage.internals.log.CleanerConfig
import org.junit.jupiter.api.Assertions.{assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, never, times, verify, when}

import java.time.Duration
import java.util.Properties
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Function

class NereusBrokerStorageRuntimeTest {
  private val scanConfig = new NereusListOffsetsScanConfig(
    100,
    1024 * 1024,
    4096,
    1024 * 1024,
    10,
    Duration.ofSeconds(5))

  @Test
  def testFactoryCreatesOnlyForEnabledModeAndRequiresExactTypedProducts(): Unit = {
    val delegate = runtimeMock()
    val runtimeCreations = new AtomicInteger
    val scanConfigCreations = new AtomicInteger
    val factory = new NereusBrokerStorageRuntimeFactory(
      context => {
        runtimeCreations.incrementAndGet()
        delegate
      },
      context => {
        scanConfigCreations.incrementAndGet()
        scanConfig
      })

    val disabled = factory.create(context(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0), false)))
    assertTrue(disabled.asyncTopicDeltaLifecycle(mock(classOf[ReplicaManager])).isEmpty)
    assertTrue(disabled.start().toCompletableFuture.isDone)
    assertTrue(runtimeCreations.get() == 0)
    assertTrue(scanConfigCreations.get() == 0)

    val enabled = factory.create(context(KafkaConfig.fromProps(enabledProperties(), false)))
    assertTrue(enabled.isInstanceOf[NereusBrokerStorageRuntime])
    assertTrue(runtimeCreations.get() == 1)
    assertTrue(scanConfigCreations.get() == 1)

    val nullRuntimeFactory = new NereusBrokerStorageRuntimeFactory(
      _ => null,
      _ => scanConfig)
    assertThrows(classOf[NullPointerException], () =>
      nullRuntimeFactory.create(context(KafkaConfig.fromProps(enabledProperties(), false))))
  }

  @Test
  def testRuntimeBindsOneReplicaManagerAndDelegatesOrderedLifecycle(): Unit = {
    val delegate = runtimeMock()
    val started = new CompletableFuture[Void]
    val drained = new CompletableFuture[Void]
    when(delegate.start()).thenReturn(started)
    when(delegate.beginDrain(any(classOf[DrainReason]))).thenReturn(drained)
    when(delegate.awaitDrained(any(classOf[Duration]))).thenReturn(drained)
    val runtime = new NereusBrokerStorageRuntime(
      context(KafkaConfig.fromProps(enabledProperties(), false)),
      delegate,
      scanConfig)

    assertSame(started, runtime.start())
    val replicaManager = mock(classOf[ReplicaManager])
    val lifecycle = runtime.asyncTopicDeltaLifecycle(replicaManager)
    assertTrue(lifecycle.nonEmpty)
    assertSame(lifecycle.get, runtime.asyncTopicDeltaLifecycle(replicaManager).get)
    assertThrows(classOf[IllegalArgumentException], () =>
      runtime.asyncTopicDeltaLifecycle(mock(classOf[ReplicaManager])))

    assertSame(drained, runtime.beginDrain(BrokerStorageDrainReason.BrokerShutdown))
    verify(delegate).beginDrain(DrainReason.BROKER_SHUTDOWN)
    assertThrows(classOf[IllegalStateException], () => runtime.asyncTopicDeltaLifecycle(replicaManager))
    assertSame(drained, runtime.awaitDrained(Duration.ofSeconds(1)))

    runtime.close()
    runtime.close()
    verify(delegate, times(1)).close()
  }

  @Test
  def testFactoryDoesNotEvaluateScanConfigAfterRuntimeCreationFailure(): Unit = {
    val scanConfigCreator = mock(classOf[Function[BrokerStorageRuntimeContext, NereusListOffsetsScanConfig]])
    val failure = new IllegalStateException("provider creation failed")
    val factory = new NereusBrokerStorageRuntimeFactory(
      _ => throw failure,
      scanConfigCreator)

    val actual = assertThrows(classOf[IllegalStateException], () =>
      factory.create(context(KafkaConfig.fromProps(enabledProperties(), false))))

    assertSame(failure, actual)
    verify(scanConfigCreator, never()).apply(any(classOf[BrokerStorageRuntimeContext]))
  }

  @Test
  def testFactoryClosesCreatedRuntimeWhenLaterAssemblyFails(): Unit = {
    val delegate = runtimeMock()
    val closeFailure = new IllegalStateException("close failed")
    org.mockito.Mockito.doThrow(closeFailure).when(delegate).close()
    val assemblyFailure = new IllegalArgumentException("scan config failed")
    val factory = new NereusBrokerStorageRuntimeFactory(
      _ => delegate,
      _ => throw assemblyFailure)

    val actual = assertThrows(classOf[IllegalArgumentException], () =>
      factory.create(context(KafkaConfig.fromProps(enabledProperties(), false))))

    assertSame(assemblyFailure, actual)
    assertTrue(actual.getSuppressed.sameElements(Array(closeFailure)))
    verify(delegate).close()
  }

  @Test
  def testProductionFactoryDefersProviderIoAndBindsRecoveryToExactReplicaManager(): Unit = {
    val recoveryCreations = new AtomicInteger
    val factory = NereusBrokerStorageRuntimeFactory.production(_ => {
      recoveryCreations.incrementAndGet()
      mock(classOf[com.nereusstream.kafka.recovery.KafkaRecoveryStateFactory])
    })
    val runtime = factory.create(context(KafkaConfig.fromProps(enabledProperties(), false)))
    val replicaManager = mock(classOf[ReplicaManager])

    assertTrue(recoveryCreations.get() == 0)
    assertTrue(runtime.asyncTopicDeltaLifecycle(replicaManager).nonEmpty)
    assertTrue(runtime.asyncTopicDeltaLifecycle(replicaManager).nonEmpty)
    assertTrue(recoveryCreations.get() == 1)

    runtime.close()
  }

  private def runtimeMock(): NereusKafkaRuntime = {
    val runtime = mock(classOf[NereusKafkaRuntime])
    when(runtime.partitionStorageManager()).thenReturn(mock(classOf[KafkaPartitionStorageManager]))
    when(runtime.start()).thenReturn(CompletableFuture.completedFuture(null))
    when(runtime.beginDrain(any(classOf[DrainReason]))).thenReturn(CompletableFuture.completedFuture(null))
    when(runtime.awaitDrained(any(classOf[Duration]))).thenReturn(CompletableFuture.completedFuture(null))
    runtime
  }

  private def context(config: KafkaConfig): BrokerStorageRuntimeContext = BrokerStorageRuntimeContext(
    config,
    "cluster-id",
    () => 9L,
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
