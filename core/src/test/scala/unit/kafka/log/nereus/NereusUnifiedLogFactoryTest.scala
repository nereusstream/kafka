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

import com.nereusstream.api.{AppendAuthority, Checksum, ChecksumType}
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState
import com.nereusstream.kafka.partition.{KafkaPartitionState, KafkaPartitionStorage, KafkaStableSnapshot}
import kafka.log.LogManager
import kafka.server.KafkaConfig
import kafka.server.storage.BrokerStorageRuntimeContext
import kafka.utils.TestUtils

import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.share.ShareCoordinatorConfig
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.metadata.{KRaftMetadataCache, MockConfigRepository}
import org.apache.kafka.server.config.{NereusKafkaConfigs, ReplicationConfigs, ServerLogConfigs}
import org.apache.kafka.server.util.{KafkaScheduler, MockTime}
import org.apache.kafka.storage.internals.log.{CleanerConfig, LogDirFailureChannel}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, when}

import java.nio.file.Files
import java.util.{Optional, Properties}

class NereusUnifiedLogFactoryTest {
  @Test
  def testSelectsEphemeralCacheAndNeverFallsBackToLocalPartitionTruth(): Unit = {
    val configuredLogDir = TestUtils.tempDir()
    val cacheDir = TestUtils.tempDir()
    val properties = enabledProperties(configuredLogDir.getAbsolutePath, cacheDir.getAbsolutePath)
    val config = KafkaConfig.fromProps(properties, false)
    val scheduler = new KafkaScheduler(1, true, "nereus-unified-log-factory-test")
    val factory = new NereusUnifiedLogFactory(context(config, scheduler))
    val expectedRoot = cacheDir.toPath.resolve("0").resolve("partition-logs").toFile.getAbsoluteFile
    val staleLocalPartition = expectedRoot.toPath.resolve("stale-local-0")
    Files.createDirectories(staleLocalPartition)
    Files.writeString(staleLocalPartition.resolve("00000000000000000000.log"), "not partition truth")

    assertEquals(Seq(expectedRoot), factory.logDirectories(Seq(configuredLogDir)))
    assertTrue(factory.initialOfflineDirectories(Seq(configuredLogDir), Seq(expectedRoot)).isEmpty)
    assertFalse(factory.loadExistingLogs)
    assertFalse(factory.scheduleLocalMaintenance)

    scheduler.startup()
    val logManager = LogManager(
      config,
      Seq(configuredLogDir.getAbsolutePath),
      new MockConfigRepository,
      scheduler,
      new MockTime,
      new BrokerTopicStats,
      new LogDirFailureChannel(1),
      factory)
    try {
      logManager.startup(Set("stale-local"))
      assertTrue(logManager.allLogs.isEmpty)
      assertEquals(Seq(expectedRoot), logManager.liveLogDirs)

      val topicId = Uuid.randomUuid()
      val log = logManager.getOrCreateLog(
        new TopicPartition("events", 0),
        topicId = Optional.of(topicId))
      assertTrue(log.isInstanceOf[NereusUnifiedLog])
      assertEquals(expectedRoot.getAbsolutePath, log.parentDir)
      assertEquals(topicId, log.topicId.orElseThrow())
      assertThrows(classOf[KafkaStorageException], () =>
        log.appendAsLeader(TestUtils.singletonRecords("must-not-hit-local-log".getBytes), 7))

      val nereusLog = log.asInstanceOf[NereusUnifiedLog]
      val source = emptySource(nereusLog, 7)
      val codec = new NereusKafkaRecoveryStateCodec(nereusLog.nereusIdentity(), 7, 0, 0)
      val recoveredState = codec.freshState()
      codec.validateRecoveredState(recoveredState, source)
      nereusLog.installRecoveredState(7, recoveredState)
      assertFalse(nereusLog.nereusWritable(7))

      val storage = mock(classOf[KafkaPartitionStorage])
      when(storage.identity()).thenReturn(nereusLog.nereusIdentity())
      when(storage.leaderEpoch()).thenReturn(7)
      when(storage.state()).thenReturn(KafkaPartitionState.LEADER_WRITABLE)
      when(storage.stableSnapshot()).thenReturn(KafkaStableSnapshot.nonTransactional(0, 0, 1))
      nereusLog.installStorage(7, storage)
      assertTrue(nereusLog.nereusWritable(7))
      assertThrows(classOf[KafkaStorageException], () =>
        nereusLog.appendAsLeader(TestUtils.singletonRecords("data-plane-pending".getBytes), 7))
      nereusLog.removeStorage(7, storage)
      assertFalse(nereusLog.nereusWritable(7))

      assertThrows(classOf[com.nereusstream.api.NereusException], () =>
        logManager.getOrCreateLog(
          new TopicPartition("missing-id", 0),
          topicId = Optional.empty()))
      assertFalse(configuredLogDir.toPath.resolve("events-0").toFile.exists())
    } finally {
      logManager.shutdown()
      scheduler.shutdown()
    }
  }

  private def context(config: KafkaConfig, scheduler: KafkaScheduler): BrokerStorageRuntimeContext =
    BrokerStorageRuntimeContext(
      config,
      "cluster-id",
      () => 9L,
      mock(classOf[KRaftMetadataCache]),
      Time.SYSTEM,
      mock(classOf[Metrics]),
      scheduler)

  private def emptySource(log: NereusUnifiedLog, leaderEpoch: Int): KafkaCheckpointSourceState =
    new KafkaCheckpointSourceState(
      new AppendAuthority(
        "kafka-partition-leader-v1",
        log.nereusIdentity().durableId().canonicalIdentity(),
        leaderEpoch,
        "broker-0",
        9),
      "writer-0",
      1,
      "fencing-token",
      1,
      0,
      0,
      1,
      "commit-1",
      new Checksum(ChecksumType.SHA256, "a" * 64),
      false,
      0)

  private def enabledProperties(logDir: String, cacheDir: String): Properties = {
    val properties = TestUtils.createBrokerConfig(0)
    properties.put(ServerLogConfigs.LOG_DIRS_CONFIG, logDir)
    properties.put(NereusKafkaConfigs.ENABLED_CONFIG, "true")
    properties.put(NereusKafkaConfigs.CLUSTER_CONFIG, "nereus-cluster")
    properties.put(NereusKafkaConfigs.OXIA_SERVICE_ADDRESS_CONFIG, "oxia://127.0.0.1:6648")
    properties.put(NereusKafkaConfigs.CACHE_DIR_CONFIG, cacheDir)
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
