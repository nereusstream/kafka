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

import com.nereusstream.api.{AppendAuthority, AppendResult, Checksum, ChecksumType}
import com.nereusstream.kafka.checkpoint.{KafkaCanonicalCheckpointState, KafkaCheckpointSourceState}
import com.nereusstream.kafka.codec.{KafkaAppendBatchEncoder, KafkaFetchAssembly, KafkaRecordBatchCodec}
import com.nereusstream.kafka.partition.{KafkaAppendContext, KafkaPartitionState, KafkaPartitionStorage, KafkaStableAppendResult, KafkaStableSnapshot, KafkaStorageReadRequest, KafkaStorageReadResult}
import com.nereusstream.kafka.retention.{KafkaDeleteRecordsCoordinator, KafkaPartitionMaintenance, KafkaTrimBarrier}
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding
import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord
import kafka.log.LogManager
import kafka.server.KafkaConfig
import kafka.server.storage.BrokerStorageRuntimeContext
import kafka.utils.TestUtils

import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.record.{
  CompressionType,
  ControlRecordType,
  EndTransactionMarker,
  MemoryRecords,
  RecordBatch,
  SimpleRecord
}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.share.ShareCoordinatorConfig
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.metadata.{KRaftMetadataCache, MockConfigRepository}
import org.apache.kafka.server.config.{NereusKafkaConfigs, ReplicationConfigs, ServerLogConfigs}
import org.apache.kafka.server.common.{RequestLocal, TransactionVersion}
import org.apache.kafka.server.util.{KafkaScheduler, MockTime}
import org.apache.kafka.server.storage.log.FetchIsolation
import org.apache.kafka.storage.internals.log.{AppendOrigin, CleanerConfig, LogDirFailureChannel, VerificationGuard}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{mock, verify, when}

import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.concurrent.{CompletableFuture, atomic}
import java.util.{Optional, OptionalLong, Properties}

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
      val codec = new NereusKafkaRecoveryStateCodec(
        nereusLog.nereusIdentity(),
        7,
        0,
        0,
        nereusLog.prepareProducerRecovery(7, 0))
      val recoveredState = codec.freshState()
      codec.validateRecoveredState(recoveredState, source)
      nereusLog.installRecoveredState(7, recoveredState)
      assertEquals(7, nereusLog.latestEpoch.orElseThrow())
      assertEquals(0L, nereusLog.endOffsetForEpoch(7).orElseThrow().offset())
      assertFalse(nereusLog.nereusWritable(7))

      val storage = mock(classOf[KafkaPartitionStorage])
      val snapshot = new atomic.AtomicReference(KafkaStableSnapshot.nonTransactional(0, 0, 1))
      val stableBytes = new atomic.AtomicReference(Array.emptyByteArray)
      val appendAcks = new atomic.AtomicReference[Short]()
      val corruptNextStableResult = new atomic.AtomicBoolean()
      when(storage.identity()).thenReturn(nereusLog.nereusIdentity())
      when(storage.leaderEpoch()).thenReturn(7)
      when(storage.state()).thenReturn(KafkaPartitionState.LEADER_WRITABLE)
      when(storage.stableSnapshot()).thenAnswer(_ => snapshot.get())
      when(storage.publishDerivedOffsets(anyLong(), anyLong(), anyLong())).thenAnswer(invocation => {
        val current = snapshot.get()
        val published = new KafkaStableSnapshot(
          current.logStartOffset(),
          invocation.getArgument[java.lang.Long](0),
          invocation.getArgument[java.lang.Long](1),
          invocation.getArgument[java.lang.Long](2),
          current.commitVersion())
        snapshot.set(published)
        published
      })
      when(storage.resign()).thenReturn(CompletableFuture.completedFuture(null))
      val maintenance = mock(classOf[KafkaPartitionMaintenance])
      when(storage.maintenance()).thenReturn(Optional.of(maintenance))
      when(storage.append(any(classOf[ByteBuffer]), any(classOf[KafkaAppendContext]))).thenAnswer(invocation => {
        val records = invocation.getArgument[ByteBuffer](0)
        val context = invocation.getArgument[KafkaAppendContext](1)
        val encoded = new KafkaAppendBatchEncoder(new KafkaRecordBatchCodec)
          .encode(records, context.expectedStartOffset())
        val owned = new Array[Byte](records.remaining())
        records.duplicate().get(owned)
        stableBytes.set(stableBytes.get() ++ owned)
        appendAcks.set(context.requiredAcks())
        val current = snapshot.get()
        val next = new KafkaStableSnapshot(
          current.logStartOffset(),
          encoded.range().endOffset(),
          current.highWatermark(),
          current.lastStableOffset(),
          current.commitVersion() + 1)
        snapshot.set(next)
        val appendResult = mock(classOf[AppendResult])
        when(appendResult.committedEndOffset()).thenReturn(encoded.range().endOffset())
        val returnedAcks =
          if (corruptNextStableResult.compareAndSet(true, false)) (context.requiredAcks() + 1).toShort
          else context.requiredAcks()
        CompletableFuture.completedFuture(
          new KafkaStableAppendResult(appendResult, encoded, next, returnedAcks))
      })
      when(storage.read(any(classOf[KafkaStorageReadRequest]))).thenAnswer(invocation => {
        val request = invocation.getArgument[KafkaStorageReadRequest](0)
        val allBatches = MemoryRecords
          .readableRecords(ByteBuffer.wrap(stableBytes.get()))
          .batches()
          .iterator()
        val selected = new java.util.ArrayList[RecordBatch]()
        var selectedBytes = 0
        var reachedBudget = false
        while (allBatches.hasNext && !reachedBudget) {
          val batch = allBatches.next()
          if (batch.lastOffset() >= request.startOffset()
            && batch.baseOffset() < request.maxOffsetExclusive()) {
            val candidateBytes = Math.addExact(selectedBytes, batch.sizeInBytes())
            if (candidateBytes <= request.maxPartitionBytes()
              || (selected.isEmpty && request.minOneMessage())) {
              selected.add(batch)
              selectedBytes = candidateBytes
            } else {
              reachedBudget = true
            }
          }
        }
        val selectedBuffer = ByteBuffer.allocate(selectedBytes)
        val selectedIterator = selected.iterator()
        while (selectedIterator.hasNext) {
          selectedIterator.next().writeTo(selectedBuffer)
        }
        selectedBuffer.flip()
        val bytes = new Array[Byte](selectedBuffer.remaining())
        selectedBuffer.get(bytes)
        val actualFirstOffset =
          if (selected.isEmpty) OptionalLong.empty()
          else OptionalLong.of(selected.get(0).baseOffset())
        val nextLogicalOffset =
          if (selected.isEmpty) request.startOffset()
          else selected.get(selected.size() - 1).nextOffset()
        val assembly = mock(classOf[KafkaFetchAssembly])
        when(assembly.recordsBuffer()).thenReturn(ByteBuffer.wrap(bytes).asReadOnlyBuffer())
        when(assembly.sizeInBytes()).thenReturn(bytes.length)
        when(assembly.actualFirstBatchBaseOffset()).thenReturn(actualFirstOffset)
        when(assembly.nextLogicalOffset()).thenReturn(nextLogicalOffset)
        when(assembly.sourceCoverageEndOffset()).thenReturn(nextLogicalOffset)
        when(assembly.firstEntryOverflow()).thenReturn(false)
        when(assembly.virtualSegmentBaseOffset()).thenReturn(0L)
        when(assembly.relativeLogicalBytePosition()).thenReturn(0L)
        when(assembly.abortedTransactions()).thenReturn(java.util.List.of())
        CompletableFuture.completedFuture(new KafkaStorageReadResult(assembly, snapshot.get()))
      })
      nereusLog.installStorage(7, storage)
      assertTrue(nereusLog.nereusWritable(7))
      val appendInfo = nereusLog.appendAsLeader(
        TestUtils.singletonRecords("stable-data".getBytes),
        7,
        AppendOrigin.CLIENT,
        RequestLocal.noCaching,
        VerificationGuard.SENTINEL,
        TransactionVersion.TV_UNKNOWN,
        -1)
      assertEquals(0L, appendInfo.firstOffset())
      assertEquals(0L, appendInfo.lastOffset())
      assertEquals((-1).toShort, appendAcks.get())
      assertEquals(1L, nereusLog.logEndOffset)
      assertEquals(0L, nereusLog.size)

      val fetched = nereusLog.read(0, 1024, FetchIsolation.LOG_END, false)
      assertEquals(stableBytes.get().length, fetched.records.sizeInBytes)
      val fetchedBatches = fetched.records.asInstanceOf[MemoryRecords].batches().iterator()
      assertTrue(fetchedBatches.hasNext)
      val fetchedBatch = fetchedBatches.next()
      assertEquals(0L, fetchedBatch.baseOffset())
      assertEquals(7, fetchedBatch.partitionLeaderEpoch())
      assertFalse(fetchedBatches.hasNext)

      val idempotent = MemoryRecords.withIdempotentRecords(
        0,
        Compression.of(CompressionType.NONE).build(),
        7,
        1.toShort,
        0,
        7,
        new SimpleRecord(1000, "idempotent".getBytes))
      val idempotentInfo = nereusLog.appendAsLeader(idempotent, 7)
      assertEquals(1L, idempotentInfo.firstOffset())
      assertEquals(1L, idempotentInfo.lastOffset())
      assertEquals(2L, nereusLog.logEndOffset)

      val producerId = 17L
      val producerEpoch = 1.toShort
      val transactionVersion = TransactionVersion.TV_2.featureLevel()
      val verificationGuard =
        nereusLog.maybeStartTransactionVerification(producerId, 0, producerEpoch, true)
      assertFalse(verificationGuard == VerificationGuard.SENTINEL)
      val transactionalRecords = MemoryRecords.withTransactionalRecords(
        Compression.NONE,
        producerId,
        producerEpoch,
        0,
        new SimpleRecord(2000, "transactional".getBytes))
      val transactionalInfo = nereusLog.appendAsLeader(
        transactionalRecords,
        7,
        AppendOrigin.CLIENT,
        RequestLocal.noCaching,
        verificationGuard,
        transactionVersion,
        -1)
      assertEquals(2L, transactionalInfo.firstOffset())
      assertEquals(2L, transactionalInfo.lastOffset())
      assertTrue(nereusLog.hasOngoingTransaction(producerId, producerEpoch))
      assertEquals(3L, snapshot.get().stableEndOffset())
      assertEquals(3L, snapshot.get().highWatermark())
      assertEquals(2L, snapshot.get().lastStableOffset())
      val blockedTransactionalFetch =
        nereusLog.read(2, 1024, FetchIsolation.TXN_COMMITTED, false)
      assertEquals(0, blockedTransactionalFetch.records.sizeInBytes)
      assertTrue(blockedTransactionalFetch.abortedTransactions.isPresent)
      assertTrue(blockedTransactionalFetch.abortedTransactions.orElseThrow().isEmpty)

      val markerEpoch = (producerEpoch + 1).toShort
      val abortMarker = MemoryRecords.withEndTransactionMarker(
        producerId,
        markerEpoch,
        new EndTransactionMarker(ControlRecordType.ABORT, 9))
      val markerInfo = nereusLog.appendAsLeader(
        abortMarker,
        7,
        AppendOrigin.COORDINATOR,
        RequestLocal.noCaching,
        VerificationGuard.SENTINEL,
        transactionVersion,
        -1)
      assertEquals(3L, markerInfo.firstOffset())
      assertEquals(3L, markerInfo.lastOffset())
      assertFalse(nereusLog.hasOngoingTransaction(producerId, markerEpoch))
      assertEquals(4L, snapshot.get().stableEndOffset())
      assertEquals(4L, snapshot.get().highWatermark())
      assertEquals(4L, snapshot.get().lastStableOffset())

      val committedFetch = nereusLog.read(2, 1024, FetchIsolation.TXN_COMMITTED, false)
      val committedBatches =
        committedFetch.records.asInstanceOf[MemoryRecords].batches().iterator()
      assertTrue(committedBatches.hasNext)
      val committedData = committedBatches.next()
      assertEquals(2L, committedData.baseOffset())
      assertTrue(committedData.isTransactional)
      assertFalse(committedData.isControlBatch)
      assertTrue(committedBatches.hasNext)
      val committedMarker = committedBatches.next()
      assertEquals(3L, committedMarker.baseOffset())
      assertTrue(committedMarker.isTransactional)
      assertTrue(committedMarker.isControlBatch)
      assertFalse(committedBatches.hasNext)
      val abortedTransactions = committedFetch.abortedTransactions.orElseThrow()
      assertEquals(1, abortedTransactions.size())
      assertEquals(producerId, abortedTransactions.get(0).producerId())
      assertEquals(2L, abortedTransactions.get(0).firstOffset())

      val secondProducerId = 18L
      val secondProducerEpoch = 1.toShort
      val secondGuard =
        nereusLog.maybeStartTransactionVerification(
          secondProducerId,
          0,
          secondProducerEpoch,
          true)
      val secondTransactionalInfo = nereusLog.appendAsLeader(
        MemoryRecords.withTransactionalRecords(
          Compression.NONE,
          secondProducerId,
          secondProducerEpoch,
          0,
          new SimpleRecord(3000, "second-transaction".getBytes)),
        7,
        AppendOrigin.CLIENT,
        RequestLocal.noCaching,
        secondGuard,
        transactionVersion,
        -1)
      assertEquals(4L, secondTransactionalInfo.firstOffset())
      val secondMarkerInfo = nereusLog.appendAsLeader(
        MemoryRecords.withEndTransactionMarker(
          secondProducerId,
          (secondProducerEpoch + 1).toShort,
          new EndTransactionMarker(ControlRecordType.ABORT, 10)),
        7,
        AppendOrigin.COORDINATOR,
        RequestLocal.noCaching,
        VerificationGuard.SENTINEL,
        transactionVersion,
        -1)
      assertEquals(5L, secondMarkerInfo.firstOffset())
      assertEquals(6L, snapshot.get().lastStableOffset())

      val boundedCommittedFetch =
        nereusLog.read(2, committedData.sizeInBytes(), FetchIsolation.TXN_COMMITTED, false)
      val boundedBatches =
        boundedCommittedFetch.records.asInstanceOf[MemoryRecords].batches().iterator()
      assertTrue(boundedBatches.hasNext)
      assertEquals(2L, boundedBatches.next().baseOffset())
      assertFalse(boundedBatches.hasNext)
      val boundedAbortedTransactions =
        boundedCommittedFetch.abortedTransactions.orElseThrow()
      assertEquals(1, boundedAbortedTransactions.size())
      assertEquals(producerId, boundedAbortedTransactions.get(0).producerId())

      val durableLogStartPublications = new atomic.AtomicInteger()
      when(maintenance.deleteRecords(
        any(classOf[KafkaPartitionMaintenance.Hooks]),
        anyLong())).thenAnswer(invocation => {
        val hooks = invocation.getArgument[KafkaPartitionMaintenance.Hooks](0)
        val requestedOffset = invocation.getArgument[java.lang.Long](1)
        val currentSource = checkpointSource(nereusLog, 7, 0, 6)
        val captured = hooks.capture(currentSource).join()
        val canonical: KafkaCanonicalCheckpointState = captured.canonicalState()
        assertEquals(6L, canonical.checkpointOffset())
        assertEquals(0L, canonical.logStartOffset())
        assertEquals(6L, canonical.producerTransactionState().mapEndOffset())
        assertEquals(6L, captured.highWatermark())
        assertEquals(6L, captured.lastStableOffset())

        val current = snapshot.get()
        snapshot.set(new KafkaStableSnapshot(
          requestedOffset,
          current.stableEndOffset(),
          current.highWatermark(),
          current.lastStableOffset(),
          current.commitVersion() + 1))
        val revalidated = mock(classOf[KafkaTrimBarrier.Snapshot])
        when(revalidated.identity()).thenReturn(nereusLog.nereusIdentity())
        val publishedBinding = mock(classOf[VersionedKafkaPartitionBinding])
        val binding = mock(classOf[KafkaPartitionBindingRecord])
        when(binding.observedLeaderEpoch()).thenReturn(7)
        when(publishedBinding.value()).thenReturn(binding)
        hooks.advanceLogStart(revalidated, requestedOffset, publishedBinding).join()
        CompletableFuture.completedFuture(
          new KafkaDeleteRecordsCoordinator.Result(
            requestedOffset,
            requestedOffset,
            Optional.empty()))
      })
      val durableLowWatermark = nereusLog.deleteRecords(
        7,
        1,
        new NereusUnifiedLog.MaintenanceAuthority {
          override def capture(
            expectedStorage: KafkaPartitionStorage,
            expectedLeaderEpoch: Int,
            capture: NereusUnifiedLog.MaintenanceCapture
          ): KafkaPartitionMaintenance.Capture = {
            assertEquals(storage, expectedStorage)
            assertEquals(7, expectedLeaderEpoch)
            capture.capture()
          }

          override def publish(
            expectedStorage: KafkaPartitionStorage,
            expectedLeaderEpoch: Int,
            durableOffset: Long
          ): Unit = {
            assertEquals(storage, expectedStorage)
            assertEquals(7, expectedLeaderEpoch)
            nereusLog.publishDurableLogStart(
              expectedStorage,
              expectedLeaderEpoch,
              durableOffset)
            durableLogStartPublications.incrementAndGet()
          }
        })
      assertEquals(1L, durableLowWatermark)
      assertEquals(1L, nereusLog.logStartOffset)
      assertEquals(1, durableLogStartPublications.get())

      corruptNextStableResult.set(true)
      assertThrows(classOf[KafkaStorageException], () =>
        nereusLog.appendAsLeader(TestUtils.singletonRecords("invalid-stable-result".getBytes), 7))
      assertEquals(6L, nereusLog.logEndOffset)
      verify(storage).resign()

      nereusLog.removeStorage(7, storage)
      assertFalse(nereusLog.nereusWritable(7))
      assertThrows(classOf[KafkaStorageException], () =>
        nereusLog.read(0, 1024, FetchIsolation.LOG_END, false))

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
    checkpointSource(log, leaderEpoch, 0, 0)

  private def checkpointSource(
    log: NereusUnifiedLog,
    leaderEpoch: Int,
    trimOffset: Long,
    endOffset: Long
  ): KafkaCheckpointSourceState =
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
      trimOffset,
      endOffset,
      1,
      "commit-1",
      new Checksum(ChecksumType.SHA256, "a" * 64),
      false,
      endOffset)

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
