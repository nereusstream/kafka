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
import com.nereusstream.kafka.partition.{KafkaListOffsetsResolver, KafkaPartitionIdentity, KafkaPartitionLeaderOpenRequest, KafkaPartitionState, KafkaPartitionStorage, KafkaPartitionStorageManager}
import kafka.cluster.Partition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.errors.{FencedLeaderEpochException, NotLeaderOrFollowerException}
import org.apache.kafka.storage.internals.log.LeaderEpochAwareOffsetLookup

import java.time.Duration
import java.util.Objects
import java.util.concurrent.{CompletableFuture, CompletionException}
import scala.collection.mutable

/**
 * Installs the exact Nereus storage instance returned by the product-owned lifecycle manager into the stock
 * [[Partition]] ListOffsets seam. It never constructs, recovers, resigns, or closes partition storage directly.
 *
 * The caller must invoke `openLeader` only after the stock partition has published the same leader epoch. Every
 * removal is epoch- and identity-safe, so a late completion from an older open cannot remove a newer lookup.
 */
final class NereusListOffsetsLifecycle(
  storageManager: KafkaPartitionStorageManager,
  scanConfig: NereusListOffsetsScanConfig
) extends AutoCloseable {
  Objects.requireNonNull(storageManager, "storageManager")
  Objects.requireNonNull(scanConfig, "scanConfig")

  private val guard = new Object
  private val slots = mutable.HashMap.empty[KafkaPartitionIdentity, Slot]
  private val inspector = new NereusRecordTimestampInspector
  private var closed = false
  private var managerShutdownOperation: LifecycleVoidResult = _

  def openLeader(
    partition: Partition,
    request: KafkaPartitionLeaderOpenRequest
  ): CompletableFuture[KafkaPartitionStorage] = {
    requireOpenIdentity(partition, request)

    val attempt = guard.synchronized {
      if (closed) {
        return CompletableFuture.failedFuture(storageClosed())
      }
      slots.get(request.identity()) match {
        case Some(current) =>
          request.authority().relationTo(current.request.authority()) match {
            case com.nereusstream.kafka.partition.KafkaLeaderAuthority.AuthorityRelation.EXACT =>
              if (!(current.partition eq partition) ||
                current.request.storageProfile() != request.storageProfile()) {
                return CompletableFuture.failedFuture(invariant(
                  "Exact Nereus leader authority conflicts with the current partition or storage profile"))
              }
              return current.result
            case com.nereusstream.kafka.partition.KafkaLeaderAuthority.AuthorityRelation.DOMINATES =>
              partition.beginLeaderEpochAwareOffsetLookup(request.leaderEpoch())
              removeLookup(current)
              current.result.fail(fenced("Nereus leader open was superseded by a newer authority"))
              slots.remove(request.identity())
            case _ =>
              return CompletableFuture.failedFuture(fenced(
                "Nereus leader open is stale or conflicts with the process-current authority"))
          }
        case None =>
          partition.beginLeaderEpochAwareOffsetLookup(request.leaderEpoch())
      }
      val created = new Slot(partition, request)
      slots.put(request.identity(), created)
      created
    }

    val opening = try {
      storageManager.openLeader(request)
    } catch {
      case failure: Throwable => CompletableFuture.failedFuture[KafkaPartitionStorage](failure)
    }
    if (opening == null) {
      completeOpen(attempt, null, new NullPointerException("Nereus partition manager returned a null open future"))
    } else {
      opening.whenComplete((storage, failure) => completeManagerOpen(attempt, storage, failure))
    }
    attempt.result
  }

  /** Revokes request admission before the product-owned storage begins draining. */
  def resign(
    partition: Partition,
    identity: KafkaPartitionIdentity,
    observedLeaderEpoch: Int,
    timeout: Duration
  ): CompletableFuture[Void] = {
    requireRoutingIdentity(partition, identity)
    resign(identity, observedLeaderEpoch, timeout)
  }

  /** Metadata deletion/follower paths may use the partition identity after stock ReplicaManager removed its object. */
  def resign(
    identity: KafkaPartitionIdentity,
    observedLeaderEpoch: Int,
    timeout: Duration
  ): CompletableFuture[Void] = {
    Objects.requireNonNull(identity, "identity")
    requireNonNegative(observedLeaderEpoch, "observedLeaderEpoch")
    requirePositive(timeout, "timeout")
    guard.synchronized {
      slots.get(identity).foreach { current =>
        if (current.request.leaderEpoch() <= observedLeaderEpoch) {
          removeLookup(current)
          current.result.fail(fenced("Nereus leader open was resigned before lookup installation"))
          slots.remove(identity)
        }
      }
    }
    protectedVoid(storageManager.resign(identity, observedLeaderEpoch, timeout))
  }

  /** Revokes any installed lookup before durable partition deletion starts. */
  def delete(
    partition: Partition,
    identity: KafkaPartitionIdentity,
    metadataOffset: Long,
    timeout: Duration
  ): CompletableFuture[Void] = {
    requireRoutingIdentity(partition, identity)
    delete(identity, metadataOffset, timeout)
  }

  def delete(
    identity: KafkaPartitionIdentity,
    metadataOffset: Long,
    timeout: Duration
  ): CompletableFuture[Void] = {
    Objects.requireNonNull(identity, "identity")
    requireNonNegative(metadataOffset, "metadataOffset")
    requirePositive(timeout, "timeout")
    guard.synchronized {
      slots.remove(identity).foreach { current =>
        removeLookup(current)
        current.result.fail(fenced("Nereus leader open was removed by partition deletion"))
      }
    }
    protectedVoid(storageManager.delete(identity, metadataOffset, timeout))
  }

  /** Stops new opens and synchronously revokes every request-path lookup without shutting down the manager. */
  def beginDrain(): Unit = {
    guard.synchronized {
      if (closed) {
        return
      }
      closed = true
      slots.values.foreach { current =>
        removeLookup(current)
        current.result.fail(storageClosed())
      }
      slots.clear()
    }
  }

  /** Stops lookup admission and drains the product-owned manager exactly once. */
  def shutdown(): CompletableFuture[Void] = {
    beginDrain()
    val operation = guard.synchronized {
      if (managerShutdownOperation != null) {
        return managerShutdownOperation
      }
      managerShutdownOperation = new LifecycleVoidResult
      managerShutdownOperation
    }
    val managerShutdown = try {
      storageManager.shutdown()
    } catch {
      case failure: Throwable => CompletableFuture.failedFuture[Void](failure)
    }
    if (managerShutdown == null) {
      operation.fail(new NullPointerException("Nereus partition manager returned a null shutdown future"))
    } else {
      managerShutdown.whenComplete((_, failure) => {
        if (failure == null) operation.succeed()
        else operation.fail(unwrap(failure))
      })
    }
    operation
  }

  private[nereus] def installedPartitions: Int = guard.synchronized {
    slots.values.count(_.installation.nonEmpty)
  }

  override def close(): Unit = {
    shutdown()
  }

  private def completeManagerOpen(
    attempt: Slot,
    storage: KafkaPartitionStorage,
    suppliedFailure: Throwable
  ): Unit = {
    if (suppliedFailure != null || storage == null) {
      completeOpen(attempt, storage, suppliedFailure)
      return
    }
    try {
      validateStorage(attempt.request, storage)
    } catch {
      case failure: Throwable =>
        completeOpen(attempt, storage, failure)
        return
    }
    if (!requiresCoordinatorCompactedProbe(attempt.request.identity().observedTopicName())) {
      completeOpen(attempt, storage, null)
      return
    }
    val probe = try {
      storage.probeMandatoryCompactedRead(attempt.request.timeout())
    } catch {
      case failure: Throwable =>
        completeOpen(attempt, storage, failure)
        return
    }
    if (probe == null) {
      completeOpen(
        attempt,
        storage,
        new NullPointerException("Nereus partition storage returned a null mandatory compacted-read probe"))
    } else {
      probe.whenComplete((_, failure) => completeOpen(attempt, storage, failure))
    }
  }

  private def completeOpen(
    attempt: Slot,
    storage: KafkaPartitionStorage,
    suppliedFailure: Throwable
  ): Unit = {
    val failure = Option(suppliedFailure).map(unwrap)
    var cleanup: Option[Throwable] = None
    guard.synchronized {
      val current = slots.get(attempt.request.identity())
      if (failure.nonEmpty) {
        if (current.contains(attempt)) {
          attempt.partition.cancelLeaderEpochAwareOffsetLookup(attempt.request.leaderEpoch())
          clearLogPublication(attempt, Option(storage))
          slots.remove(attempt.request.identity())
        }
        if (storage == null) {
          attempt.result.fail(failure.get)
        } else {
          cleanup = failure
        }
      } else if (storage == null) {
        if (current.contains(attempt)) {
          attempt.partition.cancelLeaderEpochAwareOffsetLookup(attempt.request.leaderEpoch())
          clearLogPublication(attempt, None)
          slots.remove(attempt.request.identity())
        }
        attempt.result.fail(invariant("Nereus partition manager completed with null storage"))
      } else if (closed || !current.contains(attempt)) {
        cleanup = Some(fenced("Nereus leader open completed after it was superseded"))
      } else {
        try {
          validateStorage(attempt.request, storage)
          val log = attempt.partition.localLogOrException match {
            case nereusLog: NereusUnifiedLog => nereusLog
            case _ => throw invariant("Nereus lifecycle target does not own a Nereus UnifiedLog")
          }
          log.installStorage(attempt.request.leaderEpoch(), storage)
          val resolver = new KafkaListOffsetsResolver(storage, inspector)
          val lookup = new NereusListOffsetsBridge(resolver, scanConfig)
          attempt.partition.installLeaderEpochAwareOffsetLookup(attempt.request.leaderEpoch(), lookup)
          attempt.installation = Some(new Installation(storage, lookup, log))
          attempt.result.succeed(storage)
        } catch {
          case installFailure: Throwable =>
            attempt.partition.cancelLeaderEpochAwareOffsetLookup(attempt.request.leaderEpoch())
            clearLogPublication(attempt, Some(storage))
            slots.remove(attempt.request.identity())
            cleanup = Some(installFailure)
        }
      }
    }
    cleanup.foreach(primaryFailure => cleanupStorage(attempt, primaryFailure))
  }

  private def cleanupStorage(
    attempt: Slot,
    primaryFailure: Throwable
  ): Unit = {
    val cleanup = try {
      storageManager.resign(
        attempt.request.identity(),
        attempt.request.leaderEpoch(),
        attempt.request.timeout())
    } catch {
      case failure: Throwable => CompletableFuture.failedFuture[Void](failure)
    }
    if (cleanup == null) {
      primaryFailure.addSuppressed(new NullPointerException("Nereus partition manager returned a null resign future"))
      attempt.result.fail(primaryFailure)
    } else {
      cleanup.whenComplete((_, cleanupFailure) => {
        if (cleanupFailure != null) {
          primaryFailure.addSuppressed(unwrap(cleanupFailure))
        }
        attempt.result.fail(primaryFailure)
      })
    }
  }

  private def removeLookup(slot: Slot): Unit = {
    slot.installation match {
      case Some(installation) =>
        installation.log.removeStorage(slot.request.leaderEpoch(), installation.storage)
        slot.partition.removeLeaderEpochAwareOffsetLookup(
          slot.request.leaderEpoch(),
          installation.lookup)
      case None =>
        clearLogPublication(slot, None)
        slot.partition.cancelLeaderEpochAwareOffsetLookup(slot.request.leaderEpoch())
    }
    slot.installation = None
  }

  private def clearLogPublication(
    slot: Slot,
    expectedStorage: Option[KafkaPartitionStorage]
  ): Unit = {
    try {
      slot.partition.localLogOrException match {
        case log: NereusUnifiedLog =>
          log.removeStorage(slot.request.leaderEpoch(), expectedStorage.orNull)
        case _ =>
      }
    } catch {
      case _: NotLeaderOrFollowerException =>
    }
  }

  private def requireOpenIdentity(
    partition: Partition,
    request: KafkaPartitionLeaderOpenRequest
  ): Unit = {
    requireRoutingIdentity(partition, request.identity())
    val topicId = partition.topicId.getOrElse {
      throw invariant("Nereus leader open requires a non-zero stock Kafka topic ID")
    }
    if (topicId.toString != request.identity().topicId()) {
      throw invariant("Nereus leader open topic ID does not match the stock Kafka partition")
    }
    if (!partition.isLeader) {
      throw new NotLeaderOrFollowerException(
        s"Cannot open Nereus storage for non-leader partition ${partition.topicPartition}")
    }
    if (partition.getLeaderEpoch != request.leaderEpoch()) {
      throw new FencedLeaderEpochException(
        s"Cannot open Nereus storage for partition ${partition.topicPartition} at stale leader epoch " +
          s"${request.leaderEpoch()}; current leader epoch is ${partition.getLeaderEpoch}")
    }
  }

  private def validateStorage(
    request: KafkaPartitionLeaderOpenRequest,
    storage: KafkaPartitionStorage
  ): Unit = {
    if (storage.identity() != request.identity() ||
      storage.leaderEpoch() != request.leaderEpoch() ||
      storage.storageProfile() != request.storageProfile() ||
      storage.state() != KafkaPartitionState.LEADER_WRITABLE) {
      throw invariant("Nereus partition manager returned storage outside the requested recovered leader authority")
    }
  }

  private def requiresCoordinatorCompactedProbe(topic: String): Boolean =
    topic == Topic.GROUP_METADATA_TOPIC_NAME ||
      topic == Topic.TRANSACTION_STATE_TOPIC_NAME ||
      topic == Topic.SHARE_GROUP_STATE_TOPIC_NAME

  private def requireRoutingIdentity(partition: Partition, identity: KafkaPartitionIdentity): Unit = {
    if (partition.topic != identity.observedTopicName() || partition.partitionId != identity.partition()) {
      throw invariant("Nereus partition identity does not match the stock Kafka partition")
    }
  }

  private def protectedVoid(operation: CompletableFuture[Void]): CompletableFuture[Void] = {
    if (operation == null) {
      return CompletableFuture.failedFuture(
        new NullPointerException("Nereus partition manager returned a null lifecycle future"))
    }
    val result = new LifecycleVoidResult
    operation.whenComplete((_, failure) => {
      if (failure == null) result.succeed()
      else result.fail(unwrap(failure))
    })
    result
  }

  private def requireNonNegative(value: Long, name: String): Unit = {
    if (value < 0) throw new IllegalArgumentException(s"$name must be non-negative")
  }

  private def requirePositive(value: Duration, name: String): Unit = {
    if (value == null || value.isZero || value.isNegative || value.toMillis <= 0) {
      throw new IllegalArgumentException(s"$name must be positive and millisecond-representable")
    }
  }

  private def unwrap(failure: Throwable): Throwable = {
    var current = failure
    while (current.isInstanceOf[CompletionException] && current.getCause != null) {
      current = current.getCause
    }
    current
  }

  private def fenced(message: String): NereusException =
    new NereusException(ErrorCode.FENCED_APPEND, false, message)

  private def invariant(message: String): NereusException =
    new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message)

  private def storageClosed(): NereusException =
    new NereusException(ErrorCode.STORAGE_CLOSED, false, "Nereus ListOffsets lifecycle is closed")

  private final class Slot(
    val partition: Partition,
    val request: KafkaPartitionLeaderOpenRequest
  ) {
    val result = new LifecycleOpenResult
    var installation: Option[Installation] = None
  }

  private final class Installation(
    val storage: KafkaPartitionStorage,
    val lookup: LeaderEpochAwareOffsetLookup,
    val log: NereusUnifiedLog
  )

  private final class LifecycleOpenResult extends CompletableFuture[KafkaPartitionStorage] {
    def succeed(storage: KafkaPartitionStorage): Boolean = super.complete(storage)
    def fail(failure: Throwable): Boolean = super.completeExceptionally(failure)
    override def cancel(mayInterruptIfRunning: Boolean): Boolean = false
    override def complete(value: KafkaPartitionStorage): Boolean = false
    override def completeExceptionally(failure: Throwable): Boolean = false
    override def obtrudeValue(value: KafkaPartitionStorage): Unit =
      throw new UnsupportedOperationException("Nereus lifecycle completion is operation-owned")
    override def obtrudeException(failure: Throwable): Unit =
      throw new UnsupportedOperationException("Nereus lifecycle completion is operation-owned")
  }

  private final class LifecycleVoidResult extends CompletableFuture[Void] {
    def succeed(): Boolean = super.complete(null)
    def fail(failure: Throwable): Boolean = super.completeExceptionally(failure)
    override def cancel(mayInterruptIfRunning: Boolean): Boolean = false
    override def complete(value: Void): Boolean = false
    override def completeExceptionally(failure: Throwable): Boolean = false
    override def obtrudeValue(value: Void): Unit =
      throw new UnsupportedOperationException("Nereus lifecycle completion is operation-owned")
    override def obtrudeException(failure: Throwable): Unit =
      throw new UnsupportedOperationException("Nereus lifecycle completion is operation-owned")
  }
}
