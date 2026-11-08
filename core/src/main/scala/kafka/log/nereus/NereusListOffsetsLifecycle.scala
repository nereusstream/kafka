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

import com.nereusstream.kafka.bookkeeper.protocol.{KafkaPartitionFrontiersV1, KafkaPartitionPublicationEventV1, KafkaPartitionPublicationKindV1}
import kafka.cluster.Partition
import kafka.server.nereus.{NereusKafkaObjectWalBroker, NereusKafkaObjectWalPartition, NereusKafkaOwnedProviderRuntime}
import kafka.server.storage.BrokerStorageRuntimeContext
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.errors.{FencedLeaderEpochException, KafkaStorageException}
import org.apache.kafka.common.metadata.TopicBindingAggregateRecord
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.server.storage.log.FetchIsolation

import java.time.Duration
import java.util.concurrent.{Callable, CompletableFuture, ConcurrentHashMap, CopyOnWriteArrayList}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** One exact native recovery slot. Stale completions close only their own connections and never install readiness. */
final class NereusListOffsetsLifecycle(context: BrokerStorageRuntimeContext, provider: NereusKafkaOwnedProviderRuntime,
                                     objectWal: NereusKafkaObjectWalBroker)
  extends AutoCloseable {
  private val guard = new Object
  private val slots = mutable.HashMap.empty[TopicIdPartition, Slot]
  private val subscriptions = new ConcurrentHashMap[TopicIdPartition, CopyOnWriteArrayList[FetchSubscription]]
  @volatile private var closed = false
  private var draining = false

  def openLeader(partition: Partition, identity: TopicIdPartition, aggregate: TopicBindingAggregateRecord,
                 leaderEpoch: Int, brokerEpoch: Long, metadataOffset: Long): CompletableFuture[Void] = {
    val attempt = guard.synchronized {
      if (draining || closed) return CompletableFuture.failedFuture(new FencedLeaderEpochException("Nereus lifecycle is draining"))
      slots.get(identity).foreach { previous =>
        if (previous.live && previous.epoch == leaderEpoch && previous.brokerEpoch == brokerEpoch && (previous.partition eq partition)) {
          return previous.result.copy()
        }
        if (leaderEpoch <= previous.epoch) return CompletableFuture.failedFuture(new FencedLeaderEpochException("stale Nereus leader recovery"))
        revoke(previous)
      }
      partition.beginLeaderEpochAwareOffsetLookup(leaderEpoch)
      val created = new Slot(partition, identity, leaderEpoch, brokerEpoch, metadataOffset,
        NereusKafkaObjectWalBroker.selectsObjectWal(aggregate))
      slots.put(identity, created)
      created
    }
    if (attempt.objectWal) {
      openObjectLeader(attempt, aggregate)
      return attempt.result.copy()
    }
    provider.open(identity, aggregate, leaderEpoch, brokerEpoch, metadataOffset,
      () => current(attempt), event => signalPublication(identity, event)).whenComplete { (opened, failure) =>
      var cleanup = false
      guard.synchronized {
        if (failure != null || !current(attempt)) {
          cleanup = opened != null
          attempt.result.completeExceptionally(if (failure != null) failure else new FencedLeaderEpochException("Nereus recovery was superseded"))
          if (slots.get(identity).contains(attempt)) { slots.remove(identity); revoke(attempt) }
        } else {
          try {
            val log = partition.localLogOrException match {
              case exact: NereusUnifiedLog => exact
              case _ => throw new KafkaStorageException("native partition has no Nereus log")
            }
            val state = log.installRecoveredState(leaderEpoch, opened.storage(), () => current(attempt))
            attempt.opened = Some(opened)
            attempt.log = Some(log)
            partition.installNereusRecoveredState(leaderEpoch, state)
            val lookup = new NereusListOffsetsBridge(log)
            attempt.lookup = Some(lookup)
            // Keep request admission gated until the native Controller acknowledges RECOVERED.
            partition.completeNereusRecovery(leaderEpoch).whenComplete { (_, recoveryFailure) =>
              guard.synchronized {
                if (recoveryFailure == null && current(attempt)) {
                  try {
                    opened.attachM5Log(log)
                    partition.installLeaderEpochAwareOffsetLookup(leaderEpoch, lookup)
                    attempt.result.complete(null)
                    signal(identity)
                  } catch { case installFailure: Throwable => attempt.result.completeExceptionally(installFailure); revoke(attempt) }
                } else {
                  attempt.result.completeExceptionally(if (recoveryFailure != null) recoveryFailure else new FencedLeaderEpochException("Nereus ready belongs to an older owner"))
                  revoke(attempt)
                }
              }
            }
          } catch {
            case installFailure: Throwable =>
              attempt.result.completeExceptionally(installFailure)
              slots.remove(identity); revoke(attempt); cleanup = true
          }
        }
      }
      if (cleanup) try opened.close() catch { case closeFailure: Throwable => if (failure != null) failure.addSuppressed(closeFailure) }
    }
    attempt.result.copy()
  }

  private def openObjectLeader(attempt: Slot, aggregate: TopicBindingAggregateRecord): Unit = {
    val assignment = new NereusKafkaObjectWalPartition.NativeAssignment {
      override def current(): Boolean = NereusListOffsetsLifecycle.this.current(attempt)
      override def whileCurrent[T](action: Callable[T]): T = attempt.ownerGate.synchronized {
        if (!current()) throw new FencedLeaderEpochException("Object WAL native assignment was superseded")
        action.call()
      }
    }
    objectWal.open(attempt.identity, aggregate, attempt.epoch, attempt.brokerEpoch, attempt.metadataOffset,
      assignment, event => signalPublication(attempt.identity, event)).whenComplete { (opened, failure) =>
      var cleanup = false
      guard.synchronized {
        if (failure != null || !current(attempt)) {
          cleanup = opened != null
          attempt.result.completeExceptionally(if (failure != null) failure else new FencedLeaderEpochException("Object WAL recovery was superseded"))
          if (slots.get(attempt.identity).contains(attempt)) { slots.remove(attempt.identity); revoke(attempt) }
        } else {
          try {
            val log = attempt.partition.localLogOrException match {
              case exact: NereusUnifiedLog => exact
              case _ => throw new KafkaStorageException("native Object WAL partition has no Nereus log")
            }
            val state = log.installRecoveredObjectState(attempt.epoch, opened, () => assignment.current())
            attempt.objectOpened = Some(opened)
            attempt.log = Some(log)
            attempt.partition.installNereusRecoveredState(attempt.epoch, state)
            val lookup = new NereusListOffsetsBridge(log)
            attempt.lookup = Some(lookup)
            attempt.partition.completeNereusRecovery(attempt.epoch).whenComplete { (_, recoveryFailure) =>
              guard.synchronized {
                if (recoveryFailure == null && current(attempt)) {
                  try {
                    attempt.partition.installLeaderEpochAwareOffsetLookup(attempt.epoch, lookup)
                    attempt.result.complete(null)
                    signal(attempt.identity)
                  } catch {
                    case installFailure: Throwable => attempt.result.completeExceptionally(installFailure); revoke(attempt)
                  }
                } else {
                  attempt.result.completeExceptionally(if (recoveryFailure != null) recoveryFailure else
                    new FencedLeaderEpochException("Object WAL ready belongs to an older native owner"))
                  revoke(attempt)
                }
              }
            }
          } catch {
            case installFailure: Throwable =>
              attempt.result.completeExceptionally(installFailure)
              slots.remove(attempt.identity); revoke(attempt); cleanup = true
          }
        }
      }
      if (cleanup) objectWal.closePartition(opened)
    }
  }

  def publishMetadataImage(delta: MetadataDelta, newImage: MetadataImage, publish: () => Unit): Unit = {
    val brokerId = context.config.brokerId
    val brokerChanged = Option(delta.clusterDelta()).exists(_.changedBrokers().containsKey(brokerId))
    val topics = Option(delta.topicsDelta())
    if (!brokerChanged && topics.isEmpty) {
      publish()
      return
    }

    guard.synchronized {
      def fenceIfReplaced(slot: Slot): Unit = {
        if (slot.live && !matchesAssignment(slot, newImage)) {
          // This fences the same ordered pipeline that publishes a durable callback,
          // without waiting for ReplicaManager's partition lock or closing any resources.
          fenceSlot(slot)
        }
      }

      if (brokerChanged) {
        slots.valuesIterator.foreach(fenceIfReplaced)
      } else {
        topics.foreach { changes =>
          changes.changedTopics().asScala.valuesIterator.foreach { topic =>
            topic.partitionChanges().asScala.keysIterator.foreach { partition =>
              slots.get(new TopicIdPartition(topic.id(), partition, topic.name())).foreach(fenceIfReplaced)
            }
          }
          changes.deletedTopicIds().asScala.foreach { id =>
            val previous = changes.image().getTopic(id)
            previous.partitions().asScala.keysIterator.foreach { partition =>
              slots.get(new TopicIdPartition(id, partition, previous.name())).foreach(fenceIfReplaced)
            }
          }
        }
      }
      publish()
    }
  }

  private def current(slot: Slot): Boolean =
    !closed && slot.live && matchesAssignment(slot, context.metadataCache.getImage)

  private def matchesAssignment(slot: Slot, image: MetadataImage): Boolean = {
    val topic = image.topics().getTopic(slot.identity.topicId())
    val registration = image.cluster().broker(context.config.brokerId)
    val assignment = if (topic == null) null else topic.partitions().get(slot.identity.partition())
    assignment != null && assignment.leader == context.config.brokerId && assignment.leaderEpoch == slot.epoch &&
      assignment.replicas.length == 1 && assignment.replicas(0) == context.config.brokerId &&
      assignment.isr.length == 1 && assignment.isr(0) == context.config.brokerId &&
      registration != null && registration.epoch() == slot.brokerEpoch && !registration.fenced() &&
      !registration.inControlledShutdown() && context.brokerEpochSupplier() == slot.brokerEpoch &&
      image.provenance().lastContainedOffset() >= slot.metadataOffset
  }

  def resign(identity: TopicIdPartition, epoch: Int, timeout: Duration,
             onResigned: () => CompletableFuture[Void]): CompletableFuture[Void] = {
    val closing = guard.synchronized {
      slots.get(identity).filter(_.epoch <= epoch).map { slot => slots.remove(identity); revoke(slot); slot }
    }
    closeSlot(closing, onResigned)
  }
  def delete(identity: TopicIdPartition, metadataOffset: Long, timeout: Duration,
             onResigned: () => CompletableFuture[Void]): CompletableFuture[Void] = {
    val closing = guard.synchronized { slots.remove(identity).map { slot => revoke(slot); slot } }
    closeSlot(closing, onResigned)
  }
  private def closeSlot(slot: Option[Slot], onResigned: () => CompletableFuture[Void]): CompletableFuture[Void] = {
    val coordinator = try {
      java.util.Objects.requireNonNull(onResigned(), "coordinator resignation returned a null future")
    } catch {
      case failure: Throwable => return CompletableFuture.failedFuture(failure)
    }
    coordinator.thenCompose(_ => slot.flatMap(_.objectOpened) match {
      case Some(opened) => objectWal.closePartition(opened)
      case None => slot.flatMap(_.opened).map(_.closeAsync()).getOrElse(CompletableFuture.completedFuture(null))
    })
  }
  def sealObjectWalBeforeHandoff(): CompletableFuture[Void] = guard.synchronized {
    draining = true
    val seals = slots.valuesIterator.filter(slot => current(slot) && slot.result.isDone && !slot.result.isCompletedExceptionally)
      .flatMap(_.objectOpened).map(objectWal.sealBeforeHandoff).toSeq
    CompletableFuture.allOf(seals: _*)
  }

  def beginDrain(): Unit = guard.synchronized {
    closed = true
    slots.values.foreach(revoke)
    slots.clear()
  }
  def shutdown(): CompletableFuture[Void] = { beginDrain(); CompletableFuture.completedFuture(null) }
  override def close(): Unit = beginDrain()
  private def fenceSlot(slot: Slot): Unit = {
    if (slot.objectWal) {
      slot.ownerGate.synchronized {
        slot.live = false
        slot.objectOpened.foreach(_.fence())
      }
    } else {
      slot.opened.foreach(_.fence())
      slot.live = false
    }
  }

  private def revoke(slot: Slot): Unit = {
    fenceSlot(slot)
    for (log <- slot.log; opened <- slot.opened) log.removeStorage(slot.epoch, opened.storage())
    for (log <- slot.log; opened <- slot.objectOpened) log.removeObjectStorage(slot.epoch, opened)
    // A failed ready response or a new local epoch may revoke the slot without a later resignation callback.
    // The Object route releases its source set only after the actual accepted operations have stopped.
    slot.objectOpened.foreach(objectWal.closePartition)
    slot.lookup.foreach(slot.partition.removeLeaderEpochAwareOffsetLookup(slot.epoch, _))
    slot.partition.cancelLeaderEpochAwareOffsetLookup(slot.epoch)
    slot.result.completeExceptionally(new FencedLeaderEpochException("Nereus native owner was revoked"))
    signal(slot.identity)
  }
  def subscribe(identity: TopicIdPartition, isolation: FetchIsolation, wakeup: Runnable): AutoCloseable = guard.synchronized {
    val listener = new FetchSubscription(isolation, wakeup)
    subscriptions.compute(identity, (_, existing) => {
      val listeners = if (existing == null) new CopyOnWriteArrayList[FetchSubscription] else existing
      listeners.add(listener)
      listeners
    })
    () => {
      subscriptions.computeIfPresent(identity, (_, listeners) => {
        listeners.remove(listener)
        if (listeners.isEmpty) null else listeners
      })
      ()
    }
  }
  private def signal(identity: TopicIdPartition): Unit = {
    val listeners = subscriptions.get(identity)
    if (listeners != null) listeners.forEach(_.wakeup.run())
  }
  private def signalPublication(identity: TopicIdPartition, event: KafkaPartitionPublicationEventV1): Unit = {
    val listeners = subscriptions.get(identity)
    if (listeners != null) {
      val before = event.previous().frontiers()
      val after = event.published().frontiers()
      listeners.forEach(listener => {
        if (NereusListOffsetsLifecycle.wakesFor(listener.isolation, event.kind(), before, after)) {
          listener.wakeup.run()
        }
      })
    }
  }
  private final class FetchSubscription(val isolation: FetchIsolation, val wakeup: Runnable)
  private final class Slot(val partition: Partition, val identity: TopicIdPartition, val epoch: Int,
                           val brokerEpoch: Long, val metadataOffset: Long, val objectWal: Boolean) {
    @volatile var live = true
    val result = new CompletableFuture[Void]
    val ownerGate = new Object
    var opened: Option[NereusKafkaOwnedProviderRuntime#Opened] = None
    var objectOpened: Option[NereusKafkaObjectWalPartition] = None
    var log: Option[NereusUnifiedLog] = None
    var lookup: Option[NereusListOffsetsBridge] = None
  }
}

object NereusListOffsetsLifecycle {
  private[kafka] def wakesFor(isolation: FetchIsolation, kind: KafkaPartitionPublicationKindV1,
                             before: KafkaPartitionFrontiersV1, after: KafkaPartitionFrontiersV1): Boolean = {
    if (before.trimStartOffset() != after.trimStartOffset()) return true
    kind match {
      case KafkaPartitionPublicationKindV1.COMMIT | KafkaPartitionPublicationKindV1.SPECULATIVE_ROLLBACK =>
        isolation match {
          case FetchIsolation.LOG_END => before.readableEndOffset() != after.readableEndOffset()
          case FetchIsolation.HIGH_WATERMARK => before.highWatermark() != after.highWatermark()
          case FetchIsolation.TXN_COMMITTED => before.lastStableOffset() != after.lastStableOffset()
        }
      case _ => true // A source or Owner transition invalidates a captured read plan at the same offsets.
    }
  }
}
