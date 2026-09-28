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

import kafka.cluster.Partition
import kafka.server.nereus.NereusKafkaOwnedProviderRuntime
import kafka.server.storage.BrokerStorageRuntimeContext
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.errors.{FencedLeaderEpochException, KafkaStorageException}
import org.apache.kafka.common.metadata.TopicBindingAggregateRecord

import java.time.Duration
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, CopyOnWriteArrayList}
import scala.collection.mutable

/** One exact native recovery slot. Stale completions close only their own connections and never install readiness. */
final class NereusListOffsetsLifecycle(context: BrokerStorageRuntimeContext, provider: NereusKafkaOwnedProviderRuntime)
  extends AutoCloseable {
  private val guard = new Object
  private val slots = mutable.HashMap.empty[TopicIdPartition, Slot]
  private val subscriptions = new ConcurrentHashMap[TopicIdPartition, CopyOnWriteArrayList[Runnable]]
  @volatile private var closed = false

  def openLeader(partition: Partition, identity: TopicIdPartition, aggregate: TopicBindingAggregateRecord,
                 leaderEpoch: Int, brokerEpoch: Long, metadataOffset: Long): CompletableFuture[Void] = {
    val attempt = guard.synchronized {
      if (closed) return CompletableFuture.failedFuture(new KafkaStorageException("Nereus lifecycle is draining"))
      slots.get(identity).foreach { previous =>
        if (previous.epoch == leaderEpoch && previous.brokerEpoch == brokerEpoch && (previous.partition eq partition)) {
          return previous.result.copy()
        }
        if (leaderEpoch <= previous.epoch) return CompletableFuture.failedFuture(new FencedLeaderEpochException("stale Nereus leader recovery"))
        revoke(previous)
      }
      partition.beginLeaderEpochAwareOffsetLookup(leaderEpoch)
      val created = new Slot(partition, identity, leaderEpoch, brokerEpoch)
      slots.put(identity, created)
      created
    }
    provider.open(identity, aggregate, leaderEpoch, brokerEpoch, metadataOffset,
      () => current(attempt), _ => signal(identity)).whenComplete { (opened, failure) =>
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

  private def current(slot: Slot): Boolean = {
    if (closed || !slot.live) return false
    val image = context.metadataCache.getImage
    val topic = image.topics().getTopic(slot.identity.topicId())
    val registration = image.cluster().broker(context.config.brokerId)
    val assignment = if (topic == null) null else topic.partitions().get(slot.identity.partition())
    assignment != null && assignment.leader == context.config.brokerId && assignment.leaderEpoch == slot.epoch &&
      assignment.replicas.length == 1 && assignment.replicas(0) == context.config.brokerId &&
      assignment.isr.length == 1 && assignment.isr(0) == context.config.brokerId &&
      registration != null && registration.epoch() == slot.brokerEpoch && !registration.fenced() &&
      !registration.inControlledShutdown() && context.brokerEpochSupplier() == slot.brokerEpoch
  }

  def resign(identity: TopicIdPartition, epoch: Int, timeout: Duration): CompletableFuture[Void] = {
    val closing = guard.synchronized {
      slots.get(identity).filter(_.epoch <= epoch).map { slot => slots.remove(identity); revoke(slot); slot }
    }
    closeSlot(closing)
  }
  def delete(identity: TopicIdPartition, metadataOffset: Long, timeout: Duration): CompletableFuture[Void] = {
    val closing = guard.synchronized { slots.remove(identity).map { slot => revoke(slot); slot } }
    closeSlot(closing)
  }
  private def closeSlot(slot: Option[Slot]): CompletableFuture[Void] = {
    // Physical session draining is owned by the provider; a stale resign cannot close another owner's admission.
    slot.foreach(_.opened.foreach(_.fence()))
    CompletableFuture.completedFuture(null)
  }
  def beginDrain(): Unit = guard.synchronized {
    closed = true
    slots.values.foreach(revoke)
    slots.clear()
  }
  def shutdown(): CompletableFuture[Void] = { beginDrain(); CompletableFuture.completedFuture(null) }
  override def close(): Unit = beginDrain()
  private def revoke(slot: Slot): Unit = {
    slot.live = false
    slot.opened.foreach(_.fence())
    for (log <- slot.log; opened <- slot.opened) log.removeStorage(slot.epoch, opened.storage())
    slot.lookup.foreach(slot.partition.removeLeaderEpochAwareOffsetLookup(slot.epoch, _))
    slot.partition.cancelLeaderEpochAwareOffsetLookup(slot.epoch)
    slot.result.completeExceptionally(new FencedLeaderEpochException("Nereus native owner was revoked"))
    signal(slot.identity)
  }
  def subscribe(identity: TopicIdPartition, wakeup: Runnable): AutoCloseable = guard.synchronized {
    val listeners = subscriptions.computeIfAbsent(identity, _ => new CopyOnWriteArrayList[Runnable])
    listeners.add(wakeup)
    () => { listeners.remove(wakeup); () }
  }
  private def signal(identity: TopicIdPartition): Unit = {
    val listeners = subscriptions.get(identity)
    if (listeners != null) listeners.forEach(_.run())
  }
  private final class Slot(val partition: Partition, val identity: TopicIdPartition, val epoch: Int, val brokerEpoch: Long) {
    @volatile var live = true
    val result = new CompletableFuture[Void]
    var opened: Option[NereusKafkaOwnedProviderRuntime#Opened] = None
    var log: Option[NereusUnifiedLog] = None
    var lookup: Option[NereusListOffsetsBridge] = None
  }
}
