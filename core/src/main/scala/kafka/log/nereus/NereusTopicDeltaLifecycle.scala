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

import com.nereusstream.api.{ErrorCode, NereusException, StorageProfile}
import com.nereusstream.kafka.partition.{KafkaPartitionIdentity, KafkaPartitionLeaderOpenRequest}
import kafka.cluster.Partition
import kafka.server.ReplicaManager
import kafka.server.metadata.AsyncTopicDeltaLifecycle
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.image.{MetadataImage, TopicsDelta}

import java.time.Duration
import java.util.Objects
import java.util.concurrent.CompletableFuture
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Projects one already-applied stock Kafka topic delta into the product-owned Nereus partition lifecycle. The stock
 * ReplicaManager remains the owner of Partition objects; the adapter manager remains the only storage/recovery owner.
 */
final class NereusTopicDeltaLifecycle(
  kafkaClusterId: String,
  brokerId: Int,
  brokerEpochSupplier: () => Long,
  storageProfile: StorageProfile,
  operationTimeout: Duration,
  replicaManager: ReplicaManager,
  partitionLifecycle: NereusListOffsetsLifecycle
) extends AsyncTopicDeltaLifecycle {
  Objects.requireNonNull(kafkaClusterId, "kafkaClusterId")
  Objects.requireNonNull(brokerEpochSupplier, "brokerEpochSupplier")
  Objects.requireNonNull(storageProfile, "storageProfile")
  Objects.requireNonNull(replicaManager, "replicaManager")
  Objects.requireNonNull(partitionLifecycle, "partitionLifecycle")
  if (kafkaClusterId.isBlank || brokerId < 0) {
    throw new IllegalArgumentException("Kafka cluster ID must be nonblank and broker ID must be non-negative")
  }
  requirePositive(operationTimeout)

  override def onLeaderStatePublished(partition: Partition, topicId: Uuid, leaderEpoch: Int): Unit = {
    Objects.requireNonNull(partition, "partition")
    Objects.requireNonNull(topicId, "topicId")
    if (!partition.topicId.contains(topicId)) {
      throw invariant("Stock ReplicaManager published a Nereus leader with a mismatched topic ID")
    }
    partition.beginLeaderEpochAwareOffsetLookup(leaderEpoch)
  }

  override def applyAfterReplicaManager(
    delta: TopicsDelta,
    newImage: MetadataImage,
    onLeaderReady: (TopicPartition, Int) => Unit,
    onResigned: (TopicPartition, Option[Int]) => Unit
  ): CompletableFuture[Void] = {
    Objects.requireNonNull(delta, "delta")
    Objects.requireNonNull(newImage, "newImage")
    Objects.requireNonNull(onLeaderReady, "onLeaderReady")
    Objects.requireNonNull(onResigned, "onResigned")

    val changes = delta.localChanges(brokerId)
    if (changes.deletes().isEmpty && changes.followers().isEmpty && changes.leaders().isEmpty) {
      return CompletableFuture.completedFuture(null)
    }
    val metadataOffset = newImage.provenance().lastContainedOffset()
    if (metadataOffset < 0) {
      return failPreparedLeaders(
        changes.electedLeaders().asScala,
        invariant("Nereus partition lifecycle requires a non-negative KRaft metadata offset"))
    }
    val brokerEpoch = if (changes.leaders().isEmpty) {
      0L
    } else {
      try {
        brokerEpochSupplier()
      } catch {
        case failure: Throwable =>
          return failPreparedLeaders(changes.electedLeaders().asScala, failure)
      }
    }
    if (!changes.leaders().isEmpty && brokerEpoch < 0) {
      return failPreparedLeaders(
        changes.electedLeaders().asScala,
        invariant("Nereus leader open requires a non-negative broker registration epoch"))
    }

    val tails = mutable.HashMap.empty[TopicPartition, CompletableFuture[Void]]
    def append(topicPartition: TopicPartition)(operation: => CompletableFuture[Void]): Unit = {
      val previous = tails.getOrElse(topicPartition, CompletableFuture.completedFuture(null))
      val next = previous.thenCompose(_ => {
        try {
          Objects.requireNonNull(operation, "Nereus partition lifecycle returned a null future")
        } catch {
          case failure: Throwable => CompletableFuture.failedFuture[Void](failure)
        }
      })
      tails.put(topicPartition, next)
    }

    // Delete must precede a same-name/topic-partition recreation carried in the same metadata delta.
    changes.deletes().asScala.foreach { topicPartition =>
      append(topicPartition) {
        val identity = deletedIdentity(delta, topicPartition)
        partitionLifecycle.delete(identity, metadataOffset, operationTimeout)
          .thenRun(() => onResigned(topicPartition, None))
      }
    }

    changes.followers().asScala.foreach { case (topicPartition, info) =>
      append(topicPartition) {
        val observedLeaderEpoch = info.partition().leaderEpoch
        partitionLifecycle.resign(
          identity(topicPartition, info.topicId().toString),
          observedLeaderEpoch,
          operationTimeout)
          .thenRun(() => onResigned(topicPartition, Some(observedLeaderEpoch)))
      }
    }

    changes.leaders().asScala.foreach { case (topicPartition, info) =>
      append(topicPartition) {
        val partition = replicaManager.onlinePartition(topicPartition).getOrElse {
          throw invariant("Stock ReplicaManager did not publish the Nereus leader partition before recovery")
        }
        try {
          val request = new KafkaPartitionLeaderOpenRequest(
            identity(topicPartition, info.topicId().toString),
            brokerId,
            info.partition().leaderEpoch,
            brokerEpoch,
            storageProfile,
            metadataOffset,
            operationTimeout)
          val opened = partitionLifecycle.openLeader(partition, request)
          if (changes.electedLeaders().containsKey(topicPartition)) {
            opened
              .thenRun(() => onLeaderReady(topicPartition, info.partition().leaderEpoch))
              .whenComplete((_, failure) => {
                if (failure != null) {
                  partition.cancelLeaderEpochAwareOffsetLookup(info.partition().leaderEpoch)
                }
              })
          } else {
            opened.thenRun(() => ())
          }
        } catch {
          case failure: Throwable =>
            if (changes.electedLeaders().containsKey(topicPartition)) {
              partition.cancelLeaderEpochAwareOffsetLookup(info.partition().leaderEpoch)
            }
            throw failure
        }
      }
    }

    CompletableFuture.allOf(tails.values.toArray: _*)
  }

  private def deletedIdentity(delta: TopicsDelta, topicPartition: TopicPartition): KafkaPartitionIdentity = {
    val topic = delta.image().getTopic(topicPartition.topic())
    if (topic == null || !topic.partitions().containsKey(topicPartition.partition())) {
      throw invariant("Deleted Nereus partition is absent from the previous KRaft metadata image")
    }
    identity(topicPartition, topic.id().toString)
  }

  private def identity(topicPartition: TopicPartition, topicId: String): KafkaPartitionIdentity =
    new KafkaPartitionIdentity(
      kafkaClusterId,
      topicId,
      topicPartition.partition(),
      topicPartition.topic())

  private def failPreparedLeaders(
    leaders: collection.Map[TopicPartition, org.apache.kafka.image.LocalReplicaChanges.PartitionInfo],
    failure: Throwable
  ): CompletableFuture[Void] = {
    leaders.foreach { case (topicPartition, info) =>
      replicaManager.onlinePartition(topicPartition).foreach(
        _.cancelLeaderEpochAwareOffsetLookup(info.partition().leaderEpoch))
    }
    CompletableFuture.failedFuture(failure)
  }

  private def requirePositive(timeout: Duration): Unit = {
    if (timeout == null || timeout.isZero || timeout.isNegative || timeout.toMillis <= 0) {
      throw new IllegalArgumentException("operationTimeout must be positive and millisecond-representable")
    }
  }

  private def invariant(message: String): NereusException =
    new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message)
}
