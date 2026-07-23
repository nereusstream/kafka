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

package kafka.server.metadata

import kafka.cluster.Partition
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.image.{MetadataImage, TopicsDelta}

import java.util.concurrent.CompletableFuture

/**
 * Optional post-ReplicaManager topic lifecycle seam for storage engines whose leader recovery is asynchronous.
 * Implementations invoke the callbacks only after the corresponding partition operation is fully ready.
 */
trait AsyncTopicDeltaLifecycle {
  /** Called synchronously under ReplicaManager's state-change lock immediately after a new leader epoch is published. */
  def onLeaderStatePublished(partition: Partition, topicId: Uuid, leaderEpoch: Int): Unit

  def applyAfterReplicaManager(
    delta: TopicsDelta,
    newImage: MetadataImage,
    onLeaderReady: (TopicPartition, Int) => Unit,
    onResigned: (TopicPartition, Option[Int]) => Unit
  ): CompletableFuture[Void]
}
