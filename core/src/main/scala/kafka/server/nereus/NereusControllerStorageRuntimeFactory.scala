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

import kafka.server.storage.{ControllerStorageRuntime, ControllerStorageRuntimeContext, ControllerStorageRuntimeFactory}
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.image.loader.LoaderManifest
import org.apache.kafka.metadata.nereus.KafkaTopicBindingImageValidatorV1
import org.apache.kafka.server.config.NereusKafkaStorageConfig

import java.util.concurrent.{CompletableFuture, CompletionStage}

/** Native assignment remains Controller-owned. Physical write admission is closed and recovered by the elected Broker. */
final class NereusControllerStorageRuntimeFactory extends ControllerStorageRuntimeFactory {
  override def metadataPolicy(config: kafka.server.KafkaConfig): Option[org.apache.kafka.metadata.nereus.NereusKafkaMetadataPolicyV1] =
    if (config.nereusKafkaStorageConfig.enabled()) Some(NereusKafkaOwnedProviderRuntime.metadataPolicy(config)) else None

  override def create(context: ControllerStorageRuntimeContext): ControllerStorageRuntime = {
    if (!context.config.nereusKafkaStorageConfig.enabled()) return ControllerStorageRuntimeFactory.Disabled.create(context)
    if (context.config.nereusKafkaStorageConfig.core().profile() != NereusKafkaStorageConfig.Profile.BOOKKEEPER_WAL_ONLY) {
      throw new IllegalArgumentException("NSIP-1 native Controller admits BOOKKEEPER_WAL_ONLY; Object authority is pending")
    }
    new ControllerStorageRuntime {
      override def name(): String = "NereusControllerStorageRuntime"
      override def start(): CompletionStage[Void] = CompletableFuture.completedFuture(null)
      override def onMetadataUpdate(delta: MetadataDelta, image: MetadataImage, manifest: LoaderManifest): Unit =
        KafkaTopicBindingImageValidatorV1.validatePublication(image.features(), image.topics(), delta.topicsDelta(), true)
      override def close(): Unit = ()
    }
  }
}
object NereusControllerStorageRuntimeFactory {
  def production(): NereusControllerStorageRuntimeFactory = new NereusControllerStorageRuntimeFactory
}
