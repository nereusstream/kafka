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

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.image.loader.LoaderManifest
import org.apache.kafka.server.config.NereusKafkaConfigs

import java.util.concurrent.{CompletableFuture, CompletionStage}

/** Explicit controller-runtime injection point; no reflection or process-global registry is used. */
trait ControllerStorageRuntimeFactory {
  def create(context: ControllerStorageRuntimeContext): ControllerStorageRuntime
}

object ControllerStorageRuntimeFactory {
  val Disabled: ControllerStorageRuntimeFactory = context => {
    if (context.config.nereusKafkaStorageConfig.enabled()) {
      throw new ConfigException(
        NereusKafkaConfigs.ENABLED_CONFIG,
        true,
        "requires an explicitly installed ControllerStorageRuntimeFactory")
    }
    DisabledControllerStorageRuntime
  }

  private object DisabledControllerStorageRuntime extends ControllerStorageRuntime {
    private val completed = CompletableFuture.completedFuture[Void](null)

    override def name(): String = "DisabledControllerStorageRuntime"

    override def start(): CompletionStage[Void] = completed

    override def onMetadataUpdate(
      delta: MetadataDelta,
      newImage: MetadataImage,
      manifest: LoaderManifest
    ): Unit = { }

    override def close(): Unit = { }
  }
}
