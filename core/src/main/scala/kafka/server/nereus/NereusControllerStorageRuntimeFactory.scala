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

import java.util.Objects

/** Explicit adapter-backed controller factory with provider I/O deferred to ControllerStorageRuntime.start(). */
final class NereusControllerStorageRuntimeFactory(
  mapper: NereusKafkaRuntimeConfigurationMapper,
  activationCreator: NereusKafkaControllerActivationCreator
) extends ControllerStorageRuntimeFactory {
  Objects.requireNonNull(mapper, "mapper")
  Objects.requireNonNull(activationCreator, "activationCreator")

  override def create(context: ControllerStorageRuntimeContext): ControllerStorageRuntime = {
    val exact = Objects.requireNonNull(context, "context")
    if (!exact.config.nereusKafkaStorageConfig.enabled()) {
      return ControllerStorageRuntimeFactory.Disabled.create(exact)
    }
    val mapped = mapper.mapController(
      exact.config.nereusKafkaStorageConfig,
      exact.clusterId)
    val clusterSnapshots = new NereusKafkaStorageClusterSnapshotProvider(
      exact.clusterId,
      exact.metadataCache,
      exact.logDirectories)
    val clock = new NereusKafkaClock(exact.time)
    new NereusControllerStorageRuntime(
      exact.nodeId,
      () => activationCreator.create(mapped, clusterSnapshots, clock),
      mapped.retryInterval,
      exact.faultHandler)
  }
}

object NereusControllerStorageRuntimeFactory {
  def production(): NereusControllerStorageRuntimeFactory =
    new NereusControllerStorageRuntimeFactory(
      new NereusKafkaRuntimeConfigurationMapper,
      new NereusKafkaControllerActivationCreator)
}
