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

import com.nereusstream.kafka.runtime.NereusKafkaRuntime
import com.nereusstream.kafka.recovery.KafkaRecoveryStateFactory
import kafka.log.nereus.NereusListOffsetsScanConfig
import kafka.server.ReplicaManager
import kafka.server.storage.{BrokerStorageRuntime, BrokerStorageRuntimeContext, BrokerStorageRuntimeFactory}

import org.apache.kafka.common.utils.AppInfoParser

import java.nio.file.Path
import java.util.Objects
import java.util.UUID
import java.util.function.Function
import scala.jdk.CollectionConverters._

/**
 * Explicit adapter-backed factory. Provider construction and ListOffsets limits are injected as typed functions so the
 * Kafka fork does not use reflection, a service loader, or a process-global runtime registry.
 */
final class NereusBrokerStorageRuntimeFactory(
  runtimeCreator: Function[BrokerStorageRuntimeContext, NereusKafkaRuntime],
  scanConfigCreator: Function[BrokerStorageRuntimeContext, NereusListOffsetsScanConfig],
  recoveryStateFactoryCreator: Function[ReplicaManager, KafkaRecoveryStateFactory] = null
) extends BrokerStorageRuntimeFactory {
  Objects.requireNonNull(runtimeCreator, "runtimeCreator")
  Objects.requireNonNull(scanConfigCreator, "scanConfigCreator")

  override def create(context: BrokerStorageRuntimeContext): BrokerStorageRuntime = {
    Objects.requireNonNull(context, "context")
    if (!context.config.nereusKafkaStorageConfig.enabled()) {
      return BrokerStorageRuntimeFactory.Disabled.create(context)
    }
    val runtime = Objects.requireNonNull(
      runtimeCreator.apply(context),
      "Nereus runtime creator returned null")
    try {
      val scanConfig = Objects.requireNonNull(
        scanConfigCreator.apply(context),
        "Nereus ListOffsets scan-config creator returned null")
      new NereusBrokerStorageRuntime(
        context,
        runtime,
        scanConfig,
        recoveryStateFactoryCreator)
    } catch {
      case failure: Throwable =>
        try {
          runtime.close()
        } catch {
          case closeFailure: Throwable => failure.addSuppressed(closeFailure)
        }
        throw failure
    }
  }
}

object NereusBrokerStorageRuntimeFactory {
  /** Production composition with the concrete fork-owned stock RecordBatch recovery state factory. */
  def production(): NereusBrokerStorageRuntimeFactory =
    production(replicaManager => new NereusKafkaRecoveryStateFactory(replicaManager))

  /**
   * Creates the production Object-WAL factory while keeping provider I/O behind BrokerStorageRuntime.start().
   * Fresh Kafka recovery state remains fork-owned and is created only after the exact ReplicaManager is available.
   */
  def production(
    recoveryStateFactoryCreator: Function[ReplicaManager, KafkaRecoveryStateFactory]
  ): NereusBrokerStorageRuntimeFactory = {
    val recoveryCreator = Objects.requireNonNull(recoveryStateFactoryCreator, "recoveryStateFactoryCreator")
    val mapper = new NereusKafkaRuntimeConfigurationMapper
    val productCreator = new NereusKafkaProductRuntimeCreator
    new NereusBrokerStorageRuntimeFactory(
      context => deferredRuntime(context, productCreator),
      context => mapper.listOffsets(context.config.nereusKafkaStorageConfig),
      recoveryCreator)
  }

  private def deferredRuntime(
    context: BrokerStorageRuntimeContext,
    creator: NereusKafkaProductRuntimeCreator
  ): NereusKafkaRuntime = {
    val storage = context.config.nereusKafkaStorageConfig
    val runtimeInstanceId = UUID.randomUUID().toString
    val recoveryBridge = new NereusKafkaRecoveryStateFactoryBridge
    new NereusKafkaDeferredRuntime(
      () => context.brokerEpochSupplier(),
      context.scheduler,
      context.time,
      storage.rollout().readinessTimeout(),
      brokerEpoch => creator.create(
        storage,
        context.clusterId,
        context.config.brokerId,
        brokerEpoch,
        runtimeInstanceId,
        AppInfoParser.getVersion,
        nereusBuild,
        System.getProperty("java.version"),
        context.scheduler,
        context.time,
        context.metadataCache,
        context.config.logDirs.asScala.map(Path.of(_)).asJava,
        recoveryBridge),
      recoveryBridge)
  }

  private def nereusBuild: String = {
    Option(classOf[NereusKafkaRuntime].getPackage.getImplementationVersion)
      .filter(_.nonEmpty)
      .getOrElse("development")
  }
}
