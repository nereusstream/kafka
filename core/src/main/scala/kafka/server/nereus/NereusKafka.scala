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

import kafka.Kafka
import kafka.server.storage.BrokerStorageRuntimeFactory
import kafka.server.storage.ControllerStorageRuntimeFactory

import org.apache.logging.log4j.LogManager

/**
 * Explicit native-storage process entry point. It reuses the stock Kafka lifecycle and statically injects the Nereus
 * production runtime without reflection, a service loader, or a process-global registry.
 */
object NereusKafka {
  def main(args: Array[String]): Unit = {
    // BookKeeper can initialize Slog loggers while closing handles. The Log4j JVM
    // hook must not stop its context while the broker's storage shutdown still runs.
    System.setProperty("log4j.shutdownHookEnabled", "false")
    Kafka.run(
      args,
      productionBrokerFactory,
      productionControllerFactory,
      afterShutdown = () => LogManager.shutdown())
  }

  private[nereus] def productionBrokerFactory: BrokerStorageRuntimeFactory =
    NereusBrokerStorageRuntimeFactory.production()

  private[nereus] def productionControllerFactory: ControllerStorageRuntimeFactory =
    NereusControllerStorageRuntimeFactory.production()
}
