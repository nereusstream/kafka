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

import kafka.server.KafkaConfig

import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.KRaftMetadataCache
import org.apache.kafka.server.util.KafkaScheduler

import java.util.Objects

/** Explicit owned/borrowed Kafka dependencies supplied to a broker storage runtime factory. */
final case class BrokerStorageRuntimeContext(
  config: KafkaConfig,
  clusterId: String,
  brokerEpochSupplier: () => Long,
  metadataCache: KRaftMetadataCache,
  time: Time,
  metrics: Metrics,
  scheduler: KafkaScheduler
) {
  Objects.requireNonNull(config, "config")
  Objects.requireNonNull(clusterId, "clusterId")
  Objects.requireNonNull(brokerEpochSupplier, "brokerEpochSupplier")
  Objects.requireNonNull(metadataCache, "metadataCache")
  Objects.requireNonNull(time, "time")
  Objects.requireNonNull(metrics, "metrics")
  Objects.requireNonNull(scheduler, "scheduler")
  require(clusterId.nonEmpty, "clusterId must be nonempty")
}
