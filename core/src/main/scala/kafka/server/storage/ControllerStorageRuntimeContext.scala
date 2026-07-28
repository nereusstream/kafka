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
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.KRaftMetadataCache
import org.apache.kafka.server.fault.FaultHandler

import java.nio.file.Path
import java.util
import java.util.Objects

/** Immutable stock Kafka facts needed to build one controller activation runtime. */
final case class ControllerStorageRuntimeContext(
  config: KafkaConfig,
  clusterId: String,
  nodeId: Int,
  metadataCache: KRaftMetadataCache,
  time: Time,
  logDirectories: util.List[Path],
  faultHandler: FaultHandler
) {
  Objects.requireNonNull(config, "config")
  Objects.requireNonNull(clusterId, "clusterId")
  Objects.requireNonNull(metadataCache, "metadataCache")
  Objects.requireNonNull(time, "time")
  Objects.requireNonNull(logDirectories, "logDirectories")
  Objects.requireNonNull(faultHandler, "faultHandler")
  require(clusterId.nonEmpty, "clusterId must be nonempty")
  require(nodeId >= 0, "nodeId must be non-negative")
  require(!logDirectories.isEmpty, "logDirectories must be nonempty")
}
