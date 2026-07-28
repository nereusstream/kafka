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

import org.apache.kafka.image.publisher.MetadataPublisher

import java.util.concurrent.CompletionStage

/**
 * Stock-owned controller lifecycle seam for an optional authoritative-storage activation runtime.
 *
 * The runtime is also a metadata publisher so controller leadership and image callbacks remain ordered by the stock
 * MetadataLoader. start only creates runtime resources; it must not wait for first activation to complete.
 */
trait ControllerStorageRuntime extends MetadataPublisher {
  def start(): CompletionStage[Void]
}
