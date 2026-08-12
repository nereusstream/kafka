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
package org.apache.kafka.metadata.nereus;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.List;
import java.util.Objects;

/** Side-effect-free, fully validated candidate for one successful CreateTopics item. */
public record TopicCreateCandidateV1(
    Uuid topicId,
    int numPartitions,
    CreatableTopicResult response,
    List<ApiMessageAndVersion> records
) {
    public TopicCreateCandidateV1 {
        Objects.requireNonNull(topicId, "topicId");
        Objects.requireNonNull(response, "response");
        records = List.copyOf(records);
        if (numPartitions <= 0 || records.isEmpty()) {
            throw new IllegalArgumentException("topic candidate must contain partitions and records");
        }
    }
}
