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

import com.nereusstream.domain.aggregate.ProfileOriginV1;
import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.domain.aggregate.TopicBindingAggregateV1;

import java.util.Objects;

/**
 * The validated logical aggregate owned by a Kafka topic image.
 *
 * <p>This wrapper keeps the N1 domain dependency behind the metadata package boundary. Kafka image and controller
 * code never construct a second semantic model and never encode temporary NTA1 bytes.
 */
public final class KafkaTopicBindingAggregateV1 {
    private final TopicBindingAggregateV1 value;

    KafkaTopicBindingAggregateV1(TopicBindingAggregateV1 value) {
        this.value = Objects.requireNonNull(value, "value");
    }

    TopicBindingAggregateV1 value() {
        return value;
    }

    public StorageProfileV1 storageProfile() {
        return value.initialEpoch().storageProfile();
    }

    public ProfileOriginV1 profileOrigin() {
        return value.initialEpoch().profileOrigin();
    }

    @Override
    public boolean equals(Object other) {
        return this == other
            || other instanceof KafkaTopicBindingAggregateV1 that && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return "KafkaTopicBindingAggregateV1(" + value + ")";
    }
}
