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

package kafka.server.nereus;

import com.nereusstream.kafka.activation.KafkaStorageActivationPolicy;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;

import java.time.Duration;
import java.util.Objects;

/** Immutable provider-neutral configuration for one controller-side activation runtime. */
public record NereusKafkaControllerRuntimeConfiguration(
        String nereusCluster,
        String kafkaClusterId,
        OxiaClientConfiguration oxia,
        KafkaStorageActivationPolicy activationPolicy,
        Duration retryInterval
) {
    public NereusKafkaControllerRuntimeConfiguration {
        nereusCluster = nonblank(nereusCluster, "nereusCluster");
        kafkaClusterId = nonblank(kafkaClusterId, "kafkaClusterId");
        Objects.requireNonNull(oxia, "oxia");
        Objects.requireNonNull(activationPolicy, "activationPolicy");
        Objects.requireNonNull(retryInterval, "retryInterval");
        if (!activationPolicy.kafkaClusterId().equals(kafkaClusterId)) {
            throw new IllegalArgumentException(
                    "activation policy Kafka cluster must match runtime configuration");
        }
        if (retryInterval.isNegative()
                || retryInterval.isZero()
                || retryInterval.toMillis() <= 0) {
            throw new IllegalArgumentException(
                    "retryInterval must be positive and millisecond-representable");
        }
    }

    private static String nonblank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must be nonblank");
        }
        return value;
    }
}
