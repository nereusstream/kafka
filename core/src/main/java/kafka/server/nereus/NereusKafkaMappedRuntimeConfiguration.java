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

import kafka.log.nereus.NereusListOffsetsScanConfig;

import com.nereusstream.kafka.activation.KafkaBrokerCapabilitySpecification;
import com.nereusstream.kafka.runtime.NereusKafkaMaintenanceConfiguration;
import com.nereusstream.kafka.runtime.NereusKafkaObjectWalRuntimeConfiguration;

import java.util.Objects;

/** Exact side-effect-free result of mapping one broker epoch's typed Kafka configuration. */
public record NereusKafkaMappedRuntimeConfiguration(
        NereusKafkaObjectWalRuntimeConfiguration runtime,
        KafkaBrokerCapabilitySpecification capability,
        NereusListOffsetsScanConfig listOffsets,
        NereusKafkaMaintenanceConfiguration maintenance,
        String objectProviderToken
) {
    public NereusKafkaMappedRuntimeConfiguration {
        Objects.requireNonNull(runtime, "runtime");
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(listOffsets, "listOffsets");
        Objects.requireNonNull(maintenance, "maintenance");
        Objects.requireNonNull(objectProviderToken, "objectProviderToken");
        if (objectProviderToken.isBlank()) {
            throw new IllegalArgumentException("objectProviderToken must be nonblank");
        }
    }
}
