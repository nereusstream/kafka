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

import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfigCollection;

import com.nereusstream.domain.aggregate.ProfileOriginV1;
import com.nereusstream.domain.aggregate.StorageProfileV1;

import java.util.Objects;

/** Closed profile resolver for the input-only CreateTopics pseudo-config. */
public final class NereusTopicProfileResolverV1 {
    public static final String PROFILE_CONFIG = "nereus.storage.profile";

    public record Resolution(StorageProfileV1 profile, ProfileOriginV1 origin) {
        public Resolution {
            Objects.requireNonNull(profile, "profile");
            Objects.requireNonNull(origin, "origin");
        }
    }

    private NereusTopicProfileResolverV1() {
    }

    public static Resolution resolve(
        String topicName,
        CreatableTopicConfigCollection configs,
        NereusKafkaMetadataPolicyV1 policy
    ) {
        Objects.requireNonNull(topicName, "topicName");
        Objects.requireNonNull(configs, "configs");
        Objects.requireNonNull(policy, "policy");

        boolean explicit = false;
        String value = null;
        for (CreatableTopicConfig config : configs) {
            if (PROFILE_CONFIG.equals(config.name())) {
                explicit = true;
                value = config.value();
            }
        }

        if (Topic.isInternal(topicName)) {
            if (explicit) {
                throw new InvalidConfigurationException(
                    PROFILE_CONFIG + " cannot be set explicitly for Kafka internal topic " + topicName);
            }
            if (Topic.SHARE_GROUP_STATE_TOPIC_NAME.equals(topicName)) {
                throw new InvalidConfigurationException(
                    "Kafka internal topic " + topicName + " has no admitted Nereus V2 profile");
            }
            return new Resolution(StorageProfileV1.BOOKKEEPER_WAL_ONLY, ProfileOriginV1.DEPLOYMENT_INTERNAL);
        }

        if (!explicit) {
            return new Resolution(
                policy.userTopicDefaultProfile(),
                ProfileOriginV1.DEPLOYMENT_USER_DEFAULT);
        }
        if (value == null || value.isEmpty() || !value.equals(value.trim())) {
            throw new InvalidConfigurationException(
                PROFILE_CONFIG + " must be one exact non-empty canonical profile name");
        }
        final StorageProfileV1 profile;
        try {
            profile = StorageProfileV1.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigurationException(
                "Unknown " + PROFILE_CONFIG + " value: " + value);
        }
        return new Resolution(profile, ProfileOriginV1.TOPIC_EXPLICIT);
    }

    public static boolean isProfileConfig(String name) {
        return PROFILE_CONFIG.equals(name);
    }
}
