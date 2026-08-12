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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NereusTopicProfileResolverV1Test {
    @Test
    void resolvesDefaultAndExactLastWinsOverride() {
        NereusKafkaMetadataPolicyV1 policy = KafkaTopicBindingTestFixtures.metadataPolicy();
        assertEquals(
            new NereusTopicProfileResolverV1.Resolution(
                StorageProfileV1.BOOKKEEPER_WAL_ONLY,
                ProfileOriginV1.DEPLOYMENT_USER_DEFAULT),
            NereusTopicProfileResolverV1.resolve(
                "user-topic", new CreatableTopicConfigCollection(), policy));

        CreatableTopicConfigCollection configs = new CreatableTopicConfigCollection();
        configs.add(profile(StorageProfileV1.OBJECT_WAL.name()));
        configs.add(profile(StorageProfileV1.BOOKKEEPER_WAL_ASYNC_OBJECT.name()));
        assertEquals(
            new NereusTopicProfileResolverV1.Resolution(
                StorageProfileV1.BOOKKEEPER_WAL_ASYNC_OBJECT,
                ProfileOriginV1.TOPIC_EXPLICIT),
            NereusTopicProfileResolverV1.resolve("user-topic", configs, policy));
    }

    @Test
    void freezesInternalTopicPolicyAndTreatsRemoteLogTopicAsUserTopic() {
        NereusKafkaMetadataPolicyV1 policy = KafkaTopicBindingTestFixtures.metadataPolicy();
        NereusTopicProfileResolverV1.Resolution expectedInternal =
            new NereusTopicProfileResolverV1.Resolution(
                StorageProfileV1.BOOKKEEPER_WAL_ONLY,
                ProfileOriginV1.DEPLOYMENT_INTERNAL);
        assertEquals(expectedInternal, NereusTopicProfileResolverV1.resolve(
            Topic.GROUP_METADATA_TOPIC_NAME, new CreatableTopicConfigCollection(), policy));
        assertEquals(expectedInternal, NereusTopicProfileResolverV1.resolve(
            Topic.TRANSACTION_STATE_TOPIC_NAME, new CreatableTopicConfigCollection(), policy));
        assertThrows(InvalidConfigurationException.class, () ->
            NereusTopicProfileResolverV1.resolve(
                Topic.SHARE_GROUP_STATE_TOPIC_NAME,
                new CreatableTopicConfigCollection(),
                policy));
        assertEquals(ProfileOriginV1.DEPLOYMENT_USER_DEFAULT,
            NereusTopicProfileResolverV1.resolve(
                "__remote_log_metadata", new CreatableTopicConfigCollection(), policy).origin());
    }

    @Test
    void rejectsInternalOverrideAndNonCanonicalValues() {
        NereusKafkaMetadataPolicyV1 policy = KafkaTopicBindingTestFixtures.metadataPolicy();
        CreatableTopicConfigCollection internalOverride = new CreatableTopicConfigCollection();
        internalOverride.add(profile(StorageProfileV1.BOOKKEEPER_WAL_ONLY.name()));
        assertThrows(InvalidConfigurationException.class, () ->
            NereusTopicProfileResolverV1.resolve(
                Topic.GROUP_METADATA_TOPIC_NAME, internalOverride, policy));

        for (String value : new String[] {null, "", " OBJECT_WAL", "object_wal", "UNKNOWN"}) {
            CreatableTopicConfigCollection configs = new CreatableTopicConfigCollection();
            configs.add(profile(value));
            assertThrows(InvalidConfigurationException.class, () ->
                NereusTopicProfileResolverV1.resolve("user-topic", configs, policy));
        }
    }

    private static CreatableTopicConfig profile(String value) {
        return new CreatableTopicConfig()
            .setName(NereusTopicProfileResolverV1.PROFILE_CONFIG)
            .setValue(value);
    }
}
