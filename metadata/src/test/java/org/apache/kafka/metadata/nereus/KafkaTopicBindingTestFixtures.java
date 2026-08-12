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

import com.nereusstream.domain.aggregate.FrameEncodingPolicyCatalogV1;
import com.nereusstream.domain.aggregate.InitialStorageEpochV1;
import com.nereusstream.domain.aggregate.PolicyCatalogDigest;
import com.nereusstream.domain.aggregate.ProfileOriginV1;
import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.domain.aggregate.TopicBindingAggregateV1;
import com.nereusstream.domain.aggregate.TopicBindingV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.DeterministicTopicIdsV1;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaCellId;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaProtocolCellIdentity;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.domain.protocol.ProtocolKindV1;

import java.nio.charset.StandardCharsets;

public final class KafkaTopicBindingTestFixtures {
    private KafkaTopicBindingTestFixtures() {
    }

    public static KafkaTopicBindingAggregateV1 aggregate(Uuid topicId, String topicName) {
        KafkaProtocolCellIdentity cell = new KafkaProtocolCellIdentity(
            new DeploymentId(new Id128(1L, 2L)),
            new KafkaCellId(new Id128(3L, 4L)));
        KafkaTopicIncarnationIdentity incarnation = new KafkaTopicIncarnationIdentity(
            new KafkaTopicId(new Id128(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits())),
            new KafkaTopicName(topicName));
        TopicBindingId bindingId = DeterministicTopicIdsV1.deriveBindingId(cell, incarnation);
        StorageEpochId epochId = DeterministicTopicIdsV1.deriveStorageEpochId(bindingId, 0L);
        return new KafkaTopicBindingAggregateV1(new TopicBindingAggregateV1(
            TopicBindingAggregateV1.SCHEMA_VERSION,
            new TopicBindingV1(ProtocolKindV1.KAFKA, bindingId, cell, incarnation),
            new InitialStorageEpochV1(
                epochId,
                0L,
                StorageProfileV1.BOOKKEEPER_WAL_ONLY,
                ProfileOriginV1.TOPIC_EXPLICIT,
                new PolicyCatalogDigest(Sha256Digest.hash(CanonicalBytes.copyOf(
                    "policy-v1".getBytes(StandardCharsets.US_ASCII)))),
                FrameEncodingPolicyCatalogV1.none())));
    }

    public static NereusKafkaMetadataPolicyV1 metadataPolicy() {
        return new NereusKafkaMetadataPolicyV1(
            new Uuid(1L, 2L),
            new Uuid(3L, 4L),
            Sha256Digest.hash(CanonicalBytes.copyOf(
                "policy-v1".getBytes(StandardCharsets.US_ASCII))).bytes().toByteArray(),
            StorageProfileV1.BOOKKEEPER_WAL_ONLY,
            NereusKafkaMetadataPolicyV1.INTERNAL_TOPIC_POLICY_VERSION,
            false);
    }
}
