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
import org.apache.kafka.common.metadata.TopicBindingAggregateRecord;

import com.nereusstream.domain.aggregate.FrameEncodingPolicyValueV1;
import com.nereusstream.domain.aggregate.InitialStorageEpochV1;
import com.nereusstream.domain.aggregate.PolicyCatalogDigest;
import com.nereusstream.domain.aggregate.ProfileOriginV1;
import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.domain.aggregate.TopicBindingAggregateV1;
import com.nereusstream.domain.aggregate.TopicBindingAggregateValidatorV1;
import com.nereusstream.domain.aggregate.TopicBindingV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
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

import java.util.Objects;

/** Direct generated-record to N1-domain mapping for API-key-32000 wire version 0. */
public final class KafkaTopicBindingAggregateMapperV1 {
    public static final short WIRE_VERSION = 0;

    private KafkaTopicBindingAggregateMapperV1() {
    }

    public static KafkaTopicBindingAggregateV1 fromRecord(TopicBindingAggregateRecord record, short version) {
        Objects.requireNonNull(record, "record");
        requireWireVersion(version);
        if (record.initialSealedEnd() != null) {
            throw new IllegalArgumentException("API-key-32000 wire v0 initial sealed end must be absent");
        }

        ProtocolKindV1 protocolKind = ProtocolKindV1.fromCode(Short.toUnsignedInt(record.protocolKind()));
        if (protocolKind != ProtocolKindV1.KAFKA) {
            throw new IllegalArgumentException("API-key-32000 wire v0 accepts only the Kafka protocol kind");
        }
        Id128 topicId = id128(record.topicId(), "topic ID");
        KafkaTopicIncarnationIdentity incarnation = new KafkaTopicIncarnationIdentity(
            new KafkaTopicId(topicId), new KafkaTopicName(record.topicName()));
        KafkaProtocolCellIdentity cell = new KafkaProtocolCellIdentity(
            new DeploymentId(id128(record.deploymentId(), "deployment ID")),
            new KafkaCellId(id128(record.kafkaCellId(), "Kafka Cell ID")));
        FrameEncodingPolicyValueV1 framePolicy = new FrameEncodingPolicyValueV1(
            Short.toUnsignedInt(record.frameEncodingPolicyKind()),
            Short.toUnsignedInt(record.frameEncodingPolicyVersion()),
            CanonicalBytes.copyOf(requireBytes(record.frameEncodingPolicyPayload(), "frame-policy payload")));
        TopicBindingAggregateV1 aggregate = new TopicBindingAggregateV1(
            Short.toUnsignedInt(record.aggregateSchemaVersion()),
            new TopicBindingV1(
                protocolKind,
                new TopicBindingId(Sha256Digest.copyOf(requireBytes(record.bindingId(), "Topic Binding ID"))),
                cell,
                incarnation),
            new InitialStorageEpochV1(
                new StorageEpochId(Sha256Digest.copyOf(requireBytes(record.storageEpochId(), "Storage Epoch ID"))),
                record.epochOrdinal(),
                StorageProfileV1.fromCode(Short.toUnsignedInt(record.storageProfile())),
                ProfileOriginV1.fromCode(Short.toUnsignedInt(record.profileOrigin())),
                new PolicyCatalogDigest(
                    Sha256Digest.copyOf(requireBytes(record.policyCatalogDigest(), "policy catalog digest"))),
                framePolicy));
        TopicBindingAggregateValidatorV1.validate(aggregate);
        KafkaTopicBindingAggregateV1 result = new KafkaTopicBindingAggregateV1(aggregate);
        TopicBindingAggregateRecord canonical = toRecord(result);
        if (!canonical.equals(record)) {
            throw new IllegalArgumentException("API-key-32000 wire v0 record is not canonical");
        }
        return result;
    }

    public static TopicBindingAggregateRecord toRecord(KafkaTopicBindingAggregateV1 aggregate) {
        Objects.requireNonNull(aggregate, "aggregate");
        TopicBindingAggregateV1 value = aggregate.value();
        TopicBindingAggregateValidatorV1.validate(value);
        if (!(value.binding().cellIdentity() instanceof KafkaProtocolCellIdentity cell)
                || !(value.binding().incarnationIdentity() instanceof KafkaTopicIncarnationIdentity incarnation)) {
            throw new IllegalArgumentException("Kafka physical record requires Kafka Cell and incarnation variants");
        }
        FrameEncodingPolicyValueV1 framePolicy = value.initialEpoch().frameEncodingPolicy();
        return new TopicBindingAggregateRecord()
            .setTopicId(uuid(incarnation.topicId().value()))
            .setTopicName(incarnation.topicName().value())
            .setAggregateSchemaVersion(unsignedShort(value.aggregateSchemaVersion(), "aggregate schema version"))
            .setProtocolKind(unsignedShort(value.binding().protocolKind().code(), "protocol kind"))
            .setBindingId(value.binding().bindingId().digest().bytes().toByteArray())
            .setDeploymentId(uuid(cell.deploymentId().value()))
            .setKafkaCellId(uuid(cell.cellId().value()))
            .setStorageEpochId(value.initialEpoch().storageEpochId().digest().bytes().toByteArray())
            .setEpochOrdinal(value.initialEpoch().epochOrdinal())
            .setStorageProfile(unsignedShort(value.initialEpoch().storageProfile().code(), "storage profile"))
            .setProfileOrigin(unsignedShort(value.initialEpoch().profileOrigin().code(), "profile origin"))
            .setPolicyCatalogDigest(value.initialEpoch().policyCatalogDigest().digest().bytes().toByteArray())
            .setFrameEncodingPolicyKind(unsignedShort(framePolicy.kind(), "frame-policy kind"))
            .setFrameEncodingPolicyVersion(unsignedShort(framePolicy.formatVersion(), "frame-policy version"))
            .setFrameEncodingPolicyPayload(framePolicy.payload().toByteArray())
            .setInitialSealedEnd(null);
    }

    public static void validateBackReference(
        KafkaTopicBindingAggregateV1 aggregate,
        Uuid expectedTopicId,
        String expectedTopicName
    ) {
        Objects.requireNonNull(aggregate, "aggregate");
        Objects.requireNonNull(expectedTopicId, "expectedTopicId");
        Objects.requireNonNull(expectedTopicName, "expectedTopicName");
        TopicBindingAggregateV1 value = aggregate.value();
        if (!(value.binding().incarnationIdentity() instanceof KafkaTopicIncarnationIdentity incarnation)) {
            throw new IllegalArgumentException("topic image aggregate is not a Kafka incarnation");
        }
        if (!uuid(incarnation.topicId().value()).equals(expectedTopicId)
                || !incarnation.topicName().value().equals(expectedTopicName)) {
            throw new IllegalArgumentException("topic aggregate back-reference does not match TopicRecord");
        }
    }

    private static void requireWireVersion(short version) {
        if (version != WIRE_VERSION) {
            throw new IllegalArgumentException("unsupported TopicBindingAggregateRecord wire version: " + version);
        }
    }

    private static byte[] requireBytes(byte[] value, String field) {
        return Objects.requireNonNull(value, field);
    }

    private static Id128 id128(Uuid value, String field) {
        Objects.requireNonNull(value, field);
        return new Id128(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    private static Uuid uuid(Id128 value) {
        return new Uuid(value.highBits(), value.lowBits());
    }

    private static short unsignedShort(int value, String field) {
        if (value < 0 || value > 0xffff) {
            throw new IllegalArgumentException(field + " does not fit u16");
        }
        return (short) value;
    }
}
