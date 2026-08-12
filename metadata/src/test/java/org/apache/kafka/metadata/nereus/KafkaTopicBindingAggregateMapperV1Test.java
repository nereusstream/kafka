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
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.server.common.ApiMessageAndVersion;

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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaTopicBindingAggregateMapperV1Test {
    private static final Uuid TOPIC_ID = new Uuid(0x0102030405060708L, 0x1112131415161718L);

    @Test
    void testDirectRoundTrip() {
        KafkaTopicBindingAggregateV1 aggregate = aggregate();
        TopicBindingAggregateRecord record = KafkaTopicBindingAggregateMapperV1.toRecord(aggregate);

        assertEquals(32000, record.apiKey());
        assertEquals(TOPIC_ID, record.topicId());
        assertEquals("alpha.topic", record.topicName());
        assertEquals(null, record.initialSealedEnd());
        assertEquals(
            aggregate,
            KafkaTopicBindingAggregateMapperV1.fromRecord(
                record.duplicate(), KafkaTopicBindingAggregateMapperV1.WIRE_VERSION));
        KafkaTopicBindingAggregateMapperV1.validateBackReference(aggregate, TOPIC_ID, "alpha.topic");
    }

    @Test
    void testStrictWireV0Golden() {
        ApiMessageAndVersion message = new ApiMessageAndVersion(
            KafkaTopicBindingAggregateMapperV1.toRecord(aggregate()), (short) 0);
        MetadataRecordSerde serde = MetadataRecordSerde.INSTANCE;
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer bytes = ByteBuffer.allocate(serde.recordSize(message, cache));
        serde.write(message, cache, new ByteBufferAccessor(bytes));
        assertEquals(
            "0180fa010001020304050607081112131415161718000b616c7068612e746f7069630001000100000020" +
                "4d45e6d8e07af212b1bfda10eb29ea0097585e977de00ec014bc1e92ac9c3722000000000000000100000000" +
                "000000020000000000000003000000000000000400000020171e7220633902a11e75f58a8bef6a5a51ac82e5" +
                "4e8a3d4fbe3eeb31c9204ed80000000000000000000200040000002072993b6cb83904d39a8c73bd0651aa62" +
                "51288ede5dbc2c7bcbdc54cc5bbf5d770000000000000000ffffffff",
            HexFormat.of().formatHex(bytes.array()));
    }

    @Test
    void testRejectsWrongWireVersionAndNonCanonicalFields() {
        TopicBindingAggregateRecord record = KafkaTopicBindingAggregateMapperV1.toRecord(aggregate());
        assertThrows(
            IllegalArgumentException.class,
            () -> KafkaTopicBindingAggregateMapperV1.fromRecord(record, (short) 1));
        assertThrows(
            IllegalArgumentException.class,
            () -> KafkaTopicBindingAggregateMapperV1.fromRecord(
                record.duplicate().setBindingId(new byte[31]), (short) 0));
        assertThrows(
            IllegalArgumentException.class,
            () -> KafkaTopicBindingAggregateMapperV1.fromRecord(
                record.duplicate().setInitialSealedEnd(new byte[0]), (short) 0));
        assertThrows(
            IllegalArgumentException.class,
            () -> KafkaTopicBindingAggregateMapperV1.fromRecord(
                record.duplicate().setProtocolKind((short) ProtocolKindV1.PULSAR.code()), (short) 0));
    }

    @Test
    void testRejectsWrongTopicBackReference() {
        KafkaTopicBindingAggregateV1 aggregate = aggregate();
        assertThrows(
            IllegalArgumentException.class,
            () -> KafkaTopicBindingAggregateMapperV1.validateBackReference(
                aggregate, new Uuid(19L, 20L), "alpha.topic"));
        assertThrows(
            IllegalArgumentException.class,
            () -> KafkaTopicBindingAggregateMapperV1.validateBackReference(aggregate, TOPIC_ID, "other"));
    }

    private static KafkaTopicBindingAggregateV1 aggregate() {
        KafkaProtocolCellIdentity cell = new KafkaProtocolCellIdentity(
            new DeploymentId(new Id128(1L, 2L)),
            new KafkaCellId(new Id128(3L, 4L)));
        KafkaTopicIncarnationIdentity incarnation = new KafkaTopicIncarnationIdentity(
            new KafkaTopicId(new Id128(
                TOPIC_ID.getMostSignificantBits(), TOPIC_ID.getLeastSignificantBits())),
            new KafkaTopicName("alpha.topic"));
        TopicBindingId bindingId = DeterministicTopicIdsV1.deriveBindingId(cell, incarnation);
        StorageEpochId epochId = DeterministicTopicIdsV1.deriveStorageEpochId(bindingId, 0L);
        TopicBindingAggregateV1 value = new TopicBindingAggregateV1(
            TopicBindingAggregateV1.SCHEMA_VERSION,
            new TopicBindingV1(ProtocolKindV1.KAFKA, bindingId, cell, incarnation),
            new InitialStorageEpochV1(
                epochId,
                0L,
                StorageProfileV1.BOOKKEEPER_WAL_ONLY,
                ProfileOriginV1.TOPIC_EXPLICIT,
                new PolicyCatalogDigest(Sha256Digest.hash(CanonicalBytes.copyOf(
                    "policy-v1".getBytes(StandardCharsets.US_ASCII)))),
                FrameEncodingPolicyCatalogV1.none()));
        return new KafkaTopicBindingAggregateV1(value);
    }
}
