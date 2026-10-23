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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaKafkaBookKeeperM4ReadRouteV2;
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingAuthorityStateV1;
import org.apache.kafka.server.util.Json;
import java.util.Optional;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.retention.M5BindingLogicalTrimCoordinatorV2;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1.WriterDeclarationV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5TaskSelectionCoordinatorV2;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

/** Fixed production mutation owners; declarations neither enumerate references nor qualify an old Binding. */
final class NereusKafkaM5WriterRegistryV1 {
    private NereusKafkaM5WriterRegistryV1() { }

    private static final List<Class<?>> OWNERS = List.of(KafkaBookKeeperPartitionV1.class,
            M4ReadControlCoordinatorV1.class, M5BindingLogicalTrimCoordinatorV2.class,
            M5TaskSelectionCoordinatorV2.class, OxiaPhysicalMemberInventoryV2.class);

    static java.util.Map<String, String> implementationSources(NereusKafkaM4AdmissionV2 admission) throws Exception {
        var result = new java.util.TreeMap<String, String>();
        for (Class<?> owner : OWNERS) result.put(owner.getName(), admission.implementationSourceSha256(owner).toHex());
        return java.util.Map.copyOf(result);
    }

    static M5ClosedWriterRegistryV1 create(CapabilityBinding capability, NereusKafkaM4AdmissionV2 admission) throws Exception {
        var declarations = new ArrayList<WriterDeclarationV1>();
        for (Class<?> owner : OWNERS) {
            Sha256Digest source = admission.implementationSourceSha256(owner);
            declarations.add(new WriterDeclarationV1(owner.getName(), capability,
                    Arrays.stream(FloorClassV1.values()).filter(kind -> floorOwner(kind) == owner).toList(),
                    Arrays.stream(ReferenceKindV1.values()).filter(kind -> referenceOwner(kind) == owner).toList(), source));
        }
        return new M5ClosedWriterRegistryV1(declarations);
    }

    static M5ClosedWriterRegistryV1 enroll(OxiaKafkaBookKeeperM4ReadRouteV2 route,
            NereusKafkaM4AdmissionV2 admission) throws Exception {
        var exact = route.readBindingAuthority().toCompletableFuture().join().orElseThrow();
        var authority = M5BindingAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
        var bootstrap = authority.bootstrap().orElseThrow();
        if (authority.state() != BindingAuthorityStateV1.OPEN_V1 || !authority.referenceMutationTickets().isEmpty()
                || !authority.trimMutationTickets().isEmpty() || bootstrap.kafkaPolicy().isEmpty())
            throw new IllegalStateException("native Kafka writer enrollment requires its quiescent first-policy authority");
        var registry = admittedRegistry(exact, admission);
        if (authority.writerEnrollment().isPresent()) {
            if (!authority.writerEnrollment().orElseThrow().equals(registry.enrollment()))
                throw new IllegalStateException("native Kafka writer enrollment changed");
            return registry;
        }
        var next = M5BindingAuthorityCodecV1.successor(authority, BindingAuthorityStateV1.OPEN_V1,
                authority.selectorProjection(), authority.batchSlots(), Optional.empty(), List.of(), Optional.of(registry.enrollment()));
        var candidate = M5BindingAuthorityCodecV1.encodeAuthority(next);
        route.rawBindingControl().compareAndSet(exact.key(), Optional.of(exact.canonicalStoredBytes()), candidate);
        var actual = route.readBindingAuthority().toCompletableFuture().join().orElseThrow();
        if (!actual.canonicalStoredBytes().equals(candidate))
            throw new IllegalStateException("native Kafka writer enrollment remains unresolved");
        return registry;
    }

    static void requireEnrolled(com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue exact,
            NereusKafkaM4AdmissionV2 admission) throws Exception {
        var authority = M5BindingAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
        if (!authority.writerEnrollment().orElseThrow().equals(admittedRegistry(exact, admission).enrollment()))
            throw new IllegalStateException("native source scan writer enrollment differs from its actual implementation sources");
    }

    private static M5ClosedWriterRegistryV1 admittedRegistry(
            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue exact,
            NereusKafkaM4AdmissionV2 admission) throws Exception {
        var authority = M5BindingAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
        var policy = authority.kafkaPolicy().orElseThrow();
        var first = authority.bootstrap().orElseThrow().kafkaPolicy().orElseThrow();
        if (!policy.sameAdmittedProfile(first))
            throw new IllegalStateException("native source scan policy differs from its original closed admission");
        var admitted = Json.parseBytesAs(policy.admittedAuditPolicy().toByteArray(), java.util.Map.class);
        if (!"NEREUS_M5_KAFKA_CLOSED_ADMISSION_V1".equals(admitted.get("schema"))
                || !implementationSources(admission).equals(admitted.get("writerImplementationSources")))
            throw new IllegalStateException("native Kafka writer sources were not first admitted");
        return create(authority.capability(), admission);
    }

    private static Class<?> floorOwner(FloorClassV1 kind) {
        return switch (kind) {
            case KAFKA_PRODUCER_TRANSACTION, KAFKA_REPLICATION_RECOVERY -> KafkaBookKeeperPartitionV1.class;
            case GENERATION_READ -> M4ReadControlCoordinatorV1.class;
            case LIFECYCLE_TASK -> M5TaskSelectionCoordinatorV2.class;
            case SHARED_PHYSICAL_SOURCE -> OxiaPhysicalMemberInventoryV2.class;
            case BINDING_EPOCH_POLICY, KAFKA_CONSUMER_GROUP, PULSAR_CURSOR_SUBSCRIPTION,
                    PROJECTION_MIGRATION, AUDIT_GRACE -> M5BindingLogicalTrimCoordinatorV2.class;
        };
    }

    private static Class<?> referenceOwner(ReferenceKindV1 kind) {
        return switch (kind) {
            case KAFKA_PRODUCER_RECOVERY, KAFKA_TRANSACTION_OR_ABORTED_RECOVERY,
                    KAFKA_REPLICA_OR_LEADER_EPOCH_RECOVERY, RECOVERY_CHECKPOINT_OR_SNAPSHOT -> KafkaBookKeeperPartitionV1.class;
            case MANIFEST_SELECTED, MANIFEST_FALLBACK, SOURCE_PROTECTION, READ_GENERATION_PIN_OR_OPEN_HANDLE -> M4ReadControlCoordinatorV1.class;
            case MATERIALIZATION_OR_COMPACTION_TASK -> M5TaskSelectionCoordinatorV2.class;
            case SHARED_PHYSICAL_MEMBER -> OxiaPhysicalMemberInventoryV2.class;
            case KAFKA_GROUP_RETENTION, PULSAR_SUBSCRIPTION_OR_REPLICATION_CURSOR,
                    RETIREMENT_OR_DELETE_RECONCILIATION, PROJECTION_MIGRATION_OR_EXPORT, AUDIT_GRACE -> M5BindingLogicalTrimCoordinatorV2.class;
        };
    }
}
