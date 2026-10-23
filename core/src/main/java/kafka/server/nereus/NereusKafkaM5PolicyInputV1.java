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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.object.retention.M5KafkaBindingPolicyV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingAuthorityStateV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaKafkaBookKeeperM4ReadRouteV2;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.config.NereusKafkaBookKeeperConfig;
import org.apache.kafka.server.util.Json;
import org.apache.kafka.storage.internals.log.LogConfig;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import kafka.server.KafkaConfig;
import kafka.log.nereus.NereusUnifiedLog;
import kafka.server.storage.BrokerStorageRuntimeContext;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.FencedLeaderEpochException;
import java.util.function.BooleanSupplier;
import java.util.concurrent.CompletionException;

/** Configured first-admission inputs only; file contents never constitute a reference-free proof. */
final class NereusKafkaM5PolicyInputV1 {
    private static final int MAX_BYTES = 65_536;
    private static final Set<String> KEYS = Set.of("schema", "deployment.id", "kafka.cell.id",
            "provider.scope.sha256", "provider.configuration.sha256", "policy.catalog.sha256",
            "business.audit.domain", "minimum.grace.ms");

    private final NereusKafkaBookKeeperConfig.M5SealedTargetProfile selection;
    private final CanonicalBytes auditPolicy;
    private final long auditMinimumMillis;
    private final boolean gcEnabled;
    private final boolean gcDryRun;

    private NereusKafkaM5PolicyInputV1(NereusKafkaBookKeeperConfig.M5SealedTargetProfile selection,
            CanonicalBytes auditPolicy, long auditMinimumMillis, boolean gcEnabled, boolean gcDryRun) {
        this.selection = selection;
        this.auditPolicy = auditPolicy;
        this.auditMinimumMillis = auditMinimumMillis;
        this.gcEnabled = gcEnabled;
        this.gcDryRun = gcDryRun;
    }

    static Optional<NereusKafkaM5PolicyInputV1> load(KafkaConfig config,
            BookKeeperCapabilitySnapshotV1 capability) throws Exception {
        var storage = config.nereusKafkaStorageConfig();
        var bk = storage.bookKeeper().orElseThrow();
        var selected = bk.ledgerGc().m5SealedTargetProfile();
        if (selected.isEmpty()) return Optional.empty();
        var selection = selected.orElseThrow();
        var bytes = read(selection.auditPolicyFile());
        var properties = new Properties() {
            @Override
            public synchronized Object put(Object key, Object value) {
                if (containsKey(key)) throw new IllegalArgumentException("duplicate M5 audit-policy field: " + key);
                return super.put(key, value);
            }
        };
        properties.load(new InputStreamReader(new ByteArrayInputStream(bytes.toByteArray()), StandardCharsets.UTF_8));
        if (!properties.stringPropertyNames().equals(KEYS)
                || !"NEREUS_M5_KAFKA_AUDIT_POLICY_V1".equals(properties.getProperty("schema"))
                || !"FIRST_ADMISSION_EMPTY_V1".equals(properties.getProperty("business.audit.domain"))) {
            throw new IllegalArgumentException("M5 audit-policy schema or explicit initial domain differs");
        }
        requireIdentity(properties, config, capability);
        String minimum = properties.getProperty("minimum.grace.ms");
        long auditMillis = Long.parseLong(minimum);
        if (auditMillis < 0 || !Long.toString(auditMillis).equals(minimum)
                || auditMillis > selection.grace().toMillis()) {
            throw new IllegalArgumentException("M5 grace does not meet its explicit deployment audit minimum");
        }
        return Optional.of(new NereusKafkaM5PolicyInputV1(selection, bytes, auditMillis,
                bk.ledgerGc().enabled(), bk.ledgerGc().dryRun()));
    }

    private static void requireIdentity(Properties properties, KafkaConfig config,
            BookKeeperCapabilitySnapshotV1 capability) throws Exception {
        var storage = config.nereusKafkaStorageConfig();
        var bk = storage.bookKeeper().orElseThrow();
        if (!Uuid.fromString(properties.getProperty("deployment.id")).equals(Uuid.fromString(bk.deploymentId()))
                || !Uuid.fromString(properties.getProperty("kafka.cell.id"))
                        .equals(Uuid.fromString(storage.core().cluster().orElseThrow()))
                || !capability.providerScopeId().digest().toHex().equals(properties.getProperty("provider.scope.sha256"))
                || !capability.configurationDigest().toHex().equals(properties.getProperty("provider.configuration.sha256"))
                || !NereusBookKeeperCapability.metadataPolicy(config).policyCatalogDigest().digest().toHex()
                        .equals(properties.getProperty("policy.catalog.sha256"))) {
            throw new IllegalArgumentException("M5 audit policy belongs to another deployment, Cell or BK capability");
        }
    }

    private static CanonicalBytes read(Path path) throws Exception {
        try (var input = Files.newInputStream(path)) {
            var bytes = input.readNBytes(MAX_BYTES + 1);
            if (bytes.length == 0 || bytes.length > MAX_BYTES) {
                throw new IllegalArgumentException("M5 audit-policy input is empty or exceeds its byte bound");
            }
            return CanonicalBytes.copyOf(bytes);
        }
    }

    void requireCurrent() throws Exception {
        if (!read(selection.auditPolicyFile()).equals(auditPolicy)) {
            throw new IllegalStateException("Original M5 deployment audit-policy input changed");
        }
    }

    static NereusKafkaNativeInputV2.NativeCut captureNativeCut(BrokerStorageRuntimeContext context,
            NereusUnifiedLog log, com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1 storage,
            com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1 nativeOwner, BooleanSupplier currentAssignment,
            OxiaKafkaBookKeeperM4ReadRouteV2 m4Route, Optional<NereusKafkaM5PolicyInputV1> m5PolicyInput) {
        if (log == null) throw new IllegalStateException("native M5 log is not ready");
        var image = context.metadataCache().getImage();
        var topic = image.topics().getTopic(log.topicPartition().topic());
        if (topic == null || !log.topicId().equals(Optional.of(topic.id())) || !currentAssignment.getAsBoolean())
            throw new IllegalStateException("native M5 topic incarnation or owner changed");
        var props = image.configs().configProperties(new ConfigResource(ConfigResource.Type.TOPIC, topic.name()));
        var defaults = image.configs().configProperties(new ConfigResource(ConfigResource.Type.BROKER, ""));
        var broker = image.configs().configProperties(new ConfigResource(ConfigResource.Type.BROKER,
                Integer.toString(context.config().brokerId())));
        var expected = LogConfig.fromProps(context.config().dynamicConfig().logDefaultsAtMetadataImage(defaults, broker), props);
        var cut = log.captureM5InputCut(nativeOwner.kafkaLeaderEpoch(), storage, expected,
                image.provenance().lastContainedOffset());
        m5PolicyInput.ifPresent(input -> input.requireConfiguration(m4Route, cut.config(),
                image.provenance().lastContainedOffset(), image.features().metadataVersionOrThrow().featureLevel()));
        if (!samePolicyInputs(context, new TopicIdPartition(topic.id(), log.topicPartition()), expected,
                image.features().metadataVersionOrThrow().featureLevel()) || !currentAssignment.getAsBoolean())
            throw new IllegalStateException("native M5 metadata image changed during capture");
        return new NereusKafkaNativeInputV2.NativeCut(cut.inputs(),
                new NereusKafkaNativeInputV2.TopicPolicy(cut.config(), cut.policyMetadataOffset(),
                        image.features().metadataVersionOrThrow().featureLevel()));
    }

    void enrollNativeWriters(OxiaKafkaBookKeeperM4ReadRouteV2 route, NereusKafkaM4AdmissionV2 admission) throws Exception {
        requireCurrent();
        NereusKafkaM5WriterRegistryV1.enroll(route, admission);
    }

    void requireNoPriorOwner(OxiaKafkaBookKeeperM4ReadRouteV2 route, io.oxia.client.api.AsyncOxiaClient oxia,
            PhysicalNamespaceAuthorityBindingV2 namespace, com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2.Scope scope) {
        if (route.readBindingAuthority().toCompletableFuture().join().isEmpty()) {
            String ownerKey = namespace.authorityRoot() + "/kafka-runs-v2/"
                    + Sha256Digest.hash(scope.encode()).toHex() + "/owner-admission-v1";
            if (new com.nereusstream.metadata.oxia.v2.mutation.AsyncOxiaConditionalClient(oxia).read(ownerKey)
                    .toCompletableFuture().join().isPresent())
                throw new IllegalStateException("M5 first policy cannot import an existing native Kafka Owner history");
        }
    }

    M5KafkaBindingPolicyV1 captureFirstPolicy(BrokerStorageRuntimeContext context, TopicIdPartition identity,
            BooleanSupplier current, PhysicalNamespaceAuthorityBindingV2 namespace,
            BookKeeperCapabilitySnapshotV1 capability, long minimum, NereusKafkaM4AdmissionV2 admission) {
        try {
            var image = context.metadataCache().getImage();
            var topic = image.topics().getTopic(identity.topic());
            if (topic == null || !topic.id().equals(identity.topicId()) || !current.getAsBoolean())
                throw new FencedLeaderEpochException("M5 first policy has another native topic incarnation");
            var props = image.configs().configProperties(new ConfigResource(ConfigResource.Type.TOPIC, topic.name()));
            var defaults = image.configs().configProperties(new ConfigResource(ConfigResource.Type.BROKER, ""));
            var broker = image.configs().configProperties(new ConfigResource(ConfigResource.Type.BROKER,
                    Integer.toString(context.config().brokerId())));
            var config = LogConfig.fromProps(context.config().dynamicConfig().logDefaultsAtMetadataImage(defaults, broker), props);
            var admitted = CanonicalBytes.copyOf(Json.encodeAsBytes(new TreeMap<>(java.util.Map.of(
                    "schema", "NEREUS_M5_KAFKA_CLOSED_ADMISSION_V1",
                    "originalAuditInput", new String(auditPolicy.toByteArray(), StandardCharsets.UTF_8),
                    "writerImplementationSources", new TreeMap<>(NereusKafkaM5WriterRegistryV1.implementationSources(admission))))));
            var policy = firstPolicy(namespace, capability, config, image.provenance().lastContainedOffset(),
                    image.features().metadataVersionOrThrow().featureLevel(), minimum, admitted);
            if (!samePolicyInputs(context, identity, config, image.features().metadataVersionOrThrow().featureLevel())
                    || !current.getAsBoolean())
                throw new FencedLeaderEpochException("M5 first policy configuration or owner changed");
            return policy;
        } catch (Exception failure) {
            throw new CompletionException("native M5 first policy was not admitted", failure);
        }
    }

    void writeNativeConfiguration(BrokerStorageRuntimeContext context, NereusUnifiedLog log,
            OxiaKafkaBookKeeperM4ReadRouteV2 route, BooleanSupplier current, long ownerEpoch, LogConfig proposed, long callbackOffset) {
        try {
            requireCurrent();
            var image = context.metadataCache().getImage();
            var topic = image.topics().getTopic(log.topicPartition().topic());
            if (topic == null || !log.topicId().equals(Optional.of(topic.id()))
                    || !current.getAsBoolean())
                throw new FencedLeaderEpochException("native M5 policy owner changed");
            var props = image.configs().configProperties(new ConfigResource(ConfigResource.Type.TOPIC, topic.name()));
            var defaults = image.configs().configProperties(new ConfigResource(ConfigResource.Type.BROKER, ""));
            var broker = image.configs().configProperties(new ConfigResource(ConfigResource.Type.BROKER,
                    Integer.toString(context.config().brokerId())));
            var expected = LogConfig.fromProps(context.config().dynamicConfig().logDefaultsAtMetadataImage(defaults, broker), props);
            if (!proposed.values().equals(expected.values()) || callbackOffset > image.provenance().lastContainedOffset())
                throw new IllegalStateException("native M5 policy update is not in its current KRaft image");
            updatePolicy(route, ownerEpoch, proposed,
                    image.provenance().lastContainedOffset(), image.features().metadataVersionOrThrow().featureLevel());
            if (!samePolicyInputs(context, new TopicIdPartition(topic.id(), log.topicPartition()), proposed,
                    image.features().metadataVersionOrThrow().featureLevel()) || !current.getAsBoolean())
                throw new FencedLeaderEpochException("native M5 policy configuration or owner changed during update");
        } catch (Exception failure) {
            throw new CompletionException("native M5 policy update was not admitted", failure);
        }
    }

    private static boolean samePolicyInputs(BrokerStorageRuntimeContext context, TopicIdPartition identity,
            LogConfig expected, short feature) {
        var current = context.metadataCache().getImage();
        var topic = current.topics().getTopic(identity.topic());
        if (topic == null || !identity.topicId().equals(topic.id())
                || current.features().metadataVersionOrThrow().featureLevel() != feature) return false;
        var props = current.configs().configProperties(new ConfigResource(ConfigResource.Type.TOPIC, topic.name()));
        var defaults = current.configs().configProperties(new ConfigResource(ConfigResource.Type.BROKER, ""));
        var broker = current.configs().configProperties(new ConfigResource(ConfigResource.Type.BROKER,
                Integer.toString(context.config().brokerId())));
        var actual = LogConfig.fromProps(context.config().dynamicConfig().logDefaultsAtMetadataImage(defaults, broker), props);
        return actual.values().equals(expected.values());
    }

    M5KafkaBindingPolicyV1 firstPolicy(PhysicalNamespaceAuthorityBindingV2 namespace,
            BookKeeperCapabilitySnapshotV1 capability, LogConfig config, long metadataOffset,
            short featureLevel, long observedProviderMinimumMillis) throws Exception {
        return firstPolicy(namespace, capability, config, metadataOffset, featureLevel, observedProviderMinimumMillis, auditPolicy);
    }

    private M5KafkaBindingPolicyV1 firstPolicy(PhysicalNamespaceAuthorityBindingV2 namespace,
            BookKeeperCapabilitySnapshotV1 capability, LogConfig config, long metadataOffset,
            short featureLevel, long observedProviderMinimumMillis, CanonicalBytes admitted) throws Exception {
        requireCurrent();
        return new M5KafkaBindingPolicyV1(namespace, 1, metadataOffset, featureLevel,
                canonicalLogConfiguration(config, gcEnabled, gcDryRun), capability.configurationDigest(), admitted,
                observedProviderMinimumMillis, auditMinimumMillis, durationMillis(), List.of());
    }

    static CanonicalBytes canonicalLogConfiguration(LogConfig config, boolean enabled, boolean dryRun) throws Exception {
        return CanonicalBytes.copyOf(Json.encodeAsBytes(new TreeMap<>(java.util.Map.of(
                "nativeLogValues", new TreeMap<>(config.values()), "ledgerGcEnabled", enabled, "ledgerGcDryRun", dryRun))));
    }

    void updatePolicy(OxiaKafkaBookKeeperM4ReadRouteV2 route, long ownerEpoch, LogConfig proposed,
            long metadataOffset, short feature) throws Exception {
        requireCurrent();
        var exact = route.readBindingAuthority().toCompletableFuture().join().orElseThrow();
        var authority = M5BindingAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
        if (authority.selectorProjection().ownerEpoch() != ownerEpoch)
            throw new FencedLeaderEpochException("native M5 policy authority belongs to another Owner");
        if (authority.state() != BindingAuthorityStateV1.OPEN_V1)
            throw new IllegalStateException("native M5 policy writer is fenced by the Binding scan");
        var policy = authority.kafkaPolicy().orElseThrow();
        var bytes = canonicalLogConfiguration(proposed, gcEnabled, gcDryRun);
        if (!policy.nativeLogConfiguration().equals(bytes) || policy.kafkaFeatureLevel() != feature) {
            var next = policy.withConfiguration(metadataOffset, feature, bytes);
            var result = route.logicalTrimCoordinator().updateKafkaPolicy(exact, next).toCompletableFuture().join();
            if (result.exactAuthority().isEmpty())
                throw new IllegalStateException("native M5 policy update remains unresolved: " + result.outcome());
        }
    }

    void requireConfiguration(OxiaKafkaBookKeeperM4ReadRouteV2 route, LogConfig config,
            long imageOffset, short feature) {
        try {
            requireCurrent();
            var exact = route.readBindingAuthority().toCompletableFuture().join().orElseThrow();
            var authority = M5BindingAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
            var policy = authority.kafkaPolicy().orElseThrow();
            if ((authority.state() != BindingAuthorityStateV1.OPEN_V1
                    && authority.state() != BindingAuthorityStateV1.SOURCE_SCAN_FENCED_V2)
                    || policy.configurationMetadataOffset() > imageOffset
                    || policy.kafkaFeatureLevel() != feature
                    || !policy.nativeLogConfiguration().equals(canonicalLogConfiguration(config, gcEnabled, gcDryRun)))
                throw new IllegalStateException("actual native configuration differs from its admitted Binding policy");
        } catch (Exception failure) {
            throw new CompletionException("native M5 policy is not current", failure);
        }
    }

    CanonicalBytes auditPolicy() {
        return auditPolicy;
    }

    Sha256Digest auditPolicySha256() {
        return Sha256Digest.hash(auditPolicy);
    }

    long auditMinimumMillis() {
        return auditMinimumMillis;
    }

    long durationMillis() {
        return selection.grace().toMillis();
    }
}
