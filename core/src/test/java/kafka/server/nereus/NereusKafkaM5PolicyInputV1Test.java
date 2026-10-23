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

import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.bytes.CanonicalUtf8;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.server.config.NereusKafkaBookKeeperConfig;
import org.apache.kafka.server.config.NereusKafkaConfigs;
import org.apache.kafka.server.config.NereusKafkaStorageConfig;
import org.apache.kafka.storage.internals.log.LogConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Optional;

import kafka.server.KafkaConfig;
import kafka.server.nsip.KafkaBookKeeperOwnerRelocationRealTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NereusKafkaM5PolicyInputV1Test {
    @Test
    void firstPolicyBindsNativeConfigurationProvenanceSwitchesAndApplicableMinima(@TempDir Path directory) throws Exception {
        var config = config(directory, true);
        var capability = NereusBookKeeperCapability.loadAndValidate(config);
        Files.writeString(directory.resolve("audit.properties"), policy(config, capability));
        var input = NereusKafkaM5PolicyInputV1.load(config, capability).orElseThrow();
        var namespace = new PhysicalNamespaceAuthorityBindingV2(new PhysicalResourceIdV2.Namespace(
                PhysicalResourceIdV2.ProviderKind.BOOKKEEPER, CanonicalUtf8.fromString("bk-service"),
                CanonicalUtf8.fromString("bk-instance")), new MetadataNamespaceIdentityV2(new Id128(1, 2), 3));
        var log = new LogConfig(java.util.Map.of("retention.ms", "1234", "cleanup.policy", "delete"));
        var first = input.firstPolicy(namespace, capability, log, 17, (short) 1, 12);
        assertEquals(17, first.configurationMetadataOffset());
        assertEquals(1, first.kafkaFeatureLevel());
        assertEquals(12, first.providerMinimumMillis());
        assertEquals(10, first.auditMinimumMillis());
        assertEquals(NereusKafkaM5PolicyInputV1.canonicalLogConfiguration(log, false, true), first.nativeLogConfiguration());
        var admitted = org.apache.kafka.server.util.Json.parseBytesAs(first.nativeLogConfiguration().toByteArray(), java.util.Map.class);
        assertEquals(false, admitted.get("ledgerGcEnabled"));
        assertEquals(true, admitted.get("ledgerGcDryRun"));
        assertEquals(input.auditPolicy(), first.admittedAuditPolicy());
        assertThrows(IllegalArgumentException.class, () -> input.firstPolicy(namespace, capability, log, 17, (short) 1, 26));
    }

    @Test
    void actualConfiguredPolicyInputRetainsBytesAndRejectsFileDrift(@TempDir Path directory) throws Exception {
        var config = config(directory, false);
        var capability = NereusBookKeeperCapability.loadAndValidate(config);
        assertTrue(NereusKafkaM5PolicyInputV1.load(config, capability).isEmpty());

        config = config(directory, true);
        Path policy = directory.resolve("audit.properties");
        String body = policy(config, capability);
        Files.writeString(policy, body);
        var input = NereusKafkaM5PolicyInputV1.load(config, capability).orElseThrow();
        assertEquals(25, input.durationMillis());
        assertEquals(10, input.auditMinimumMillis());
        assertEquals(body, new String(input.auditPolicy().toByteArray(), java.nio.charset.StandardCharsets.UTF_8));
        input.requireCurrent();
        Files.writeString(policy, body.replace("minimum.grace.ms=10", "minimum.grace.ms=11"));
        assertThrows(IllegalStateException.class, input::requireCurrent);
        // Loading a new requested input is distinct from retaining the original admitted bytes.
        assertEquals(11, NereusKafkaM5PolicyInputV1.load(config, capability).orElseThrow().auditMinimumMillis());
    }

    @Test
    void missingForeignAmbiguousOrInsufficientAuditInputCannotBeSelected(@TempDir Path directory) throws Exception {
        var config = config(directory, true);
        var capability = NereusBookKeeperCapability.loadAndValidate(config);
        Path policy = directory.resolve("audit.properties");
        assertThrows(java.nio.file.NoSuchFileException.class, () -> NereusKafkaM5PolicyInputV1.load(config, capability));
        String body = policy(config, capability);
        String cell = config.nereusKafkaStorageConfig().core().cluster().orElseThrow();
        for (String invalid : java.util.List.of(
                body + "minimum.grace.ms=0\n",
                body.replace("kafka.cell.id=" + cell, "kafka.cell.id=" + org.apache.kafka.common.Uuid.randomUuid()),
                body.replace("minimum.grace.ms=10", "minimum.grace.ms=26"),
                body.replace("FIRST_ADMISSION_EMPTY_V1", "UNKNOWN"),
                body + "unregistered.hold=present\n")) {
            Files.writeString(policy, invalid);
            assertThrows(IllegalArgumentException.class, () -> NereusKafkaM5PolicyInputV1.load(config, capability));
        }
    }

    private static KafkaConfig config(Path directory, boolean selected) throws Exception {
        KafkaBookKeeperOwnerRelocationRealTest.writeCapability(directory, Optional.empty());
        var properties = new HashMap<>(KafkaBookKeeperOwnerRelocationRealTest.nativeConfig(
                directory, "zk://127.0.0.1:2181/ledgers", "127.0.0.1:6648"));
        if (selected) {
            properties.put(NereusKafkaConfigs.BOOKKEEPER_M5_GC_PROFILE_CONFIG,
                    NereusKafkaBookKeeperConfig.M5SealedTargetProfile.NAME);
            properties.put(NereusKafkaConfigs.BOOKKEEPER_M5_GC_GRACE_MS_CONFIG, 25L);
            properties.put(NereusKafkaConfigs.BOOKKEEPER_M5_GC_AUDIT_POLICY_FILE_CONFIG,
                    directory.resolve("audit.properties").toString());
        }
        var snapshot = NereusKafkaStorageConfig.from(new AbstractConfig(NereusKafkaConfigs.CONFIG_DEF, properties));
        var config = mock(KafkaConfig.class);
        when(config.nereusKafkaStorageConfig()).thenReturn(snapshot);
        when(config.getString(NereusKafkaConfigs.BOOKKEEPER_CAPABILITY_FILE_CONFIG))
                .thenReturn(directory.resolve("capability.properties").toString());
        return config;
    }

    private static String policy(KafkaConfig config, BookKeeperCapabilitySnapshotV1 capability) throws Exception {
        var storage = config.nereusKafkaStorageConfig();
        return "schema=NEREUS_M5_KAFKA_AUDIT_POLICY_V1\n"
                + "deployment.id=" + storage.bookKeeper().orElseThrow().deploymentId() + "\n"
                + "kafka.cell.id=" + storage.core().cluster().orElseThrow() + "\n"
                + "provider.scope.sha256=" + capability.providerScopeId().digest().toHex() + "\n"
                + "provider.configuration.sha256=" + capability.configurationDigest().toHex() + "\n"
                + "policy.catalog.sha256=" + NereusBookKeeperCapability.metadataPolicy(config)
                        .policyCatalogDigest().digest().toHex() + "\n"
                + "business.audit.domain=FIRST_ADMISSION_EMPTY_V1\n"
                + "minimum.grace.ms=10\n";
    }
}
