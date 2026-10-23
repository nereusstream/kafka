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

import org.apache.kafka.common.test.KafkaClusterTestKit;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Explicit actual deployment inputs and persisted-policy checks for the existing native Broker fixture. */
final class KafkaBookKeeperM5PolicyRealScenario {
    private KafkaBookKeeperM5PolicyRealScenario() { }

    static boolean interceptPut(String method, boolean selector, Object[] args) {
        return method.equals("put") && !(selector && com.nereusstream.storage.object.retention.M5BindingBootstrapV1.isBootstrap(
                com.nereusstream.domain.bytes.CanonicalBytes.copyOf((byte[]) args[1])));
    }

    static void configure(KafkaClusterTestKit.Builder builder, Path inputs, Map<String, Object> nativeConfig,
            boolean sealedGcProfile, String mode) throws Exception {
        nativeConfig.forEach(builder::setConfigProp);
        builder.setConfigProp("nereus.kafka.storage.bookkeeper.max.reads.inflight", "3");
        builder.setConfigProp("nereus.kafka.storage.bookkeeper.max.entries.per.ledger", "8");
        if (sealedGcProfile) {
            var descriptor = NereusBookKeeperCapability.descriptor(inputs.resolve("capability.properties"));
            var audit = inputs.resolve("m5-audit-policy.properties");
            Files.writeString(audit, "schema=NEREUS_M5_KAFKA_AUDIT_POLICY_V1\n"
                    + "deployment.id=" + nativeConfig.get("nereus.kafka.storage.bookkeeper.deployment.id") + "\n"
                    + "kafka.cell.id=" + nativeConfig.get("nereus.kafka.storage.cluster") + "\n"
                    + "provider.scope.sha256=" + descriptor.getProperty("providerScopeSha256") + "\n"
                    + "provider.configuration.sha256=" + descriptor.getProperty("configurationDigest") + "\n"
                    + "policy.catalog.sha256=" + descriptor.getProperty("policyCatalogDigest") + "\n"
                    + "business.audit.domain=FIRST_ADMISSION_EMPTY_V1\nminimum.grace.ms=10\n");
            builder.setConfigProp("nereus.kafka.storage.bookkeeper.gc.enabled", Boolean.toString(!mode.endsWith("_DISABLED")));
            builder.setConfigProp("nereus.kafka.storage.bookkeeper.gc.dry.run", Boolean.toString(mode.endsWith("_DRY_RUN") || mode.endsWith("_DISABLED")));
            builder.setConfigProp("nereus.kafka.storage.bookkeeper.m5.gc.profile", "M5_SEALED_TARGET_CONFIRMED_DRAIN_V1");
            builder.setConfigProp("nereus.kafka.storage.bookkeeper.m5.gc.grace.ms", "25");
            builder.setConfigProp("nereus.kafka.storage.bookkeeper.m5.gc.audit.policy.file", audit.toString());
        }
    }

    static void recoveryFailure(org.apache.kafka.common.TopicIdPartition partition, Throwable failure, int broker,
            java.util.concurrent.CompletableFuture<NereusKafkaOwnedProviderRuntime.Opened> zero,
            java.util.concurrent.CompletableFuture<NereusKafkaOwnedProviderRuntime.Opened> one) {
        if (partition.topic().equals("native-cold") && failure != null)
            (broker == 0 ? zero : one).completeExceptionally(failure);
    }

    static void withConfigScanDrain(NereusKafkaOwnedProviderRuntime.Opened opened,
            org.apache.kafka.clients.admin.Admin admin) throws Exception {
        var authority = com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1.decodeAuthority(
                opened.nativeM5BindingRoute().readBindingAuthority().toCompletableFuture().get(30, TimeUnit.SECONDS)
                        .orElseThrow().canonicalStoredBytes());
        if (authority.kafkaPolicy().isEmpty()) {
            changeConfiguration(admin);
            return;
        }
        try (var scan = opened.openNativeM5SourceScan().toCompletableFuture().get(30, TimeUnit.SECONDS)) {
            var entered = new java.util.concurrent.CompletableFuture<Void>();
            var release = new java.util.concurrent.CompletableFuture<Void>();
            var pending = scan.observe(() -> {
                opened.nativeM5InputCut();
                entered.complete(null);
                release.join();
                // This acquisition would deadlock if the configuration callback held the log guard.
                return opened.nativeM5InputCut();
            }).toCompletableFuture();
            try {
                entered.get(30, TimeUnit.SECONDS);
                changeConfiguration(admin);
                org.apache.kafka.test.TestUtils.waitForCondition(() -> {
                    try {
                        scan.requireCurrent().toCompletableFuture().join();
                        return false;
                    } catch (RuntimeException closed) {
                        return true;
                    }
                }, 30_000, "actual native policy writer did not close its source scan");
                assertFalse(pending.isDone(), "configuration passed an accepted source observation");
                release.complete(null);
                assertThrows(java.util.concurrent.ExecutionException.class, () -> pending.get(30, TimeUnit.SECONDS));
                scan.closeAsync().get(30, TimeUnit.SECONDS);
            } finally {
                release.complete(null);
            }
        }
    }

    private static void changeConfiguration(org.apache.kafka.clients.admin.Admin admin) throws Exception {
        var resource = new org.apache.kafka.common.config.ConfigResource(
                org.apache.kafka.common.config.ConfigResource.Type.TOPIC, "native-cold");
        admin.incrementalAlterConfigs(Map.of(resource, java.util.List.of(
                new org.apache.kafka.clients.admin.AlterConfigOp(new org.apache.kafka.clients.admin.ConfigEntry(
                        "delete.retention.ms", "7777"), org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET),
                new org.apache.kafka.clients.admin.AlterConfigOp(new org.apache.kafka.clients.admin.ConfigEntry(
                        "compression.type", "producer"), org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET))))
                .all().get(30, TimeUnit.SECONDS);
    }

    static void verifyInitialPolicy(KafkaClusterTestKit cluster, NereusKafkaOwnedProviderRuntime.Opened opened,
            NereusKafkaNativeInputV2.Captured captured) throws Exception {
        if (Files.exists(Path.of(cluster.brokers().get(0).config().getString(
                org.apache.kafka.server.config.NereusKafkaConfigs.BOOKKEEPER_CAPABILITY_FILE_CONFIG)).resolveSibling("m5-audit-policy.properties"))) {
            var exact = opened.nativeM5BindingRoute().readBindingAuthority().toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow();
            var authority = com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
            var admitted = authority.kafkaPolicy().orElseThrow();
            assertTrue(authority.bootstrap().orElseThrow().kafkaPolicy().isPresent());
            assertTrue(authority.writerEnrollment().isPresent(), "actual native writer inventory was not persisted");
            assertEquals(10, authority.writerEnrollment().orElseThrow().floorClasses().size());
            assertEquals(15, authority.writerEnrollment().orElseThrow().referenceKinds().size());
            var admittedInput = org.apache.kafka.server.util.Json.parseBytesAs(admitted.admittedAuditPolicy().toByteArray(), Map.class);
            assertEquals("NEREUS_M5_KAFKA_CLOSED_ADMISSION_V1", admittedInput.get("schema"));
            assertEquals(5, ((Map<?, ?>) admittedInput.get("writerImplementationSources")).size());
            assertEquals(NereusKafkaM5PolicyInputV1.canonicalLogConfiguration(captured.topicPolicy().config(), true, false), admitted.nativeLogConfiguration());
            assertEquals(25, admitted.graceDurationMillis());
            assertEquals(10, admitted.auditMinimumMillis());
            assertEquals(0, admitted.providerMinimumMillis());
            try (var scan = opened.openNativeM5SourceScan().toCompletableFuture().get(30, TimeUnit.SECONDS)) {
                scan.requireCurrent().toCompletableFuture().get(30, TimeUnit.SECONDS);
                var frozen = com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1
                        .decodeAuthority(scan.exactAuthority().canonicalStoredBytes());
                assertTrue(frozen.sourceScan().isPresent());
                assertEquals(admitted, frozen.kafkaPolicy().orElseThrow());
                opened.nativeM5InputCut();
                var observed = new java.util.concurrent.CompletableFuture<Void>();
                var release = new java.util.concurrent.CompletableFuture<Void>();
                var pending = scan.observe(() -> {
                    var actual = opened.nativeM5InputCut();
                    observed.complete(null);
                    release.join();
                    return actual;
                }).toCompletableFuture();
                try {
                    observed.get(30, TimeUnit.SECONDS);
                    var closing = scan.closeAsync();
                    assertFalse(closing.isDone(), "scan cancellation passed its original accepted observation");
                    release.complete(null);
                    assertThrows(java.util.concurrent.ExecutionException.class, () -> pending.get(30, TimeUnit.SECONDS));
                    closing.get(30, TimeUnit.SECONDS);
                } finally {
                    release.complete(null);
                }
            }
            var reopened = com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1.decodeAuthority(
                    opened.nativeM5BindingRoute().readBindingAuthority().toCompletableFuture().get(30, TimeUnit.SECONDS)
                            .orElseThrow().canonicalStoredBytes());
            assertTrue(reopened.sourceScan().isEmpty());
            assertEquals(admitted, reopened.kafkaPolicy().orElseThrow());
        }
    }
}
