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

package org.apache.kafka.server.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NereusKafkaStorageConfigTest {
    @Test
    void disabledDefaultsAreSideEffectFreeAndRegisteredInTheBrokerConfigDef() {
        NereusKafkaStorageConfig config = parse(Map.of());

        assertFalse(config.enabled());
        assertEquals(58, NereusKafkaConfigs.CONFIG_DEF.names().size());
        assertTrue(AbstractKafkaConfig.CONFIG_DEF.names().containsAll(NereusKafkaConfigs.CONFIG_DEF.names()));
        assertEquals(
                NereusKafkaStorageConfig.Profile.BOOKKEEPER_WAL_ASYNC_OBJECT,
                config.core().profile());
        assertEquals(Duration.ofSeconds(30), config.append().timeout());
        assertEquals(Duration.ofMinutes(15), config.lifecycle().recoveryTimeout());
        assertTrue(config.core().cacheDir().isEmpty());
        assertTrue(config.retentionCompaction().compactionSpillDir().isEmpty());
    }

    @Test
    void enabledSnapshotRequiresExactProfileDependenciesAndDerivesSpillDirectory() {
        Map<String, Object> properties = enabledProperties();

        NereusKafkaStorageConfig config = parse(properties);

        assertTrue(config.enabled());
        assertEquals("nereus-prod", config.core().cluster().orElseThrow());
        assertTrue(config.core().profile().usesBookKeeper());
        assertTrue(config.core().profile().usesObjectStorage());
        Path cacheDir = Path.of("build/nereus-cache").toAbsolutePath().normalize();
        assertEquals(cacheDir, config.core().cacheDir().orElseThrow());
        assertEquals(
                cacheDir.resolve("spill"),
                config.retentionCompaction().compactionSpillDir().orElseThrow());
    }

    @Test
    void bookKeeperOnlyProfileDoesNotRequireObjectConfiguration() {
        Map<String, Object> properties = enabledProperties();
        properties.put(NereusKafkaConfigs.PROFILE_CONFIG, "BOOKKEEPER_WAL_ONLY");
        properties.remove(NereusKafkaConfigs.OBJECT_PROVIDER_CONFIG);
        properties.remove(NereusKafkaConfigs.OBJECT_BUCKET_CONFIG);

        NereusKafkaStorageConfig config = parse(properties);

        assertFalse(config.core().profile().usesObjectStorage());
    }

    @Test
    void enabledSnapshotRejectsMissingAndInvalidProfileDependencies() {
        Map<String, Object> missingBookKeeper = enabledProperties();
        missingBookKeeper.remove(NereusKafkaConfigs.BOOKKEEPER_METADATA_SERVICE_URI_CONFIG);
        assertConfigFailure(missingBookKeeper, NereusKafkaConfigs.BOOKKEEPER_METADATA_SERVICE_URI_CONFIG);

        Map<String, Object> missingObject = enabledProperties();
        missingObject.remove(NereusKafkaConfigs.OBJECT_BUCKET_CONFIG);
        assertConfigFailure(missingObject, NereusKafkaConfigs.OBJECT_BUCKET_CONFIG);

        Map<String, Object> invalidUri = enabledProperties();
        invalidUri.put(NereusKafkaConfigs.BOOKKEEPER_METADATA_SERVICE_URI_CONFIG, "relative/path");
        assertConfigFailure(invalidUri, NereusKafkaConfigs.BOOKKEEPER_METADATA_SERVICE_URI_CONFIG);
    }

    @Test
    void enabledSnapshotRejectsUnsafeCrossFieldRelationships() {
        Map<String, Object> session = enabledProperties();
        session.put(NereusKafkaConfigs.SESSION_TTL_MS_CONFIG, 10_000L);
        session.put(NereusKafkaConfigs.SESSION_RENEW_INTERVAL_MS_CONFIG, 5_000L);
        assertConfigFailure(session, NereusKafkaConfigs.SESSION_TTL_MS_CONFIG);

        Map<String, Object> appendBudget = enabledProperties();
        appendBudget.put(NereusKafkaConfigs.APPEND_INFLIGHT_BYTES_CONFIG, 64L * 1024L * 1024L);
        appendBudget.put(NereusKafkaConfigs.APPEND_REQUEST_BYTES_CONFIG, 128L * 1024L * 1024L);
        assertConfigFailure(appendBudget, NereusKafkaConfigs.APPEND_INFLIGHT_BYTES_CONFIG);

        Map<String, Object> capability = enabledProperties();
        capability.put(NereusKafkaConfigs.CAPABILITY_HEARTBEAT_MS_CONFIG, 10_000L);
        capability.put(NereusKafkaConfigs.CAPABILITY_EXPIRY_MS_CONFIG, 20_000L);
        assertConfigFailure(capability, NereusKafkaConfigs.CAPABILITY_EXPIRY_MS_CONFIG);

        Map<String, Object> shutdown = enabledProperties();
        shutdown.put(NereusKafkaConfigs.SHUTDOWN_DRAIN_TIMEOUT_MS_CONFIG, 10_000L);
        shutdown.put(NereusKafkaConfigs.SHUTDOWN_CHECKPOINT_TIMEOUT_MS_CONFIG, 20_000L);
        assertConfigFailure(shutdown, NereusKafkaConfigs.SHUTDOWN_CHECKPOINT_TIMEOUT_MS_CONFIG);
    }

    @Test
    void configDefRejectsReservedLegacyProfileAndHardRangeViolations() {
        assertThrows(ConfigException.class, () -> parse(Map.of(
                NereusKafkaConfigs.PROFILE_CONFIG, "OBJECT_WAL")));
        assertThrows(ConfigException.class, () -> parse(Map.of(
                NereusKafkaConfigs.FETCH_MAX_ENTRY_BYTES_CONFIG,
                NereusKafkaConfigs.MAX_KAFKA_ENTRY_BYTES + 1)));
        assertThrows(ConfigException.class, () -> parse(Map.of(
                NereusKafkaConfigs.CHECKPOINT_RETAINED_REFERENCES_CONFIG, 4)));
    }

    private static NereusKafkaStorageConfig parse(Map<String, ?> properties) {
        return NereusKafkaStorageConfig.from(
                new AbstractConfig(NereusKafkaConfigs.CONFIG_DEF, properties));
    }

    private static Map<String, Object> enabledProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(NereusKafkaConfigs.ENABLED_CONFIG, true);
        properties.put(NereusKafkaConfigs.CLUSTER_CONFIG, "nereus-prod");
        properties.put(NereusKafkaConfigs.OXIA_SERVICE_ADDRESS_CONFIG, "oxia://127.0.0.1:6648");
        properties.put(NereusKafkaConfigs.CACHE_DIR_CONFIG, "build/nereus-cache");
        properties.put(NereusKafkaConfigs.BOOKKEEPER_METADATA_SERVICE_URI_CONFIG, "bk://127.0.0.1/ledgers");
        properties.put(NereusKafkaConfigs.OBJECT_PROVIDER_CONFIG, "s3");
        properties.put(NereusKafkaConfigs.OBJECT_BUCKET_CONFIG, "nereus-kafka");
        return properties;
    }

    private static void assertConfigFailure(Map<String, ?> properties, String key) {
        ConfigException failure = assertThrows(ConfigException.class, () -> parse(properties));
        assertTrue(failure.getMessage().contains(key), failure::getMessage);
    }
}
