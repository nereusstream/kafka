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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.config.NereusKafkaStorageConfig;

import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.objectstore.S3CompatibleObjectStoreProvider;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NereusKafkaRuntimeConfigurationMapperTest {
    private final NereusKafkaRuntimeConfigurationMapper mapper =
            new NereusKafkaRuntimeConfigurationMapper();

    @Test
    void mapsExactObjectWalRuntimeWithoutProviderIo() {
        NereusKafkaStorageConfig config = configuration(
                NereusKafkaStorageConfig.Profile.OBJECT_WAL_SYNC_OBJECT,
                "s3");
        NereusKafkaMappedRuntimeConfiguration mapped =
                map(config);

        assertEquals(
                "kafka-broker-7-epoch-0",
                mapped.runtime().runtime().writerId());
        assertEquals(1, mapped.runtime().runtime().operationOwnerEpoch());
        assertEquals(
                100_000,
                mapped.runtime().runtime().recoveryChunkRecords());
        assertEquals(
                256 * 1024 * 1024,
                mapped.runtime().runtime().recoveryChunkBytes());
        assertEquals(
                java.util.Set.of(StorageProfile.OBJECT_WAL_SYNC_OBJECT),
                mapped.runtime().runtime().executableProfiles());
        assertFalse(mapped.runtime().streamStorage().autoAcquireAppendSession());
        assertEquals(
                NereusKafkaRuntimeConfigurationMapper.MAX_COMMIT_CHAIN_SCAN,
                mapped.runtime().streamStorage().maxCommitChainScan());
        assertEquals(
                mapped.runtime().streamStorage().maxCommitChainScan(),
                mapped.runtime().oxia().maxCommitChainScan());
        assertEquals(
                S3CompatibleObjectStoreProvider.class.getName(),
                mapped.runtime().objectStore().providerClassName());
        assertEquals(
                URI.create("https://s3.us-east-1.amazonaws.com"),
                mapped.runtime().objectStore().endpoint());
        assertEquals(
                NereusKafkaRuntimeConfigurationMapper.DEFAULT_S3_REGION,
                mapped.runtime().objectStore().region());
        assertTrue(mapped.runtime().objectStore().prefix().matches(
                "nereus/kafka/[0-9a-f]{64}"));
        assertEquals("s3", mapped.objectProviderToken());
        assertTrue(mapped.listOffsets().readTargetBytes()
                <= mapped.listOffsets().hardMaxReadBytes());
        assertEquals(
                Path.of("/tmp/nereus-kafka-cache/checkpoint-staging"),
                mapped.maintenance().stagingDirectory());
        assertEquals(
                1024L * 1024 * 1024,
                mapped.maintenance().maxStagingBytes());
        assertEquals(
                config.retentionCompaction().retentionCheckInterval(),
                mapped.maintenance().retentionInterval());
        assertEquals(
                config.lifecycle().executorThreads(),
                mapped.maintenance().maxConcurrentPartitions());
        assertEquals(
                ChecksumType.SHA256,
                mapped.maintenance().contentPolicySha256().type());
        assertEquals(
                "0.1.0-f9-dev",
                mapped.maintenance().writerBuild());
        assertEquals(
                config.retentionCompaction().retentionCheckInterval(),
                mapped.compaction().interval());
        assertEquals(4, mapped.compaction().maxConcurrentPartitions());
        assertEquals(256, mapped.compaction().maxPartitionsPerPass());
        assertEquals(256, mapped.compaction().metadataScanPageSize());
        assertEquals(65_536, mapped.compaction().sourceReadPageRecords());
        assertEquals(64 * 1024 * 1024, mapped.compaction().sourceReadPageBytes());
        assertEquals(
                ReadIsolation.COMMITTED,
                mapped.compaction().sourceReadOptions().isolation());
        assertEquals(
                100_000_000,
                mapped.compaction().executorLimits().maxSourceBatches());
        assertEquals(
                100_000_000,
                mapped.compaction().executorLimits().maxOutputBatches());
        assertEquals(
                8L * 1024 * 1024 * 1024,
                mapped.compaction().executorLimits().maxOutputBytes());
        assertEquals(
                Path.of("/tmp/nereus-kafka-cache/spill"),
                mapped.compaction().stagingDirectory());
        assertEquals(
                100L * 1024 * 1024 * 1024,
                mapped.compaction().maxStagingBytes());
        assertEquals(
                8 * 1024 * 1024,
                mapped.compaction().uploadChunkBytes());
        assertEquals(
                Duration.ofMinutes(15),
                mapped.compaction().partitionPass().claimDuration());
        assertEquals(
                Duration.ofMinutes(5),
                mapped.compaction().partitionPass().claimRenewInterval());
        assertEquals(3, mapped.compaction().partitionPass().maxTaskAttempts());
    }

    @Test
    void freezesCompatibilityAndProviderDigestsIndependentOfProcessIdentity() {
        NereusKafkaStorageConfig config = configuration(
                NereusKafkaStorageConfig.Profile.OBJECT_WAL_SYNC_OBJECT,
                "s3");
        NereusKafkaMappedRuntimeConfiguration first = map(config, "run-a");
        NereusKafkaMappedRuntimeConfiguration second = map(config, "run-b");

        var firstRecord = first.capability().initialRecord(1);
        var secondRecord = second.capability().initialRecord(1);
        assertArrayEquals(
                firstRecord.configCompatibilitySha256(),
                secondRecord.configCompatibilitySha256());
        assertArrayEquals(
                firstRecord.codeCapabilitySha256(),
                secondRecord.codeCapabilitySha256());
        assertArrayEquals(
                firstRecord.providerScopeSha256(),
                secondRecord.providerScopeSha256());
        assertEquals(
                first.maintenance().contentPolicySha256(),
                second.maintenance().contentPolicySha256());
    }

    @Test
    void rejectsProfileWithoutExecutableProviderBeforeResourceCreation() {
        ConfigException failure = assertThrows(
                ConfigException.class,
                () -> map(configuration(
                        NereusKafkaStorageConfig.Profile.OBJECT_WAL_ASYNC_OBJECT,
                        "s3")));

        assertTrue(failure.getMessage().contains("OBJECT_WAL_SYNC_OBJECT"));
    }

    @Test
    void rejectsProviderTokenWithoutReflectionFallback() {
        ConfigException failure = assertThrows(
                ConfigException.class,
                () -> map(configuration(
                        NereusKafkaStorageConfig.Profile.OBJECT_WAL_SYNC_OBJECT,
                        "custom.Provider")));

        assertTrue(failure.getMessage().contains("explicit s3 provider token"));
    }

    private NereusKafkaMappedRuntimeConfiguration map(
            NereusKafkaStorageConfig config
    ) {
        return map(config, "run-a");
    }

    private NereusKafkaMappedRuntimeConfiguration map(
            NereusKafkaStorageConfig config,
            String runtimeInstanceId
    ) {
        return mapper.map(
                config,
                "kafka-cluster-a",
                7,
                0,
                runtimeInstanceId,
                "4.3.0",
                "0.1.0-f9-dev",
                "21");
    }

    private static NereusKafkaStorageConfig configuration(
            NereusKafkaStorageConfig.Profile profile,
            String provider
    ) {
        return new NereusKafkaStorageConfig(
                true,
                new NereusKafkaStorageConfig.Core(
                        Optional.of("nereus-a"),
                        profile,
                        Optional.of("oxia://127.0.0.1:6648"),
                        "default",
                        Optional.of(provider),
                        Optional.of("bucket-a"),
                        Optional.empty(),
                        Optional.empty(),
                        true,
                        Optional.of(URI.create("bk://127.0.0.1/ledgers")),
                        Optional.of(Path.of("/tmp/nereus-kafka-cache"))),
                new NereusKafkaStorageConfig.Append(
                        Duration.ofSeconds(30),
                        8,
                        256,
                        512L * 1024 * 1024,
                        128L * 1024 * 1024,
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(5),
                        2),
                new NereusKafkaStorageConfig.Fetch(
                        Duration.ofSeconds(20),
                        16,
                        512,
                        1024L * 1024 * 1024,
                        64L * 1024 * 1024,
                        128L * 1024 * 1024,
                        1024),
                new NereusKafkaStorageConfig.Lifecycle(
                        4,
                        128,
                        8,
                        Duration.ofMinutes(15),
                        100_000,
                        256L * 1024 * 1024,
                        8L * 1024 * 1024 * 1024,
                        1_000_000,
                        1024L * 1024 * 1024,
                        Duration.ofMinutes(5),
                        3,
                        1024L * 1024 * 1024,
                        Duration.ofSeconds(30),
                        256),
                new NereusKafkaStorageConfig.RetentionCompaction(
                        Duration.ofMinutes(5),
                        true,
                        4,
                        8,
                        8L * 1024 * 1024 * 1024,
                        100_000_000,
                        1024 * 1024,
                        1024L * 1024 * 1024,
                        100,
                        Optional.of(Path.of("/tmp/nereus-kafka-cache/spill")),
                        100L * 1024 * 1024 * 1024),
                new NereusKafkaStorageConfig.Rollout(
                        true,
                        Duration.ofMinutes(5),
                        Duration.ofSeconds(5),
                        Duration.ofSeconds(30),
                        Duration.ofMinutes(2),
                        Duration.ofMinutes(1)));
    }
}
