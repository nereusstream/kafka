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

import kafka.log.nereus.NereusListOffsetsScanConfig;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.config.NereusKafkaConfigs;
import org.apache.kafka.server.config.NereusKafkaStorageConfig;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.kafka.activation.KafkaBrokerCapabilitySpecification;
import com.nereusstream.kafka.activation.KafkaStorageActivationPolicy;
import com.nereusstream.kafka.compaction.KafkaCompactionPartitionPass;
import com.nereusstream.kafka.compaction.KafkaCompactionTwoPassExecutor;
import com.nereusstream.kafka.runtime.NereusKafkaCompactionRuntimeConfiguration;
import com.nereusstream.kafka.runtime.NereusKafkaMaintenanceConfiguration;
import com.nereusstream.kafka.runtime.NereusKafkaObjectWalRuntimeConfiguration;
import com.nereusstream.kafka.runtime.NereusKafkaRuntimeConfiguration;
import com.nereusstream.metadata.oxia.KafkaBrokerIdentity;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import com.nereusstream.objectstore.ObjectPutRetryPolicy;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import com.nereusstream.objectstore.S3CompatibleObjectStoreProvider;
import com.nereusstream.objectstore.staging.StagingFileManager;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Closed, deterministic mapping from the stock-owned 58-key snapshot to the first executable Nereus provider graph.
 *
 * <p>The mapper performs no provider, filesystem, network or scheduler I/O. Unsupported profiles/providers fail before
 * an owned resource is created.
 */
public final class NereusKafkaRuntimeConfigurationMapper {
    public static final String S3_PROVIDER_TOKEN = "s3";
    public static final String DEFAULT_S3_REGION = "us-east-1";
    public static final int MAX_COMMIT_CHAIN_SCAN = 10_000;
    private static final int MAX_DERIVED_INDEX_REPAIR_COMMITS = 256;
    private static final int MIN_PENDING_OPERATIONS = 1_024;
    private static final Duration MAXIMUM_CLOCK_SKEW = Duration.ofSeconds(5);
    private static final Duration APPEND_RECOVERY_BACKOFF_MIN = Duration.ofMillis(100);
    private static final Duration APPEND_RECOVERY_BACKOFF_MAX = Duration.ofSeconds(5);
    private static final String CONFIG_DIGEST_DOMAIN = "nereus-kafka-config-compatibility-v1";
    private static final String PROVIDER_DIGEST_DOMAIN = "nereus-kafka-provider-scope-v1";
    private static final String CODE_DIGEST_DOMAIN = "nereus-kafka-code-capability-v1";

    public NereusKafkaMappedRuntimeConfiguration map(
            NereusKafkaStorageConfig storage,
            String kafkaClusterId,
            int brokerId,
            long brokerEpoch,
            String runtimeInstanceId,
            String kafkaVersion,
            String nereusBuild,
            String javaVersion
    ) {
        NereusKafkaStorageConfig exact = Objects.requireNonNull(storage, "storage");
        requireExecutableStorage(exact);
        String cluster = required(exact.core().cluster(), NereusKafkaConfigs.CLUSTER_CONFIG);
        String oxiaAddress = required(
                exact.core().oxiaServiceAddress(),
                NereusKafkaConfigs.OXIA_SERVICE_ADDRESS_CONFIG);
        String providerToken = canonicalProvider(required(
                exact.core().objectProvider(),
                NereusKafkaConfigs.OBJECT_PROVIDER_CONFIG));
        String bucket = required(
                exact.core().objectBucket(),
                NereusKafkaConfigs.OBJECT_BUCKET_CONFIG);
        String exactKafkaClusterId = nonblank(kafkaClusterId, "kafkaClusterId");
        String exactRuntimeInstanceId = nonblank(runtimeInstanceId, "runtimeInstanceId");
        if (brokerId < 0 || brokerEpoch < 0) {
            throw new IllegalArgumentException("brokerId and brokerEpoch must be non-negative");
        }

        String region = exact.core().objectRegion().orElse(DEFAULT_S3_REGION);
        URI endpoint = exact.core().objectEndpoint().orElseGet(() -> defaultS3Endpoint(region));
        String prefix = "nereus/kafka/" + HexFormat.of().formatHex(
                sha256(exactKafkaClusterId.getBytes(StandardCharsets.UTF_8)));
        Duration providerTimeout = minimum(exact.append().timeout(), exact.fetch().timeout());
        int maxConnections = addExact(
                exact.append().executorThreads(),
                exact.fetch().executorThreads(),
                exact.lifecycle().executorThreads());
        ObjectStoreConfiguration objectStore = new ObjectStoreConfiguration(
                S3CompatibleObjectStoreProvider.class.getName(),
                endpoint,
                region,
                bucket,
                prefix,
                exact.core().objectPathStyleAccess(),
                providerTimeout,
                ObjectPutRetryPolicy.defaults(),
                maxConnections,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        OxiaClientConfiguration oxia =
                oxiaConfiguration(exact, oxiaAddress, providerTimeout);

        long operationEpoch = Math.addExact(brokerEpoch, 1);
        String brokerIdentity = "kafka-broker-" + brokerId + "-epoch-" + brokerEpoch;
        Duration operationTtl = maximum(
                exact.lifecycle().recoveryTimeout(),
                exact.append().sessionTtl());
        Set<StorageProfile> executableProfiles = Set.of(StorageProfile.OBJECT_WAL_SYNC_OBJECT);
        NereusKafkaRuntimeConfiguration runtime = new NereusKafkaRuntimeConfiguration(
                cluster,
                exactKafkaClusterId,
                brokerIdentity,
                exact.append().sessionTtl(),
                exact.append().sessionRenewInterval(),
                brokerIdentity,
                operationEpoch,
                operationTtl,
                exact.lifecycle().recoveryChunkRecords(),
                Math.toIntExact(exact.lifecycle().recoveryChunkBytes()),
                executableProfiles);

        int maxInFlightAppends = addExact(
                exact.append().executorThreads(),
                exact.append().executorQueueCapacity());
        int maxAppendRecoveryTerminals = multiplyExact(maxInFlightAppends, 2);
        int maxObjectBytes = Math.toIntExact(Math.min(
                exact.append().requestBytes(),
                exact.fetch().maxEntryBytes()));
        Duration appendRecoveryTerminalTtl = multiplyExact(
                maximum(exact.append().sessionTtl(), exact.append().timeout()), 2);
        StreamStorageConfig streamStorage = new StreamStorageConfig(
                cluster,
                brokerIdentity,
                exact.append().sessionTtl(),
                exact.append().sessionRenewInterval(),
                exact.append().sessionRenewInterval(),
                exact.append().timeout(),
                exact.fetch().timeout(),
                exact.rollout().shutdownDrainTimeout(),
                exact.fetch().operationMaxRereads(),
                MAX_COMMIT_CHAIN_SCAN,
                Math.max(MAX_DERIVED_INDEX_REPAIR_COMMITS, exact.lifecycle().registryScanPageSize()),
                exact.lifecycle().executorQueueCapacity(),
                maxInFlightAppends,
                exact.append().inflightBytes(),
                exact.fetch().executorThreads(),
                exact.fetch().inflightBytes(),
                maxObjectBytes,
                exact.lifecycle().recoveryChunkRecords(),
                exact.lifecycle().checkpointInterval(),
                false,
                true,
                true,
                exactRuntimeInstanceId,
                exact.append().timeout(),
                APPEND_RECOVERY_BACKOFF_MIN,
                minimum(APPEND_RECOVERY_BACKOFF_MAX, exact.append().timeout()),
                appendRecoveryTerminalTtl,
                maxInFlightAppends,
                maxAppendRecoveryTerminals);

        Duration pendingProtection = maximum(
                exact.lifecycle().recoveryTimeout(),
                exact.rollout().shutdownDrainTimeout());
        Duration orphanGrace = multiplyExact(pendingProtection, 2);
        NereusKafkaObjectWalRuntimeConfiguration objectWal =
                new NereusKafkaObjectWalRuntimeConfiguration(
                        runtime,
                        streamStorage,
                        oxia,
                        objectStore,
                        pendingProtection,
                        MAXIMUM_CLOCK_SKEW,
                        orphanGrace,
                        exact.lifecycle().executorThreads());

        byte[] configurationDigest = configurationCompatibilitySha256(exact);
        byte[] providerDigest = providerScopeSha256(
                exact, exactKafkaClusterId, providerToken, endpoint, region, prefix);
        KafkaBrokerCapabilitySpecification capability =
                new KafkaBrokerCapabilitySpecification(
                        exactKafkaClusterId,
                        new KafkaBrokerIdentity(brokerId, brokerEpoch),
                        exactRuntimeInstanceId,
                        nonblank(kafkaVersion, "kafkaVersion"),
                        nonblank(nereusBuild, "nereusBuild"),
                        nonblank(javaVersion, "javaVersion"),
                        executableProfiles,
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                        configurationDigest,
                        codeCapabilitySha256(),
                        providerDigest,
                        exact.rollout().capabilityHeartbeat(),
                        exact.rollout().capabilityExpiry());

        return mapped(
                exact,
                objectWal,
                capability,
                configurationDigest,
                providerTimeout,
                pendingProtection, orphanGrace,
                nereusBuild,
                providerToken);
    }

    /**
     * Maps the controller's minimal activation graph without requiring a broker epoch or constructing provider resources.
     */
    public NereusKafkaControllerRuntimeConfiguration mapController(
            NereusKafkaStorageConfig storage,
            String kafkaClusterId
    ) {
        NereusKafkaStorageConfig exact =
                Objects.requireNonNull(storage, "storage");
        requireExecutableStorage(exact);
        String nereusCluster =
                required(exact.core().cluster(), NereusKafkaConfigs.CLUSTER_CONFIG);
        String oxiaAddress = required(
                exact.core().oxiaServiceAddress(),
                NereusKafkaConfigs.OXIA_SERVICE_ADDRESS_CONFIG);
        canonicalProvider(required(
                exact.core().objectProvider(),
                NereusKafkaConfigs.OBJECT_PROVIDER_CONFIG));
        required(
                exact.core().objectBucket(),
                NereusKafkaConfigs.OBJECT_BUCKET_CONFIG);
        String exactKafkaClusterId =
                nonblank(kafkaClusterId, "kafkaClusterId");
        Duration providerTimeout =
                minimum(exact.append().timeout(), exact.fetch().timeout());
        Duration retryInterval = minimum(
                exact.rollout().capabilityHeartbeat(),
                Duration.ofSeconds(1));
        return new NereusKafkaControllerRuntimeConfiguration(
                nereusCluster,
                exactKafkaClusterId,
                oxiaConfiguration(exact, oxiaAddress, providerTimeout),
                new KafkaStorageActivationPolicy(
                        exactKafkaClusterId,
                        Set.of(StorageProfile.OBJECT_WAL_SYNC_OBJECT),
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                        exact.rollout().capabilityExpiry()),
                retryInterval);
    }

    /** Maps request-scan limits without requiring broker identity or constructing provider resources. */
    public NereusListOffsetsScanConfig listOffsets(NereusKafkaStorageConfig storage) {
        NereusKafkaStorageConfig exact = Objects.requireNonNull(storage, "storage");
        long exactMaxObjectBytes = Math.min(
                exact.append().requestBytes(),
                exact.fetch().maxEntryBytes());
        int maxObjectBytes = Math.toIntExact(exactMaxObjectBytes);
        return new NereusListOffsetsScanConfig(
                exact.lifecycle().recoveryChunkRecords(),
                exact.lifecycle().recoveryChunkBytes(),
                Math.toIntExact(Math.min(exactMaxObjectBytes, 1024L * 1024L)),
                maxObjectBytes,
                exact.fetch().operationMaxRereads(),
                exact.fetch().timeout());
    }

    private NereusKafkaMappedRuntimeConfiguration mapped(
            NereusKafkaStorageConfig storage,
            NereusKafkaObjectWalRuntimeConfiguration objectWal,
            KafkaBrokerCapabilitySpecification capability,
            byte[] configurationDigest,
            Duration providerTimeout,
            Duration pendingProtection,
            Duration orphanGrace,
            String nereusBuild,
            String providerToken
    ) {
        return new NereusKafkaMappedRuntimeConfiguration(
                objectWal,
                capability,
                listOffsets(storage),
                maintenance(
                        storage,
                        configurationDigest,
                        providerTimeout,
                        pendingProtection,
                        orphanGrace,
                        nereusBuild),
                compaction(
                        storage,
                        providerTimeout,
                        pendingProtection,
                        orphanGrace),
                providerToken);
    }

    private static NereusKafkaMaintenanceConfiguration maintenance(
            NereusKafkaStorageConfig storage,
            byte[] configurationDigest,
            Duration providerTimeout,
            Duration pendingProtection,
            Duration orphanGrace,
            String nereusBuild
    ) {
        Path checkpointStagingDirectory = storage.core().cacheDir()
                .orElseThrow()
                .resolve("checkpoint-staging")
                .normalize();
        int checkpointUploadChunkBytes = Math.toIntExact(Math.min(
                StagingFileManager.MAX_UPLOAD_CHUNK_BYTES,
                storage.lifecycle().checkpointMaxBytes()));
        return new NereusKafkaMaintenanceConfiguration(
                checkpointStagingDirectory,
                storage.lifecycle().checkpointMaxBytes(),
                checkpointUploadChunkBytes,
                orphanGrace,
                providerTimeout,
                storage.lifecycle().recoveryTimeout(),
                storage.append().timeout(),
                pendingProtection,
                storage.retentionCompaction().retentionCheckInterval(),
                storage.lifecycle().executorThreads(),
                Math.max(
                        storage.lifecycle().executorThreads(),
                        storage.lifecycle().registryScanPageSize()),
                new Checksum(
                        ChecksumType.SHA256,
                        HexFormat.of().formatHex(configurationDigest)),
                nonblank(nereusBuild, "nereusBuild"));
    }

    private static NereusKafkaCompactionRuntimeConfiguration compaction(
            NereusKafkaStorageConfig storage,
            Duration providerTimeout,
            Duration pendingProtection,
            Duration orphanGrace
    ) {
        NereusKafkaStorageConfig.RetentionCompaction configured =
                storage.retentionCompaction();
        int concurrentPartitions = Math.min(
                configured.compactionWorkerThreads(),
                configured.compactionMaxConcurrentTasks());
        int maximumPartitions = Math.max(
                concurrentPartitions,
                storage.lifecycle().registryScanPageSize());
        int sourcePageRecords = Math.min(
                65_536,
                storage.lifecycle().recoveryChunkRecords());
        int sourcePageBytes = Math.toIntExact(Math.min(
                64L * 1024 * 1024,
                storage.lifecycle().recoveryChunkBytes()));
        int uploadChunkBytes = Math.max(
                StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                Math.min(StagingFileManager.MAX_UPLOAD_CHUNK_BYTES, sourcePageBytes));
        long maximumOutputBatches = Math.min(
                Integer.MAX_VALUE,
                configured.compactionTaskMaxRecords());
        Duration claimRenewal = dividePositive(pendingProtection, 3);
        return new NereusKafkaCompactionRuntimeConfiguration(
                configured.retentionCheckInterval(),
                concurrentPartitions,
                maximumPartitions,
                storage.lifecycle().registryScanPageSize(),
                new ReadOptions(
                        sourcePageRecords,
                        sourcePageBytes,
                        ReadIsolation.COMMITTED,
                        providerTimeout),
                sourcePageRecords,
                sourcePageBytes,
                new KafkaCompactionTwoPassExecutor.Limits(
                        configured.compactionTaskMaxRecords(),
                        Math.toIntExact(maximumOutputBatches),
                        configured.compactionTaskMaxSourceBytes()),
                configured.compactionSpillDir().orElseThrow(),
                configured.compactionSpillMaxBytes(),
                uploadChunkBytes,
                orphanGrace,
                providerTimeout,
                new KafkaCompactionPartitionPass.Configuration(
                        pendingProtection,
                        claimRenewal,
                        MAXIMUM_CLOCK_SKEW,
                        minimum(Duration.ofSeconds(1), providerTimeout),
                        Math.max(3, storage.append().sessionRenewFailureGrace() + 1),
                        storage.lifecycle().registryScanPageSize(),
                        maximumPartitions));
    }

    private static OxiaClientConfiguration oxiaConfiguration(
            NereusKafkaStorageConfig storage,
            String oxiaAddress,
            Duration providerTimeout
    ) {
        int maxPendingOperations = Math.max(
                MIN_PENDING_OPERATIONS,
                addExact(
                        storage.append().executorQueueCapacity(),
                        storage.fetch().executorQueueCapacity(),
                        storage.lifecycle().executorQueueCapacity()));
        return new OxiaClientConfiguration(
                oxiaAddress,
                storage.core().oxiaNamespace(),
                providerTimeout,
                storage.append().sessionTtl(),
                MAX_COMMIT_CHAIN_SCAN,
                maxPendingOperations);
    }

    private static void requireExecutableStorage(
            NereusKafkaStorageConfig storage
    ) {
        if (!storage.enabled()) {
            throw new ConfigException(
                    NereusKafkaConfigs.ENABLED_CONFIG,
                    false,
                    "cannot map a disabled Nereus Kafka storage configuration");
        }
        if (storage.core().profile()
                != NereusKafkaStorageConfig.Profile.OBJECT_WAL_SYNC_OBJECT) {
            throw new ConfigException(
                    NereusKafkaConfigs.PROFILE_CONFIG,
                    storage.core().profile().name(),
                    "only OBJECT_WAL_SYNC_OBJECT has a production provider runtime");
        }
    }

    private static byte[] configurationCompatibilitySha256(NereusKafkaStorageConfig storage) {
        return digest(CONFIG_DIGEST_DOMAIN, output -> {
            NereusKafkaStorageConfig.Append append = storage.append();
            NereusKafkaStorageConfig.Fetch fetch = storage.fetch();
            NereusKafkaStorageConfig.Lifecycle lifecycle = storage.lifecycle();
            NereusKafkaStorageConfig.RetentionCompaction compaction =
                    storage.retentionCompaction();
            writeText(output, storage.core().profile().name());
            output.writeLong(append.requestBytes());
            output.writeLong(append.sessionTtl().toMillis());
            output.writeLong(append.sessionRenewInterval().toMillis());
            output.writeInt(append.sessionRenewFailureGrace());
            output.writeLong(fetch.maxEntryBytes());
            output.writeLong(fetch.maxResponseBytes());
            output.writeInt(fetch.operationMaxRereads());
            output.writeInt(lifecycle.recoveryChunkRecords());
            output.writeLong(lifecycle.recoveryChunkBytes());
            output.writeLong(lifecycle.checkpointIntervalRecords());
            output.writeLong(lifecycle.checkpointIntervalBytes());
            output.writeLong(lifecycle.checkpointInterval().toMillis());
            output.writeInt(lifecycle.checkpointRetainedReferences());
            output.writeLong(lifecycle.checkpointMaxBytes());
            output.writeBoolean(compaction.compactionEnabled());
            output.writeLong(compaction.compactionTaskMaxSourceBytes());
            output.writeLong(compaction.compactionTaskMaxRecords());
            output.writeInt(compaction.compactionKeyMaxBytes());
            output.writeLong(compaction.compactionDecodeMaxUncompressedBytes());
            output.writeInt(compaction.compactionDecodeMaxRatio());
            output.writeBoolean(storage.rollout().activationRequired());
        });
    }

    private static byte[] providerScopeSha256(
            NereusKafkaStorageConfig storage,
            String kafkaClusterId,
            String provider,
            URI endpoint,
            String region,
            String prefix
    ) {
        return digest(PROVIDER_DIGEST_DOMAIN, output -> {
            writeText(output, storage.core().cluster().orElseThrow());
            writeText(output, kafkaClusterId);
            writeText(output, storage.core().oxiaServiceAddress().orElseThrow());
            writeText(output, storage.core().oxiaNamespace());
            writeText(output, provider);
            writeText(output, endpoint.toASCIIString());
            writeText(output, region);
            writeText(output, storage.core().objectBucket().orElseThrow());
            writeText(output, prefix);
            output.writeBoolean(storage.core().objectPathStyleAccess());
        });
    }

    private static byte[] codeCapabilitySha256() {
        return digest(CODE_DIGEST_DOMAIN, output -> {
            output.writeInt(KafkaStorageProtocolActivationRecord.PROTOCOL_VERSION);
            output.writeInt(KafkaStorageProtocolActivationRecord.API_VERSION);
            output.writeInt(KafkaStorageProtocolActivationRecord.STREAM_HEAD_SESSION_VERSION);
            output.writeInt(KafkaStorageProtocolActivationRecord.BINDING_VERSION);
            output.writeInt(KafkaStorageProtocolActivationRecord.OBJECT_WAL_ENTRY_INDEX_VERSION);
            output.writeInt(KafkaStorageProtocolActivationRecord.NCP_VERSION);
            output.writeInt(KafkaStorageProtocolActivationRecord.NTC_VERSION);
            output.writeInt(KafkaStorageProtocolActivationRecord.CHECKPOINT_VERSION);
            output.writeInt(KafkaStorageProtocolActivationRecord.COMPACTION_STRATEGY_VERSION);
            output.writeInt(KafkaStorageProtocolActivationRecord.KAFKA_FEATURE_LEVEL);
            writeText(output, StorageProfile.OBJECT_WAL_SYNC_OBJECT.name());
        });
    }

    private static String canonicalProvider(String value) {
        String canonical = value.trim().toLowerCase(java.util.Locale.ROOT);
        if (!canonical.equals(S3_PROVIDER_TOKEN)) {
            throw new ConfigException(
                    NereusKafkaConfigs.OBJECT_PROVIDER_CONFIG,
                    value,
                    "only the explicit s3 provider token is supported");
        }
        return canonical;
    }

    private static URI defaultS3Endpoint(String region) {
        try {
            return URI.create("https://s3." + region + ".amazonaws.com");
        } catch (IllegalArgumentException failure) {
            throw new ConfigException(
                    NereusKafkaConfigs.OBJECT_REGION_CONFIG,
                    region,
                    "cannot derive a valid AWS S3 endpoint");
        }
    }

    private static String required(Optional<String> value, String name) {
        return value.orElseThrow(() -> new ConfigException(
                name, null, "must be configured before runtime mapping"));
    }

    private static String nonblank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must be nonblank");
        }
        return value;
    }

    private static Duration minimum(Duration left, Duration right) {
        return left.compareTo(right) <= 0 ? left : right;
    }

    private static Duration maximum(Duration left, Duration right) {
        return left.compareTo(right) >= 0 ? left : right;
    }

    private static Duration multiplyExact(Duration value, int multiplier) {
        try {
            return value.multipliedBy(multiplier);
        } catch (ArithmeticException failure) {
            throw new ConfigException("Nereus Kafka duration mapping overflows");
        }
    }

    private static Duration dividePositive(Duration value, int divisor) {
        long valueMillis = value.toMillis();
        if (valueMillis <= 1) {
            throw new ConfigException(
                    "Nereus Kafka compaction claim duration must exceed one millisecond");
        }
        return Duration.ofMillis(Math.max(1, valueMillis / divisor));
    }

    private static int addExact(int first, int second, int third) {
        return Math.addExact(Math.addExact(first, second), third);
    }

    private static int addExact(int first, int second) {
        return Math.addExact(first, second);
    }

    private static int multiplyExact(int value, int multiplier) {
        return Math.multiplyExact(value, multiplier);
    }

    private static byte[] digest(String domain, DigestWriter writer) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            writeText(output, domain);
            writer.write(output);
            output.flush();
            return sha256(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("in-memory Nereus Kafka digest encoding failed", failure);
        }
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static void writeText(DataOutputStream output, String value) throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    @FunctionalInterface
    private interface DigestWriter {
        void write(DataOutputStream output) throws IOException;
    }
}
