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

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/** Immutable, side-effect-free snapshot of the complete Nereus Kafka storage configuration. */
public record NereusKafkaStorageConfig(
        boolean enabled,
        Core core,
        Append append,
        Fetch fetch,
        Lifecycle lifecycle,
        RetentionCompaction retentionCompaction,
        Rollout rollout
) {
    public NereusKafkaStorageConfig {
        Objects.requireNonNull(core, "core");
        Objects.requireNonNull(append, "append");
        Objects.requireNonNull(fetch, "fetch");
        Objects.requireNonNull(lifecycle, "lifecycle");
        Objects.requireNonNull(retentionCompaction, "retentionCompaction");
        Objects.requireNonNull(rollout, "rollout");
        if (enabled) {
            validateEnabled(core, append, fetch, retentionCompaction, rollout);
        }
    }

    public static NereusKafkaStorageConfig from(AbstractConfig config) {
        Objects.requireNonNull(config, "config");
        Optional<Path> cacheDir = optionalPath(config, NereusKafkaConfigs.CACHE_DIR_CONFIG);
        Optional<Path> spillDir = optionalPath(config, NereusKafkaConfigs.COMPACTION_SPILL_DIR_CONFIG)
                .or(() -> cacheDir.map(path -> path.resolve("spill").normalize()));
        return new NereusKafkaStorageConfig(
                config.getBoolean(NereusKafkaConfigs.ENABLED_CONFIG),
                new Core(
                        optionalText(config, NereusKafkaConfigs.CLUSTER_CONFIG),
                        Profile.valueOf(config.getString(NereusKafkaConfigs.PROFILE_CONFIG)),
                        optionalText(config, NereusKafkaConfigs.OXIA_SERVICE_ADDRESS_CONFIG),
                        requiredConfiguredText(config, NereusKafkaConfigs.OXIA_NAMESPACE_CONFIG),
                        optionalText(config, NereusKafkaConfigs.OBJECT_PROVIDER_CONFIG),
                        optionalText(config, NereusKafkaConfigs.OBJECT_BUCKET_CONFIG),
                        optionalUri(config, NereusKafkaConfigs.OBJECT_ENDPOINT_CONFIG),
                        optionalText(config, NereusKafkaConfigs.OBJECT_REGION_CONFIG),
                        config.getBoolean(NereusKafkaConfigs.OBJECT_PATH_STYLE_ACCESS_CONFIG),
                        optionalUri(config, NereusKafkaConfigs.BOOKKEEPER_METADATA_SERVICE_URI_CONFIG),
                        cacheDir),
                new Append(
                        duration(config, NereusKafkaConfigs.APPEND_TIMEOUT_MS_CONFIG),
                        config.getInt(NereusKafkaConfigs.APPEND_EXECUTOR_THREADS_CONFIG),
                        config.getInt(NereusKafkaConfigs.APPEND_EXECUTOR_QUEUE_CAPACITY_CONFIG),
                        config.getLong(NereusKafkaConfigs.APPEND_INFLIGHT_BYTES_CONFIG),
                        config.getLong(NereusKafkaConfigs.APPEND_REQUEST_BYTES_CONFIG),
                        duration(config, NereusKafkaConfigs.SESSION_TTL_MS_CONFIG),
                        duration(config, NereusKafkaConfigs.SESSION_RENEW_INTERVAL_MS_CONFIG),
                        config.getInt(NereusKafkaConfigs.SESSION_RENEW_FAILURE_GRACE_CONFIG)),
                new Fetch(
                        duration(config, NereusKafkaConfigs.FETCH_TIMEOUT_MS_CONFIG),
                        config.getInt(NereusKafkaConfigs.FETCH_EXECUTOR_THREADS_CONFIG),
                        config.getInt(NereusKafkaConfigs.FETCH_EXECUTOR_QUEUE_CAPACITY_CONFIG),
                        config.getLong(NereusKafkaConfigs.FETCH_INFLIGHT_BYTES_CONFIG),
                        config.getLong(NereusKafkaConfigs.FETCH_MAX_ENTRY_BYTES_CONFIG),
                        config.getLong(NereusKafkaConfigs.FETCH_MAX_RESPONSE_BYTES_CONFIG),
                        config.getInt(NereusKafkaConfigs.FETCH_OPERATION_MAX_REREADS_CONFIG)),
                new Lifecycle(
                        config.getInt(NereusKafkaConfigs.LIFECYCLE_EXECUTOR_THREADS_CONFIG),
                        config.getInt(NereusKafkaConfigs.LIFECYCLE_EXECUTOR_QUEUE_CAPACITY_CONFIG),
                        config.getInt(NereusKafkaConfigs.RECOVERY_EXECUTOR_THREADS_CONFIG),
                        duration(config, NereusKafkaConfigs.RECOVERY_TIMEOUT_MS_CONFIG),
                        config.getInt(NereusKafkaConfigs.RECOVERY_CHUNK_RECORDS_CONFIG),
                        config.getLong(NereusKafkaConfigs.RECOVERY_CHUNK_BYTES_CONFIG),
                        config.getLong(NereusKafkaConfigs.RECOVERY_WARN_BYTES_CONFIG),
                        config.getLong(NereusKafkaConfigs.CHECKPOINT_INTERVAL_RECORDS_CONFIG),
                        config.getLong(NereusKafkaConfigs.CHECKPOINT_INTERVAL_BYTES_CONFIG),
                        duration(config, NereusKafkaConfigs.CHECKPOINT_INTERVAL_MS_CONFIG),
                        config.getInt(NereusKafkaConfigs.CHECKPOINT_RETAINED_REFERENCES_CONFIG),
                        config.getLong(NereusKafkaConfigs.CHECKPOINT_MAX_BYTES_CONFIG),
                        duration(config, NereusKafkaConfigs.REGISTRY_SCAN_INTERVAL_MS_CONFIG),
                        config.getInt(NereusKafkaConfigs.REGISTRY_SCAN_PAGE_SIZE_CONFIG)),
                new RetentionCompaction(
                        duration(config, NereusKafkaConfigs.RETENTION_CHECK_INTERVAL_MS_CONFIG),
                        config.getBoolean(NereusKafkaConfigs.COMPACTION_ENABLED_CONFIG),
                        config.getInt(NereusKafkaConfigs.COMPACTION_WORKER_THREADS_CONFIG),
                        config.getInt(NereusKafkaConfigs.COMPACTION_MAX_CONCURRENT_TASKS_CONFIG),
                        config.getLong(NereusKafkaConfigs.COMPACTION_TASK_MAX_SOURCE_BYTES_CONFIG),
                        config.getLong(NereusKafkaConfigs.COMPACTION_TASK_MAX_RECORDS_CONFIG),
                        config.getInt(NereusKafkaConfigs.COMPACTION_KEY_MAX_BYTES_CONFIG),
                        config.getLong(NereusKafkaConfigs.COMPACTION_DECODE_MAX_UNCOMPRESSED_BYTES_CONFIG),
                        config.getInt(NereusKafkaConfigs.COMPACTION_DECODE_MAX_RATIO_CONFIG),
                        spillDir,
                        config.getLong(NereusKafkaConfigs.COMPACTION_SPILL_MAX_BYTES_CONFIG)),
                new Rollout(
                        config.getBoolean(NereusKafkaConfigs.ACTIVATION_REQUIRED_CONFIG),
                        duration(config, NereusKafkaConfigs.READINESS_TIMEOUT_MS_CONFIG),
                        duration(config, NereusKafkaConfigs.CAPABILITY_HEARTBEAT_MS_CONFIG),
                        duration(config, NereusKafkaConfigs.CAPABILITY_EXPIRY_MS_CONFIG),
                        duration(config, NereusKafkaConfigs.SHUTDOWN_DRAIN_TIMEOUT_MS_CONFIG),
                        duration(config, NereusKafkaConfigs.SHUTDOWN_CHECKPOINT_TIMEOUT_MS_CONFIG)));
    }

    private static void validateEnabled(
            Core core,
            Append append,
            Fetch fetch,
            RetentionCompaction retentionCompaction,
            Rollout rollout) {
        validateProviders(core);
        validateBufferRelationships(append, fetch);
        validateRollout(retentionCompaction, rollout);
    }

    private static void validateProviders(Core core) {
        requirePresent(core.cluster(), NereusKafkaConfigs.CLUSTER_CONFIG);
        requirePresent(core.oxiaServiceAddress(), NereusKafkaConfigs.OXIA_SERVICE_ADDRESS_CONFIG);
        requirePresent(core.cacheDir(), NereusKafkaConfigs.CACHE_DIR_CONFIG);
        if (core.profile().usesBookKeeper()) {
            requirePresent(
                    core.bookKeeperMetadataServiceUri(),
                    NereusKafkaConfigs.BOOKKEEPER_METADATA_SERVICE_URI_CONFIG);
        }
        if (core.profile().usesObjectStorage()) {
            requirePresent(core.objectProvider(), NereusKafkaConfigs.OBJECT_PROVIDER_CONFIG);
            requirePresent(core.objectBucket(), NereusKafkaConfigs.OBJECT_BUCKET_CONFIG);
        }
    }

    private static void validateBufferRelationships(Append append, Fetch fetch) {
        if (append.inflightBytes() < append.requestBytes()) {
            throw invalid(NereusKafkaConfigs.APPEND_INFLIGHT_BYTES_CONFIG,
                    "must be greater than or equal to the per-request append byte limit");
        }
        if (append.sessionTtl().compareTo(multiply(append.sessionRenewInterval(), 3)) < 0) {
            throw invalid(NereusKafkaConfigs.SESSION_TTL_MS_CONFIG,
                    "must be at least three times the session renewal interval");
        }
        if (fetch.inflightBytes() < fetch.maxResponseBytes()) {
            throw invalid(NereusKafkaConfigs.FETCH_INFLIGHT_BYTES_CONFIG,
                    "must be greater than or equal to the maximum fetch response size");
        }
        if (fetch.maxResponseBytes() < fetch.maxEntryBytes()) {
            throw invalid(NereusKafkaConfigs.FETCH_MAX_RESPONSE_BYTES_CONFIG,
                    "must be greater than or equal to the maximum entry size");
        }
    }

    private static void validateRollout(
            RetentionCompaction retentionCompaction,
            Rollout rollout) {
        if (!retentionCompaction.compactionEnabled()) {
            throw invalid(NereusKafkaConfigs.COMPACTION_ENABLED_CONFIG,
                    "must remain true for the initial Nereus Kafka protocol");
        }
        if (!rollout.activationRequired()) {
            throw invalid(NereusKafkaConfigs.ACTIVATION_REQUIRED_CONFIG,
                    "must remain true in production runtime configuration");
        }
        if (rollout.capabilityExpiry().compareTo(multiply(rollout.capabilityHeartbeat(), 3)) < 0) {
            throw invalid(NereusKafkaConfigs.CAPABILITY_EXPIRY_MS_CONFIG,
                    "must be at least three times the capability heartbeat interval");
        }
        if (rollout.shutdownCheckpointTimeout().compareTo(rollout.shutdownDrainTimeout()) > 0) {
            throw invalid(NereusKafkaConfigs.SHUTDOWN_CHECKPOINT_TIMEOUT_MS_CONFIG,
                    "cannot exceed the shutdown drain timeout");
        }
    }

    private static Duration duration(AbstractConfig config, String name) {
        return Duration.ofMillis(config.getLong(name));
    }

    private static Optional<String> optionalText(AbstractConfig config, String name) {
        return Optional.ofNullable(config.getString(name)).map(String::trim).filter(value -> !value.isEmpty());
    }

    private static String requiredConfiguredText(AbstractConfig config, String name) {
        return optionalText(config, name).orElseThrow(() -> invalid(name, "cannot be blank"));
    }

    private static Optional<Path> optionalPath(AbstractConfig config, String name) {
        return optionalText(config, name).map(value -> {
            try {
                return Path.of(value).toAbsolutePath().normalize();
            } catch (RuntimeException failure) {
                throw new ConfigException(name, value, "must be a valid filesystem path");
            }
        });
    }

    private static Optional<URI> optionalUri(AbstractConfig config, String name) {
        return optionalText(config, name).map(value -> {
            try {
                URI uri = new URI(value);
                if (uri.getScheme() == null || uri.getScheme().isBlank()) {
                    throw new URISyntaxException(value, "URI scheme is required");
                }
                return uri;
            } catch (URISyntaxException failure) {
                throw new ConfigException(name, value, "must be an absolute URI");
            }
        });
    }

    private static Duration multiply(Duration value, int multiplier) {
        try {
            return value.multipliedBy(multiplier);
        } catch (ArithmeticException failure) {
            throw new ConfigException("Nereus Kafka duration relationship overflows");
        }
    }

    private static void requirePresent(Optional<?> value, String name) {
        if (value.isEmpty()) {
            throw invalid(name, "must be configured when Nereus Kafka storage is enabled");
        }
    }

    private static ConfigException invalid(String name, String message) {
        return new ConfigException(name, null, message);
    }

    public enum Profile {
        OBJECT_WAL_SYNC_OBJECT(false, true),
        OBJECT_WAL_ASYNC_OBJECT(false, true),
        BOOKKEEPER_WAL_ONLY(true, false),
        BOOKKEEPER_WAL_ASYNC_OBJECT(true, true),
        BOOKKEEPER_WAL_SYNC_OBJECT(true, true);

        private final boolean usesBookKeeper;
        private final boolean usesObjectStorage;

        Profile(boolean usesBookKeeper, boolean usesObjectStorage) {
            this.usesBookKeeper = usesBookKeeper;
            this.usesObjectStorage = usesObjectStorage;
        }

        public boolean usesBookKeeper() {
            return usesBookKeeper;
        }

        public boolean usesObjectStorage() {
            return usesObjectStorage;
        }
    }

    public record Core(
            Optional<String> cluster,
            Profile profile,
            Optional<String> oxiaServiceAddress,
            String oxiaNamespace,
            Optional<String> objectProvider,
            Optional<String> objectBucket,
            Optional<URI> objectEndpoint,
            Optional<String> objectRegion,
            boolean objectPathStyleAccess,
            Optional<URI> bookKeeperMetadataServiceUri,
            Optional<Path> cacheDir
    ) {
        public Core {
            Objects.requireNonNull(cluster, "cluster");
            Objects.requireNonNull(profile, "profile");
            Objects.requireNonNull(oxiaServiceAddress, "oxiaServiceAddress");
            Objects.requireNonNull(oxiaNamespace, "oxiaNamespace");
            Objects.requireNonNull(objectProvider, "objectProvider");
            Objects.requireNonNull(objectBucket, "objectBucket");
            Objects.requireNonNull(objectEndpoint, "objectEndpoint");
            Objects.requireNonNull(objectRegion, "objectRegion");
            Objects.requireNonNull(bookKeeperMetadataServiceUri, "bookKeeperMetadataServiceUri");
            Objects.requireNonNull(cacheDir, "cacheDir");
        }
    }

    public record Append(
            Duration timeout,
            int executorThreads,
            int executorQueueCapacity,
            long inflightBytes,
            long requestBytes,
            Duration sessionTtl,
            Duration sessionRenewInterval,
            int sessionRenewFailureGrace
    ) { }

    public record Fetch(
            Duration timeout,
            int executorThreads,
            int executorQueueCapacity,
            long inflightBytes,
            long maxEntryBytes,
            long maxResponseBytes,
            int operationMaxRereads
    ) { }

    public record Lifecycle(
            int executorThreads,
            int executorQueueCapacity,
            int recoveryExecutorThreads,
            Duration recoveryTimeout,
            int recoveryChunkRecords,
            long recoveryChunkBytes,
            long recoveryWarnBytes,
            long checkpointIntervalRecords,
            long checkpointIntervalBytes,
            Duration checkpointInterval,
            int checkpointRetainedReferences,
            long checkpointMaxBytes,
            Duration registryScanInterval,
            int registryScanPageSize
    ) { }

    public record RetentionCompaction(
            Duration retentionCheckInterval,
            boolean compactionEnabled,
            int compactionWorkerThreads,
            int compactionMaxConcurrentTasks,
            long compactionTaskMaxSourceBytes,
            long compactionTaskMaxRecords,
            int compactionKeyMaxBytes,
            long compactionDecodeMaxUncompressedBytes,
            int compactionDecodeMaxRatio,
            Optional<Path> compactionSpillDir,
            long compactionSpillMaxBytes
    ) {
        public RetentionCompaction {
            Objects.requireNonNull(compactionSpillDir, "compactionSpillDir");
        }
    }

    public record Rollout(
            boolean activationRequired,
            Duration readinessTimeout,
            Duration capabilityHeartbeat,
            Duration capabilityExpiry,
            Duration shutdownDrainTimeout,
            Duration shutdownCheckpointTimeout
    ) { }
}
