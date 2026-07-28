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
        Rollout rollout,
        Optional<NereusKafkaBookKeeperConfig> bookKeeper
) {
    public NereusKafkaStorageConfig(
            boolean enabled,
            Core core,
            Append append,
            Fetch fetch,
            Lifecycle lifecycle,
            RetentionCompaction retentionCompaction,
            Rollout rollout
    ) {
        this(
                enabled,
                core,
                append,
                fetch,
                lifecycle,
                retentionCompaction,
                rollout,
                Optional.empty());
    }

    public NereusKafkaStorageConfig {
        Objects.requireNonNull(core, "core");
        Objects.requireNonNull(append, "append");
        Objects.requireNonNull(fetch, "fetch");
        Objects.requireNonNull(lifecycle, "lifecycle");
        Objects.requireNonNull(retentionCompaction, "retentionCompaction");
        Objects.requireNonNull(rollout, "rollout");
        bookKeeper = Objects.requireNonNull(bookKeeper, "bookKeeper");
        if (enabled) {
            validateEnabled(
                    core,
                    append,
                    fetch,
                    retentionCompaction,
                    rollout,
                    bookKeeper);
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
                        duration(config, NereusKafkaConfigs.SHUTDOWN_CHECKPOINT_TIMEOUT_MS_CONFIG)),
                config.getBoolean(NereusKafkaConfigs.ENABLED_CONFIG)
                                && profile(config).usesBookKeeper()
                        ? Optional.of(bookKeeper(config))
                        : Optional.empty());
    }

    private static void validateEnabled(
            Core core,
            Append append,
            Fetch fetch,
            RetentionCompaction retentionCompaction,
            Rollout rollout,
            Optional<NereusKafkaBookKeeperConfig> bookKeeper) {
        validateProviders(core, bookKeeper);
        validateBufferRelationships(append, fetch);
        validateRollout(retentionCompaction, rollout);
        bookKeeper.ifPresent(exact -> validateBookKeeper(exact, append, fetch, rollout));
    }

    private static void validateProviders(
            Core core,
            Optional<NereusKafkaBookKeeperConfig> bookKeeper) {
        requirePresent(core.cluster(), NereusKafkaConfigs.CLUSTER_CONFIG);
        requirePresent(core.oxiaServiceAddress(), NereusKafkaConfigs.OXIA_SERVICE_ADDRESS_CONFIG);
        requirePresent(core.cacheDir(), NereusKafkaConfigs.CACHE_DIR_CONFIG);
        requirePresent(core.objectProvider(), NereusKafkaConfigs.OBJECT_PROVIDER_CONFIG);
        requirePresent(core.objectBucket(), NereusKafkaConfigs.OBJECT_BUCKET_CONFIG);
        if (core.profile().usesBookKeeper()) {
            requirePresent(
                    core.bookKeeperMetadataServiceUri(),
                    NereusKafkaConfigs.BOOKKEEPER_METADATA_SERVICE_URI_CONFIG);
            if (bookKeeper.isEmpty()) {
                throw invalid(
                        NereusKafkaConfigs.BOOKKEEPER_DEPLOYMENT_ID_CONFIG,
                        "complete BookKeeper configuration is required by the selected profile");
            }
        } else if (bookKeeper.isPresent()) {
            throw invalid(
                    NereusKafkaConfigs.PROFILE_CONFIG,
                    "BookKeeper configuration cannot be installed for an Object-only profile");
        }
    }

    private static void validateBookKeeper(
            NereusKafkaBookKeeperConfig bookKeeper,
            Append append,
            Fetch fetch,
            Rollout rollout) {
        if (bookKeeper.operationTimeout().compareTo(append.timeout()) > 0
                || bookKeeper.operationTimeout().compareTo(fetch.timeout()) > 0
                || bookKeeper.operationTimeout().compareTo(rollout.shutdownDrainTimeout()) > 0) {
            throw invalid(
                    NereusKafkaConfigs.BOOKKEEPER_OPERATION_TIMEOUT_MS_CONFIG,
                    "must fit append, fetch, and shutdown-drain deadlines");
        }
        if (bookKeeper.allocationTimeout().compareTo(append.timeout()) > 0) {
            throw invalid(
                    NereusKafkaConfigs.BOOKKEEPER_ALLOCATION_TIMEOUT_MS_CONFIG,
                    "must fit the append deadline");
        }
        if (bookKeeper.sealTimeout().compareTo(rollout.shutdownDrainTimeout()) > 0
                || bookKeeper.deleteTimeout().compareTo(rollout.shutdownDrainTimeout()) > 0) {
            throw invalid(
                    NereusKafkaConfigs.BOOKKEEPER_SEAL_TIMEOUT_MS_CONFIG,
                    "seal and delete timeouts must fit the shutdown-drain deadline");
        }
        long appendCapacity = Math.addExact(
                append.executorThreads(), append.executorQueueCapacity());
        if (bookKeeper.maxWritesInFlight() > appendCapacity) {
            throw invalid(
                    NereusKafkaConfigs.BOOKKEEPER_MAX_WRITES_INFLIGHT_CONFIG,
                    "cannot exceed the bounded append executor capacity");
        }
        if (bookKeeper.maxReadsInFlight() > fetch.executorThreads()
                || bookKeeper.maxReadBytesInFlight() > fetch.inflightBytes()) {
            throw invalid(
                    NereusKafkaConfigs.BOOKKEEPER_MAX_READS_INFLIGHT_CONFIG,
                    "BookKeeper read limits cannot exceed the Fetch executor and byte budgets");
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

    private static Profile profile(AbstractConfig config) {
        return Profile.valueOf(config.getString(NereusKafkaConfigs.PROFILE_CONFIG));
    }

    private static NereusKafkaBookKeeperConfig bookKeeper(AbstractConfig config) {
        return new NereusKafkaBookKeeperConfig(
                requiredConfiguredText(config, NereusKafkaConfigs.BOOKKEEPER_DEPLOYMENT_ID_CONFIG),
                requiredConfiguredText(config, NereusKafkaConfigs.BOOKKEEPER_CLUSTER_ALIAS_CONFIG),
                requiredConfiguredText(
                        config,
                        NereusKafkaConfigs.BOOKKEEPER_PROVIDER_SCOPE_SHA256_CONFIG),
                config.getInt(NereusKafkaConfigs.BOOKKEEPER_LEDGER_ID_PREFIX_BITS_CONFIG),
                requiredLong(
                        config,
                        NereusKafkaConfigs.BOOKKEEPER_LEDGER_ID_PREFIX_VALUE_CONFIG),
                requiredConfiguredText(
                        config,
                        NereusKafkaConfigs.BOOKKEEPER_LEDGER_ID_RESERVATION_ID_CONFIG),
                config.getInt(NereusKafkaConfigs.BOOKKEEPER_ENSEMBLE_SIZE_CONFIG),
                config.getInt(NereusKafkaConfigs.BOOKKEEPER_WRITE_QUORUM_SIZE_CONFIG),
                config.getInt(NereusKafkaConfigs.BOOKKEEPER_ACK_QUORUM_SIZE_CONFIG),
                requiredConfiguredText(config, NereusKafkaConfigs.BOOKKEEPER_DIGEST_TYPE_CONFIG),
                optionalPath(config, NereusKafkaConfigs.BOOKKEEPER_PASSWORD_FILE_CONFIG)
                        .orElseThrow(() -> invalid(
                                NereusKafkaConfigs.BOOKKEEPER_PASSWORD_FILE_CONFIG,
                                "must be configured by a BookKeeper profile")),
                requiredConfiguredText(
                        config,
                        NereusKafkaConfigs.BOOKKEEPER_PASSWORD_VERSION_CONFIG),
                config.getLong(NereusKafkaConfigs.BOOKKEEPER_MAX_ENTRIES_PER_LEDGER_CONFIG),
                config.getLong(NereusKafkaConfigs.BOOKKEEPER_MAX_BYTES_PER_LEDGER_CONFIG),
                config.getInt(
                        NereusKafkaConfigs.BOOKKEEPER_MAX_APPEND_RANGES_PER_LEDGER_CONFIG),
                config.getInt(NereusKafkaConfigs.BOOKKEEPER_PROTECTION_SLOTS_PER_RANGE_CONFIG),
                config.getInt(
                        NereusKafkaConfigs.BOOKKEEPER_MAX_READER_LEASES_PER_LEDGER_CONFIG),
                config.getInt(
                        NereusKafkaConfigs.BOOKKEEPER_MAX_UNCERTAIN_ALLOCATIONS_CONFIG),
                duration(config, NereusKafkaConfigs.BOOKKEEPER_MAX_LEDGER_AGE_MS_CONFIG),
                config.getInt(NereusKafkaConfigs.BOOKKEEPER_MAX_WRITES_INFLIGHT_CONFIG),
                config.getInt(NereusKafkaConfigs.BOOKKEEPER_MAX_READS_INFLIGHT_CONFIG),
                config.getLong(NereusKafkaConfigs.BOOKKEEPER_MAX_READ_BYTES_INFLIGHT_CONFIG),
                duration(config, NereusKafkaConfigs.BOOKKEEPER_OPERATION_TIMEOUT_MS_CONFIG),
                duration(config, NereusKafkaConfigs.BOOKKEEPER_ALLOCATION_TIMEOUT_MS_CONFIG),
                duration(config, NereusKafkaConfigs.BOOKKEEPER_SEAL_TIMEOUT_MS_CONFIG),
                duration(config, NereusKafkaConfigs.BOOKKEEPER_DELETE_TIMEOUT_MS_CONFIG),
                duration(config, NereusKafkaConfigs.BOOKKEEPER_READER_LEASE_TTL_MS_CONFIG),
                duration(config, NereusKafkaConfigs.BOOKKEEPER_READER_LEASE_RENEW_MS_CONFIG),
                duration(
                        config,
                        NereusKafkaConfigs.BOOKKEEPER_RETENTION_SCAN_INTERVAL_MS_CONFIG),
                config.getInt(NereusKafkaConfigs.BOOKKEEPER_RETENTION_PAGE_SIZE_CONFIG),
                config.getLong(NereusKafkaConfigs.BOOKKEEPER_READINESS_EPOCH_CONFIG),
                requiredConfiguredText(
                        config,
                        NereusKafkaConfigs.BOOKKEEPER_READINESS_SHA256_CONFIG),
                config.getInt(
                        NereusKafkaConfigs.BOOKKEEPER_PERSISTENT_BROKER_COUNT_CONFIG),
                new NereusKafkaBookKeeperConfig.LedgerGc(
                        config.getInt(
                                NereusKafkaConfigs
                                        .BOOKKEEPER_GC_MAX_CONCURRENT_DELETES_CONFIG),
                        duration(
                                config,
                                NereusKafkaConfigs
                                        .BOOKKEEPER_GC_MAX_CLOCK_SKEW_MS_CONFIG),
                        duration(
                                config,
                                NereusKafkaConfigs
                                        .BOOKKEEPER_GC_DRAIN_GRACE_MS_CONFIG),
                        duration(
                                config,
                                NereusKafkaConfigs
                                        .BOOKKEEPER_GC_LATE_CREATE_AUDIT_GRACE_MS_CONFIG),
                        config.getBoolean(
                                NereusKafkaConfigs.BOOKKEEPER_GC_ENABLED_CONFIG),
                        config.getBoolean(
                                NereusKafkaConfigs.BOOKKEEPER_GC_DRY_RUN_CONFIG)));
    }

    private static long requiredLong(AbstractConfig config, String name) {
        Long value = config.getLong(name);
        if (value == null) {
            throw invalid(name, "must be configured by a BookKeeper profile");
        }
        return value;
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
