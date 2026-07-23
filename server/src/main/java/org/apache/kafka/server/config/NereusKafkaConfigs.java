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

import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

/**
 * Stock-compatible configuration surface for the optional Nereus Kafka storage runtime.
 *
 * <p>This class deliberately has no dependency on Nereus artifacts. Cross-field and Kafka-mode validation is
 * performed by the typed runtime configuration layer; this definition only owns names, defaults, primitive types,
 * and independent hard ranges.
 */
public final class NereusKafkaConfigs {
    private static final long MIB = 1024L * 1024L;
    private static final long GIB = 1024L * MIB;
    private static final long TIB = 1024L * GIB;

    public static final long MAX_KAFKA_ENTRY_BYTES = 64L * MIB;
    public static final long MAX_KAFKA_REQUEST_BYTES = 256L * MIB;
    public static final long MAX_KAFKA_CHECKPOINT_BYTES = GIB;

    public static final String ENABLED_CONFIG = "nereus.kafka.storage.enabled";
    public static final boolean ENABLED_DEFAULT = false;
    public static final String CLUSTER_CONFIG = "nereus.kafka.storage.cluster";
    public static final String PROFILE_CONFIG = "nereus.kafka.storage.profile";
    public static final String PROFILE_DEFAULT = "BOOKKEEPER_WAL_ASYNC_OBJECT";
    public static final String OXIA_SERVICE_ADDRESS_CONFIG =
            "nereus.kafka.storage.oxia.service.address";
    public static final String OXIA_NAMESPACE_CONFIG = "nereus.kafka.storage.oxia.namespace";
    public static final String OXIA_NAMESPACE_DEFAULT = "default";
    public static final String OBJECT_PROVIDER_CONFIG = "nereus.kafka.storage.object.provider";
    public static final String OBJECT_BUCKET_CONFIG = "nereus.kafka.storage.object.bucket";
    public static final String OBJECT_ENDPOINT_CONFIG = "nereus.kafka.storage.object.endpoint";
    public static final String OBJECT_REGION_CONFIG = "nereus.kafka.storage.object.region";
    public static final String OBJECT_PATH_STYLE_ACCESS_CONFIG =
            "nereus.kafka.storage.object.path.style.access";
    public static final String BOOKKEEPER_METADATA_SERVICE_URI_CONFIG =
            "nereus.kafka.storage.bookkeeper.metadata.service.uri";
    public static final String CACHE_DIR_CONFIG = "nereus.kafka.storage.cache.dir";

    public static final String APPEND_TIMEOUT_MS_CONFIG = "nereus.kafka.storage.append.timeout.ms";
    public static final long APPEND_TIMEOUT_MS_DEFAULT = 30_000L;
    public static final String APPEND_EXECUTOR_THREADS_CONFIG =
            "nereus.kafka.storage.append.executor.threads";
    public static final int APPEND_EXECUTOR_THREADS_DEFAULT =
            Math.min(64, Math.max(8, Runtime.getRuntime().availableProcessors() * 2));
    public static final String APPEND_EXECUTOR_QUEUE_CAPACITY_CONFIG =
            "nereus.kafka.storage.append.executor.queue.capacity";
    public static final int APPEND_EXECUTOR_QUEUE_CAPACITY_DEFAULT = 4096;
    public static final String APPEND_INFLIGHT_BYTES_CONFIG =
            "nereus.kafka.storage.append.inflight.bytes";
    public static final long APPEND_INFLIGHT_BYTES_DEFAULT = 512L * MIB;
    public static final String APPEND_REQUEST_BYTES_CONFIG =
            "nereus.kafka.storage.append.request.bytes";
    public static final long APPEND_REQUEST_BYTES_DEFAULT = 128L * MIB;
    public static final String SESSION_TTL_MS_CONFIG = "nereus.kafka.storage.session.ttl.ms";
    public static final long SESSION_TTL_MS_DEFAULT = 30_000L;
    public static final String SESSION_RENEW_INTERVAL_MS_CONFIG =
            "nereus.kafka.storage.session.renew.interval.ms";
    public static final long SESSION_RENEW_INTERVAL_MS_DEFAULT = 5_000L;
    public static final String SESSION_RENEW_FAILURE_GRACE_CONFIG =
            "nereus.kafka.storage.session.renew.failure.grace";
    public static final int SESSION_RENEW_FAILURE_GRACE_DEFAULT = 2;

    public static final String FETCH_TIMEOUT_MS_CONFIG = "nereus.kafka.storage.fetch.timeout.ms";
    public static final long FETCH_TIMEOUT_MS_DEFAULT = 30_000L;
    public static final String FETCH_EXECUTOR_THREADS_CONFIG =
            "nereus.kafka.storage.fetch.executor.threads";
    public static final int FETCH_EXECUTOR_THREADS_DEFAULT =
            Math.min(128, Math.max(16, Runtime.getRuntime().availableProcessors() * 4));
    public static final String FETCH_EXECUTOR_QUEUE_CAPACITY_CONFIG =
            "nereus.kafka.storage.fetch.executor.queue.capacity";
    public static final int FETCH_EXECUTOR_QUEUE_CAPACITY_DEFAULT = 4096;
    public static final String FETCH_INFLIGHT_BYTES_CONFIG =
            "nereus.kafka.storage.fetch.inflight.bytes";
    public static final long FETCH_INFLIGHT_BYTES_DEFAULT = GIB;
    public static final String FETCH_MAX_ENTRY_BYTES_CONFIG =
            "nereus.kafka.storage.fetch.max.entry.bytes";
    public static final long FETCH_MAX_ENTRY_BYTES_DEFAULT = MAX_KAFKA_ENTRY_BYTES;
    public static final String FETCH_MAX_RESPONSE_BYTES_CONFIG =
            "nereus.kafka.storage.fetch.max.response.bytes";
    public static final long FETCH_MAX_RESPONSE_BYTES_DEFAULT = 128L * MIB;
    public static final String FETCH_OPERATION_MAX_REREADS_CONFIG =
            "nereus.kafka.storage.fetch.operation.max.rereads";
    public static final int FETCH_OPERATION_MAX_REREADS_DEFAULT = 1024;

    public static final String LIFECYCLE_EXECUTOR_THREADS_CONFIG =
            "nereus.kafka.storage.lifecycle.executor.threads";
    public static final int LIFECYCLE_EXECUTOR_THREADS_DEFAULT = 8;
    public static final String LIFECYCLE_EXECUTOR_QUEUE_CAPACITY_CONFIG =
            "nereus.kafka.storage.lifecycle.executor.queue.capacity";
    public static final int LIFECYCLE_EXECUTOR_QUEUE_CAPACITY_DEFAULT = 2048;
    public static final String RECOVERY_EXECUTOR_THREADS_CONFIG =
            "nereus.kafka.storage.recovery.executor.threads";
    public static final int RECOVERY_EXECUTOR_THREADS_DEFAULT = 8;
    public static final String RECOVERY_TIMEOUT_MS_CONFIG =
            "nereus.kafka.storage.recovery.timeout.ms";
    public static final long RECOVERY_TIMEOUT_MS_DEFAULT = 900_000L;
    public static final String RECOVERY_CHUNK_RECORDS_CONFIG =
            "nereus.kafka.storage.recovery.chunk.records";
    public static final int RECOVERY_CHUNK_RECORDS_DEFAULT = 100_000;
    public static final String RECOVERY_CHUNK_BYTES_CONFIG =
            "nereus.kafka.storage.recovery.chunk.bytes";
    public static final long RECOVERY_CHUNK_BYTES_DEFAULT = 256L * MIB;
    public static final String RECOVERY_WARN_BYTES_CONFIG =
            "nereus.kafka.storage.recovery.warn.bytes";
    public static final long RECOVERY_WARN_BYTES_DEFAULT = 8L * GIB;
    public static final String CHECKPOINT_INTERVAL_RECORDS_CONFIG =
            "nereus.kafka.storage.checkpoint.interval.records";
    public static final long CHECKPOINT_INTERVAL_RECORDS_DEFAULT = 1_000_000L;
    public static final String CHECKPOINT_INTERVAL_BYTES_CONFIG =
            "nereus.kafka.storage.checkpoint.interval.bytes";
    public static final long CHECKPOINT_INTERVAL_BYTES_DEFAULT = GIB;
    public static final String CHECKPOINT_INTERVAL_MS_CONFIG =
            "nereus.kafka.storage.checkpoint.interval.ms";
    public static final long CHECKPOINT_INTERVAL_MS_DEFAULT = 300_000L;
    public static final String CHECKPOINT_RETAINED_REFERENCES_CONFIG =
            "nereus.kafka.storage.checkpoint.retained.references";
    public static final int CHECKPOINT_RETAINED_REFERENCES_DEFAULT = 3;
    public static final String CHECKPOINT_MAX_BYTES_CONFIG =
            "nereus.kafka.storage.checkpoint.max.bytes";
    public static final long CHECKPOINT_MAX_BYTES_DEFAULT = MAX_KAFKA_CHECKPOINT_BYTES;
    public static final String REGISTRY_SCAN_INTERVAL_MS_CONFIG =
            "nereus.kafka.storage.registry.scan.interval.ms";
    public static final long REGISTRY_SCAN_INTERVAL_MS_DEFAULT = 30_000L;
    public static final String REGISTRY_SCAN_PAGE_SIZE_CONFIG =
            "nereus.kafka.storage.registry.scan.page.size";
    public static final int REGISTRY_SCAN_PAGE_SIZE_DEFAULT = 256;

    public static final String RETENTION_CHECK_INTERVAL_MS_CONFIG =
            "nereus.kafka.storage.retention.check.interval.ms";
    public static final long RETENTION_CHECK_INTERVAL_MS_DEFAULT = 300_000L;
    public static final String COMPACTION_ENABLED_CONFIG =
            "nereus.kafka.storage.compaction.enabled";
    public static final boolean COMPACTION_ENABLED_DEFAULT = true;
    public static final String COMPACTION_WORKER_THREADS_CONFIG =
            "nereus.kafka.storage.compaction.worker.threads";
    public static final int COMPACTION_WORKER_THREADS_DEFAULT = 4;
    public static final String COMPACTION_MAX_CONCURRENT_TASKS_CONFIG =
            "nereus.kafka.storage.compaction.max.concurrent.tasks";
    public static final int COMPACTION_MAX_CONCURRENT_TASKS_DEFAULT = 8;
    public static final String COMPACTION_TASK_MAX_SOURCE_BYTES_CONFIG =
            "nereus.kafka.storage.compaction.task.max.source.bytes";
    public static final long COMPACTION_TASK_MAX_SOURCE_BYTES_DEFAULT = 8L * GIB;
    public static final String COMPACTION_TASK_MAX_RECORDS_CONFIG =
            "nereus.kafka.storage.compaction.task.max.records";
    public static final long COMPACTION_TASK_MAX_RECORDS_DEFAULT = 100_000_000L;
    public static final String COMPACTION_KEY_MAX_BYTES_CONFIG =
            "nereus.kafka.storage.compaction.key.max.bytes";
    public static final int COMPACTION_KEY_MAX_BYTES_DEFAULT = (int) MIB;
    public static final String COMPACTION_DECODE_MAX_UNCOMPRESSED_BYTES_CONFIG =
            "nereus.kafka.storage.compaction.decode.max.uncompressed.bytes";
    public static final long COMPACTION_DECODE_MAX_UNCOMPRESSED_BYTES_DEFAULT = GIB;
    public static final String COMPACTION_DECODE_MAX_RATIO_CONFIG =
            "nereus.kafka.storage.compaction.decode.max.ratio";
    public static final int COMPACTION_DECODE_MAX_RATIO_DEFAULT = 100;
    public static final String COMPACTION_SPILL_DIR_CONFIG =
            "nereus.kafka.storage.compaction.spill.dir";
    public static final String COMPACTION_SPILL_MAX_BYTES_CONFIG =
            "nereus.kafka.storage.compaction.spill.max.bytes";
    public static final long COMPACTION_SPILL_MAX_BYTES_DEFAULT = 100L * GIB;

    public static final String ACTIVATION_REQUIRED_CONFIG =
            "nereus.kafka.storage.activation.required";
    public static final boolean ACTIVATION_REQUIRED_DEFAULT = true;
    public static final String READINESS_TIMEOUT_MS_CONFIG =
            "nereus.kafka.storage.readiness.timeout.ms";
    public static final long READINESS_TIMEOUT_MS_DEFAULT = 300_000L;
    public static final String CAPABILITY_HEARTBEAT_MS_CONFIG =
            "nereus.kafka.storage.capability.heartbeat.ms";
    public static final long CAPABILITY_HEARTBEAT_MS_DEFAULT = 5_000L;
    public static final String CAPABILITY_EXPIRY_MS_CONFIG =
            "nereus.kafka.storage.capability.expiry.ms";
    public static final long CAPABILITY_EXPIRY_MS_DEFAULT = 30_000L;
    public static final String SHUTDOWN_DRAIN_TIMEOUT_MS_CONFIG =
            "nereus.kafka.storage.shutdown.drain.timeout.ms";
    public static final long SHUTDOWN_DRAIN_TIMEOUT_MS_DEFAULT = 120_000L;
    public static final String SHUTDOWN_CHECKPOINT_TIMEOUT_MS_CONFIG =
            "nereus.kafka.storage.shutdown.checkpoint.timeout.ms";
    public static final long SHUTDOWN_CHECKPOINT_TIMEOUT_MS_DEFAULT = 60_000L;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ENABLED_CONFIG, BOOLEAN, ENABLED_DEFAULT, HIGH,
                    "Enable the optional Nereus native Kafka storage runtime.")
            .define(CLUSTER_CONFIG, STRING, null, HIGH,
                    "Canonical Nereus storage namespace for this Kafka cluster.")
            .define(PROFILE_CONFIG, STRING, PROFILE_DEFAULT,
                    ConfigDef.ValidString.in(
                            "OBJECT_WAL_SYNC_OBJECT",
                            "OBJECT_WAL_ASYNC_OBJECT",
                            "BOOKKEEPER_WAL_ONLY",
                            "BOOKKEEPER_WAL_ASYNC_OBJECT",
                            "BOOKKEEPER_WAL_SYNC_OBJECT"),
                    HIGH, "Storage profile used for newly created Kafka partition bindings.")
            .define(OXIA_SERVICE_ADDRESS_CONFIG, STRING, null, HIGH,
                    "Oxia service address used by the Nereus metadata store.")
            .define(OXIA_NAMESPACE_CONFIG, STRING, OXIA_NAMESPACE_DEFAULT, MEDIUM,
                    "Oxia namespace used by the Nereus metadata store.")
            .define(OBJECT_PROVIDER_CONFIG, STRING, null, HIGH,
                    "Object-store provider name required by object-using profiles.")
            .define(OBJECT_BUCKET_CONFIG, STRING, null, HIGH,
                    "Object-store bucket required by object-using profiles.")
            .define(OBJECT_ENDPOINT_CONFIG, STRING, null, MEDIUM,
                    "Optional object-store endpoint.")
            .define(OBJECT_REGION_CONFIG, STRING, null, MEDIUM,
                    "Optional object-store region.")
            .define(OBJECT_PATH_STYLE_ACCESS_CONFIG, BOOLEAN, false, LOW,
                    "Use provider-specific path-style object-store access.")
            .define(BOOKKEEPER_METADATA_SERVICE_URI_CONFIG, STRING, null, HIGH,
                    "BookKeeper metadata service URI required by BookKeeper profiles.")
            .define(CACHE_DIR_CONFIG, STRING, null, HIGH,
                    "Dedicated ephemeral Nereus cache directory.")
            .define(APPEND_TIMEOUT_MS_CONFIG, LONG, APPEND_TIMEOUT_MS_DEFAULT,
                    between(1_000L, 300_000L), MEDIUM, "Stable append timeout in milliseconds.")
            .define(APPEND_EXECUTOR_THREADS_CONFIG, INT, APPEND_EXECUTOR_THREADS_DEFAULT,
                    between(1, 256), MEDIUM, "Bounded append executor thread count.")
            .define(APPEND_EXECUTOR_QUEUE_CAPACITY_CONFIG, INT, APPEND_EXECUTOR_QUEUE_CAPACITY_DEFAULT,
                    between(1, 65_536), MEDIUM, "Bounded append executor queue capacity.")
            .define(APPEND_INFLIGHT_BYTES_CONFIG, LONG, APPEND_INFLIGHT_BYTES_DEFAULT,
                    between(64L * MIB, 16L * GIB), MEDIUM, "Global append in-flight byte budget.")
            .define(APPEND_REQUEST_BYTES_CONFIG, LONG, APPEND_REQUEST_BYTES_DEFAULT,
                    between(MIB, MAX_KAFKA_REQUEST_BYTES), MEDIUM, "Per-request append byte budget.")
            .define(SESSION_TTL_MS_CONFIG, LONG, SESSION_TTL_MS_DEFAULT,
                    atLeast(1_000L), MEDIUM, "Authoritative append session TTL in milliseconds.")
            .define(SESSION_RENEW_INTERVAL_MS_CONFIG, LONG, SESSION_RENEW_INTERVAL_MS_DEFAULT,
                    between(500L, 300_000L), MEDIUM, "Append session renewal interval in milliseconds.")
            .define(SESSION_RENEW_FAILURE_GRACE_CONFIG, INT, SESSION_RENEW_FAILURE_GRACE_DEFAULT,
                    between(0, 10), MEDIUM, "Bounded renewal failures tolerated before write fencing.")
            .define(FETCH_TIMEOUT_MS_CONFIG, LONG, FETCH_TIMEOUT_MS_DEFAULT,
                    between(1_000L, 300_000L), MEDIUM, "Fetch operation timeout in milliseconds.")
            .define(FETCH_EXECUTOR_THREADS_CONFIG, INT, FETCH_EXECUTOR_THREADS_DEFAULT,
                    between(1, 512), MEDIUM, "Bounded fetch executor thread count.")
            .define(FETCH_EXECUTOR_QUEUE_CAPACITY_CONFIG, INT, FETCH_EXECUTOR_QUEUE_CAPACITY_DEFAULT,
                    between(1, 65_536), MEDIUM, "Bounded fetch executor queue capacity.")
            .define(FETCH_INFLIGHT_BYTES_CONFIG, LONG, FETCH_INFLIGHT_BYTES_DEFAULT,
                    between(64L * MIB, 32L * GIB), MEDIUM, "Global fetch in-flight byte budget.")
            .define(FETCH_MAX_ENTRY_BYTES_CONFIG, LONG, FETCH_MAX_ENTRY_BYTES_DEFAULT,
                    between(1L, MAX_KAFKA_ENTRY_BYTES), HIGH, "Maximum encoded Kafka entry size.")
            .define(FETCH_MAX_RESPONSE_BYTES_CONFIG, LONG, FETCH_MAX_RESPONSE_BYTES_DEFAULT,
                    between(MIB, MAX_KAFKA_REQUEST_BYTES), MEDIUM, "Maximum assembled fetch response size.")
            .define(FETCH_OPERATION_MAX_REREADS_CONFIG, INT, FETCH_OPERATION_MAX_REREADS_DEFAULT,
                    between(1, 1_000_000), MEDIUM, "Maximum bounded re-reads in one fetch operation.")
            .define(LIFECYCLE_EXECUTOR_THREADS_CONFIG, INT, LIFECYCLE_EXECUTOR_THREADS_DEFAULT,
                    between(1, 64), MEDIUM, "Partition lifecycle executor thread count.")
            .define(LIFECYCLE_EXECUTOR_QUEUE_CAPACITY_CONFIG, INT, LIFECYCLE_EXECUTOR_QUEUE_CAPACITY_DEFAULT,
                    between(1, 65_536), MEDIUM, "Partition lifecycle executor queue capacity.")
            .define(RECOVERY_EXECUTOR_THREADS_CONFIG, INT, RECOVERY_EXECUTOR_THREADS_DEFAULT,
                    between(1, 128), MEDIUM, "Partition recovery executor thread count.")
            .define(RECOVERY_TIMEOUT_MS_CONFIG, LONG, RECOVERY_TIMEOUT_MS_DEFAULT,
                    between(10_000L, 3_600_000L), HIGH, "Partition recovery timeout in milliseconds.")
            .define(RECOVERY_CHUNK_RECORDS_CONFIG, INT, RECOVERY_CHUNK_RECORDS_DEFAULT,
                    between(1, 1_000_000), MEDIUM, "Maximum records processed by one recovery chunk.")
            .define(RECOVERY_CHUNK_BYTES_CONFIG, LONG, RECOVERY_CHUNK_BYTES_DEFAULT,
                    between(MIB, GIB), MEDIUM, "Maximum bytes processed by one recovery chunk.")
            .define(RECOVERY_WARN_BYTES_CONFIG, LONG, RECOVERY_WARN_BYTES_DEFAULT,
                    atLeast(1L), LOW, "Recovery replay size warning threshold.")
            .define(CHECKPOINT_INTERVAL_RECORDS_CONFIG, LONG, CHECKPOINT_INTERVAL_RECORDS_DEFAULT,
                    atLeast(1L), MEDIUM, "Record interval for Kafka checkpoint publication.")
            .define(CHECKPOINT_INTERVAL_BYTES_CONFIG, LONG, CHECKPOINT_INTERVAL_BYTES_DEFAULT,
                    between(64L * MIB, TIB), MEDIUM, "Byte interval for Kafka checkpoint publication.")
            .define(CHECKPOINT_INTERVAL_MS_CONFIG, LONG, CHECKPOINT_INTERVAL_MS_DEFAULT,
                    between(10_000L, 86_400_000L), MEDIUM, "Time interval for Kafka checkpoint publication.")
            .define(CHECKPOINT_RETAINED_REFERENCES_CONFIG, INT, CHECKPOINT_RETAINED_REFERENCES_DEFAULT,
                    between(1, 3), HIGH, "Number of checkpoint references retained in the binding.")
            .define(CHECKPOINT_MAX_BYTES_CONFIG, LONG, CHECKPOINT_MAX_BYTES_DEFAULT,
                    between(MIB, MAX_KAFKA_CHECKPOINT_BYTES), HIGH, "Maximum encoded Kafka checkpoint size.")
            .define(REGISTRY_SCAN_INTERVAL_MS_CONFIG, LONG, REGISTRY_SCAN_INTERVAL_MS_DEFAULT,
                    between(1_000L, 3_600_000L), LOW, "Kafka binding registry scan interval.")
            .define(REGISTRY_SCAN_PAGE_SIZE_CONFIG, INT, REGISTRY_SCAN_PAGE_SIZE_DEFAULT,
                    between(1, 1024), LOW, "Kafka binding registry scan page size.")
            .define(RETENTION_CHECK_INTERVAL_MS_CONFIG, LONG, RETENTION_CHECK_INTERVAL_MS_DEFAULT,
                    between(1_000L, 86_400_000L), LOW, "Kafka retention evaluation interval.")
            .define(COMPACTION_ENABLED_CONFIG, BOOLEAN, COMPACTION_ENABLED_DEFAULT, HIGH,
                    "Enable Kafka topic compaction materialization.")
            .define(COMPACTION_WORKER_THREADS_CONFIG, INT, COMPACTION_WORKER_THREADS_DEFAULT,
                    between(1, 128), MEDIUM, "Compaction worker thread count.")
            .define(COMPACTION_MAX_CONCURRENT_TASKS_CONFIG, INT, COMPACTION_MAX_CONCURRENT_TASKS_DEFAULT,
                    between(1, 256), MEDIUM, "Maximum concurrent compaction tasks.")
            .define(COMPACTION_TASK_MAX_SOURCE_BYTES_CONFIG, LONG, COMPACTION_TASK_MAX_SOURCE_BYTES_DEFAULT,
                    between(64L * MIB, TIB), MEDIUM, "Maximum source bytes processed by one compaction task.")
            .define(COMPACTION_TASK_MAX_RECORDS_CONFIG, LONG, COMPACTION_TASK_MAX_RECORDS_DEFAULT,
                    atLeast(1L), MEDIUM, "Maximum records processed by one compaction task.")
            .define(COMPACTION_KEY_MAX_BYTES_CONFIG, INT, COMPACTION_KEY_MAX_BYTES_DEFAULT,
                    between(1, (int) MIB), HIGH, "Maximum compacted Kafka key size.")
            .define(COMPACTION_DECODE_MAX_UNCOMPRESSED_BYTES_CONFIG, LONG,
                    COMPACTION_DECODE_MAX_UNCOMPRESSED_BYTES_DEFAULT,
                    between(MIB, GIB), HIGH, "Maximum uncompressed bytes decoded by one compaction chunk.")
            .define(COMPACTION_DECODE_MAX_RATIO_CONFIG, INT, COMPACTION_DECODE_MAX_RATIO_DEFAULT,
                    between(1, 1000), HIGH, "Maximum accepted compression expansion ratio.")
            .define(COMPACTION_SPILL_DIR_CONFIG, STRING, null, MEDIUM,
                    "Optional compaction spill directory; defaults below the cache directory.")
            .define(COMPACTION_SPILL_MAX_BYTES_CONFIG, LONG, COMPACTION_SPILL_MAX_BYTES_DEFAULT,
                    atLeast(1L), MEDIUM, "Global compaction spill byte budget.")
            .define(ACTIVATION_REQUIRED_CONFIG, BOOLEAN, ACTIVATION_REQUIRED_DEFAULT, HIGH,
                    "Require cluster-wide Nereus protocol activation before broker readiness.")
            .define(READINESS_TIMEOUT_MS_CONFIG, LONG, READINESS_TIMEOUT_MS_DEFAULT,
                    atLeast(1L), HIGH, "Broker Nereus readiness timeout.")
            .define(CAPABILITY_HEARTBEAT_MS_CONFIG, LONG, CAPABILITY_HEARTBEAT_MS_DEFAULT,
                    between(1_000L, 30_000L), MEDIUM, "Broker Nereus capability heartbeat interval.")
            .define(CAPABILITY_EXPIRY_MS_CONFIG, LONG, CAPABILITY_EXPIRY_MS_DEFAULT,
                    atLeast(1_000L), HIGH, "Broker Nereus capability expiry interval.")
            .define(SHUTDOWN_DRAIN_TIMEOUT_MS_CONFIG, LONG, SHUTDOWN_DRAIN_TIMEOUT_MS_DEFAULT,
                    between(1_000L, 900_000L), HIGH, "Broker Nereus shutdown drain timeout.")
            .define(SHUTDOWN_CHECKPOINT_TIMEOUT_MS_CONFIG, LONG, SHUTDOWN_CHECKPOINT_TIMEOUT_MS_DEFAULT,
                    atLeast(1_000L), MEDIUM, "Broker Nereus shutdown checkpoint timeout.");

    private NereusKafkaConfigs() {
    }
}
