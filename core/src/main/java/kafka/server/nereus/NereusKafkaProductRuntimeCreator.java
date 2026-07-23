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

import com.nereusstream.kafka.recovery.KafkaPartitionRecoveryLauncher;
import com.nereusstream.kafka.runtime.NereusKafkaObjectWalActivationContext;
import com.nereusstream.kafka.runtime.NereusKafkaObjectWalRuntimeContext;
import com.nereusstream.kafka.runtime.NereusKafkaObjectWalRuntimeFactory;
import com.nereusstream.kafka.runtime.NereusKafkaRuntime;
import com.nereusstream.objectstore.ObjectStoreProvider;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import com.nereusstream.objectstore.S3CompatibleObjectStoreProvider;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.KRaftMetadataCache;
import org.apache.kafka.server.config.NereusKafkaStorageConfig;
import org.apache.kafka.server.util.KafkaScheduler;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Creates the activation-backed product runtime from an already-registered Kafka broker epoch. */
public final class NereusKafkaProductRuntimeCreator {
    private final NereusKafkaRuntimeConfigurationMapper mapper;

    public NereusKafkaProductRuntimeCreator() {
        this(new NereusKafkaRuntimeConfigurationMapper());
    }

    NereusKafkaProductRuntimeCreator(
            NereusKafkaRuntimeConfigurationMapper mapper
    ) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    public NereusKafkaRuntime create(
            NereusKafkaStorageConfig storage,
            String kafkaClusterId,
            int brokerId,
            long brokerEpoch,
            String runtimeInstanceId,
            String kafkaVersion,
            String nereusBuild,
            String javaVersion,
            KafkaScheduler scheduler,
            Time time,
            KRaftMetadataCache metadataCache,
            List<Path> logDirectories,
            KafkaPartitionRecoveryLauncher recoveryLauncher
    ) {
        NereusKafkaMappedRuntimeConfiguration mapped = mapper.map(
                storage,
                kafkaClusterId,
                brokerId,
                brokerEpoch,
                runtimeInstanceId,
                kafkaVersion,
                nereusBuild,
                javaVersion);
        ObjectStoreProvider provider = switch (mapped.objectProviderToken()) {
            case NereusKafkaRuntimeConfigurationMapper.S3_PROVIDER_TOKEN ->
                new S3CompatibleObjectStoreProvider();
            default -> throw new IllegalStateException(
                    "mapped an unsupported Nereus object provider");
        };
        NereusKafkaClock clock = new NereusKafkaClock(time);
        NereusKafkaObjectWalRuntimeContext context =
                new NereusKafkaObjectWalRuntimeContext(
                        provider,
                        emptySecretResolver(),
                        Objects.requireNonNull(
                                scheduler, "scheduler").scheduledExecutorService(),
                        Objects.requireNonNull(
                                recoveryLauncher, "recoveryLauncher"),
                        clock,
                        () -> CompletableFuture.completedFuture(null));
        NereusKafkaObjectWalActivationContext activation =
                new NereusKafkaObjectWalActivationContext(
                        mapped.capability(),
                        new NereusKafkaStorageClusterSnapshotProvider(
                                kafkaClusterId,
                                Objects.requireNonNull(
                                        metadataCache, "metadataCache"),
                                logDirectories),
                        storage.rollout().readinessTimeout(),
                        activationPollInterval(
                                storage.rollout().capabilityHeartbeat(),
                                storage.rollout().readinessTimeout()));
        return NereusKafkaObjectWalRuntimeFactory.createActivated(
                mapped.runtime(), context, activation);
    }

    private static ObjectStoreSecretResolver emptySecretResolver() {
        return ignored -> Optional.empty();
    }

    private static Duration activationPollInterval(
            Duration heartbeat,
            Duration waitTimeout
    ) {
        Duration oneSecond = Duration.ofSeconds(1);
        Duration candidate = heartbeat.compareTo(oneSecond) < 0
                ? heartbeat : oneSecond;
        return candidate.compareTo(waitTimeout) <= 0
                ? candidate : waitTimeout;
    }
}
