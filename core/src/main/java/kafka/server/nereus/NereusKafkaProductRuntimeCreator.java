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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.KRaftMetadataCache;
import org.apache.kafka.server.config.NereusKafkaBookKeeperConfig;
import org.apache.kafka.server.config.NereusKafkaStorageConfig;
import org.apache.kafka.server.util.KafkaScheduler;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.bookkeeper.BookKeeperBrokerReadiness;
import com.nereusstream.bookkeeper.BookKeeperBrokerReadinessProvider;
import com.nereusstream.bookkeeper.BookKeeperPasswordProvider;
import com.nereusstream.bookkeeper.BookKeeperSecretRef;
import com.nereusstream.kafka.runtime.NereusKafkaBookKeeperWalRuntimeContext;
import com.nereusstream.kafka.runtime.NereusKafkaCompactionContext;
import com.nereusstream.kafka.runtime.NereusKafkaMaintenanceContext;
import com.nereusstream.kafka.runtime.NereusKafkaObjectWalActivationContext;
import com.nereusstream.kafka.runtime.NereusKafkaObjectWalRuntimeContext;
import com.nereusstream.kafka.runtime.NereusKafkaObjectWalRuntimeFactory;
import com.nereusstream.kafka.runtime.NereusKafkaRuntime;
import com.nereusstream.objectstore.ObjectStoreProvider;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import com.nereusstream.objectstore.S3CompatibleObjectStoreProvider;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;

import java.io.IOException;
import java.nio.file.Files;
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
            NereusKafkaForkRuntimeBridges bridges
    ) {
        NereusKafkaForkRuntimeBridges exactBridges =
                Objects.requireNonNull(bridges, "bridges");
        NereusKafkaMappedRuntimeConfiguration mapped = mapper.map(
                storage,
                kafkaClusterId,
                brokerId,
                brokerEpoch,
                runtimeInstanceId,
                kafkaVersion,
                nereusBuild,
                javaVersion);
        exactBridges.ownedPartitions().configureCompaction(storage, nereusBuild);
        BookKeeper ownedBookKeeper = createBookKeeper(storage);
        ObjectStoreProvider provider;
        try {
            provider = switch (mapped.objectProviderToken()) {
                case NereusKafkaRuntimeConfigurationMapper.S3_PROVIDER_TOKEN ->
                    new S3CompatibleObjectStoreProvider();
                default -> throw new IllegalStateException(
                        "mapped an unsupported Nereus object provider");
            };
        } catch (Throwable failure) {
            closeAfterFailure(ownedBookKeeper, failure);
            throw failure;
        }
        NereusKafkaClock clock = new NereusKafkaClock(time);
        NereusKafkaObjectWalRuntimeContext context =
                new NereusKafkaObjectWalRuntimeContext(
                        provider,
                        emptySecretResolver(),
                        Objects.requireNonNull(
                                scheduler, "scheduler").scheduledExecutorService(),
                        Objects.requireNonNull(
                                exactBridges.recoveryStateFactory(),
                                "recoveryStateFactory"),
                        clock,
                        () -> CompletableFuture.completedFuture(null),
                        bookKeeperContext(storage, ownedBookKeeper));
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
                                storage.rollout().readinessTimeout()),
                        Optional.of(new NereusKafkaCompactionContext(
                                mapped.compaction(),
                                exactBridges.ownedPartitions())),
                        Optional.of(new NereusKafkaMaintenanceContext(
                                mapped.maintenance(),
                                exactBridges.ownedPartitions())));
        try {
            NereusKafkaRuntime runtime =
                    NereusKafkaObjectWalRuntimeFactory.createActivated(
                            mapped.runtime(), context, activation);
            return ownedBookKeeper == null
                    ? runtime
                    : new NereusKafkaOwnedProviderRuntime(runtime, ownedBookKeeper);
        } catch (Throwable failure) {
            closeAfterFailure(ownedBookKeeper, failure);
            throw failure;
        }
    }

    private static BookKeeper createBookKeeper(
            NereusKafkaStorageConfig storage
    ) {
        if (storage.bookKeeper().isEmpty()) {
            return null;
        }
        NereusKafkaBookKeeperConfig configured =
                storage.bookKeeper().orElseThrow();
        ClientConfiguration client = new ClientConfiguration()
                .setMetadataServiceUri(
                        storage.core().bookKeeperMetadataServiceUri()
                                .orElseThrow()
                                .toASCIIString())
                .setClientConnectTimeoutMillis(
                        Math.toIntExact(configured.operationTimeout().toMillis()))
                .setAddEntryTimeout(ceilSeconds(configured.operationTimeout()))
                .setReadEntryTimeout(ceilSeconds(configured.operationTimeout()));
        try {
            return BookKeeper.forConfig(client).build();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "interrupted while creating the BookKeeper client", failure);
        } catch (Exception failure) {
            throw new IllegalStateException(
                    "failed to create the BookKeeper client", failure);
        }
    }

    private static Optional<NereusKafkaBookKeeperWalRuntimeContext>
            bookKeeperContext(
                    NereusKafkaStorageConfig storage,
                    BookKeeper client
    ) {
        if (client == null) {
            return Optional.empty();
        }
        NereusKafkaBookKeeperConfig configured =
                storage.bookKeeper().orElseThrow();
        BookKeeperBrokerReadiness readiness = new BookKeeperBrokerReadiness(
                configured.readinessEpoch(),
                new Checksum(
                        ChecksumType.SHA256,
                        configured.readinessSha256()),
                configured.persistentBrokerCount());
        BookKeeperBrokerReadinessProvider readinessProvider =
                new BookKeeperBrokerReadinessProvider() {
                    @Override
                    public CompletableFuture<BookKeeperBrokerReadiness>
                            requireBookKeeperPrimaryWalReadiness() {
                        return CompletableFuture.completedFuture(readiness);
                    }

                    @Override
                    public Optional<BookKeeperBrokerReadiness>
                            currentBookKeeperPrimaryWalReadiness() {
                        return Optional.of(readiness);
                    }
                };
        return Optional.of(new NereusKafkaBookKeeperWalRuntimeContext(
                client,
                readinessProvider,
                passwordProvider(configured)));
    }

    private static BookKeeperPasswordProvider passwordProvider(
            NereusKafkaBookKeeperConfig configured
    ) {
        String expectedReference =
                configured.passwordFile().toUri().toASCIIString();
        return reference -> {
            BookKeeperSecretRef exact =
                    Objects.requireNonNull(reference, "reference");
            if (!exact.reference().equals(expectedReference)
                    || !exact.identityVersion().equals(configured.passwordVersion())) {
                throw new IllegalArgumentException(
                        "BookKeeper password reference does not match the typed Kafka configuration");
            }
            try {
                byte[] password = Files.readAllBytes(configured.passwordFile());
                if (password.length > 64 * 1024) {
                    java.util.Arrays.fill(password, (byte) 0);
                    throw new IllegalArgumentException(
                            "BookKeeper password file exceeds 64 KiB");
                }
                return password;
            } catch (IOException failure) {
                throw new IllegalStateException(
                        "failed to read the configured BookKeeper password file", failure);
            }
        };
    }

    private static int ceilSeconds(Duration timeout) {
        return Math.toIntExact(Math.max(
                1L,
                Math.addExact(timeout.toMillis(), 999L) / 1_000L));
    }

    private static void closeAfterFailure(
            AutoCloseable resource,
            Throwable failure
    ) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Throwable closeFailure) {
            failure.addSuppressed(closeFailure);
        }
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
