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

import kafka.server.KafkaConfig;

import org.apache.kafka.server.config.NereusKafkaConfigs;
import org.apache.kafka.server.config.NereusKafkaStorageConfig;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.protocol.KafkaProtocolCellIdentity;
import com.nereusstream.kafka.bookkeeper.read.KafkaObjectMaterializedPrefixV1.ReadFailure;
import com.nereusstream.kafka.bookkeeper.read.KafkaObjectMaterializedPrefixV1.UnresolvedRead;
import com.nereusstream.metadata.oxia.v2.retention.OxiaCellProviderScopeRegistryV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.storage.object.materialization.M5CellProviderProfileV2;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.GenerationObject;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession.VerifiedObjectConsumption;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession.VerifiedReadCleanupException;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.read.BindingReadRouteV1.FailureClass;
import com.nereusstream.storage.object.s3.S3C1ObjectProviderTransport;
import com.nereusstream.storage.object.s3.S3C1ProviderException;
import com.nereusstream.storage.object.s3.S3M5PhysicalNamespaceAuthorityV2;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/** Reopens one already-admitted S3 namespace and credential scope for one Kafka Cell. */
final class NereusKafkaObjectProvider implements AutoCloseable {
    private static final String ACCESS_KEY = "aws_access_key_id";
    private static final String SECRET_KEY = "aws_secret_access_key";
    private static final String SESSION_TOKEN = "aws_session_token";

    private final OxiaCellProviderScopeRegistryV2.Admission admission;
    private final S3C1ObjectProviderTransport transport;
    private final C1ObjectProviderSession session;
    private final OxiaQuotaTargetDeleteStoreV2 physicalQuota;
    private final ThreadPoolExecutor fetchReads;
    private final Duration shutdownTimeout;
    private final ArrayDeque<PendingRead> readCleanup = new ArrayDeque<>();

    private record PendingRead(VerifiedReadCleanupException failure, CompletableFuture<Void> drain) { }

    private NereusKafkaObjectProvider(OxiaCellProviderScopeRegistryV2.Admission admission,
            S3C1ObjectProviderTransport transport, C1ObjectProviderSession session,
            OxiaQuotaTargetDeleteStoreV2 physicalQuota, KafkaConfig config) {
        this.admission = admission;
        this.transport = transport;
        this.session = session;
        this.physicalQuota = physicalQuota;
        var storage = config.nereusKafkaStorageConfig();
        var fetch = storage.fetch();
        shutdownTimeout = storage.rollout().shutdownDrainTimeout();
        fetchReads = new ThreadPoolExecutor(fetch.executorThreads(), fetch.executorThreads(), 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(fetch.executorQueueCapacity()), task -> {
                    var thread = new Thread(task, "nereus-object-fetch-" + config.brokerId());
                    thread.setDaemon(true);
                    return thread;
                });
    }

    static M5CellProviderProfileV2 profile(KafkaConfig config, KafkaProtocolCellIdentity cell) throws IOException {
        var core = config.nereusKafkaStorageConfig().core();
        if (core.objectProvider().filter("s3"::equals).isEmpty()) {
            throw new IllegalArgumentException("native Kafka Object requires the qualified s3 provider");
        }
        var path = requiredPath(config, NereusKafkaConfigs.OBJECT_CAPABILITY_FILE_CONFIG);
        M5CellProviderProfileV2 profile;
        try (var in = Files.newInputStream(path)) {
            profile = M5CellProviderProfileV2.decode(CanonicalBytes.copyOf(
                    in.readNBytes(M5CellProviderProfileV2.MAX_BYTES + 1)));
        }
        if (!profile.protocolCell().equals(cell)
                || !profile.adapterVersion().equals(S3C1ObjectProviderTransport.ADAPTER_VERSION)
                || !profile.providerConfigurationSha256().equals(configurationSha256(config))) {
            throw new IllegalArgumentException("native Kafka Object capability belongs to another Cell or provider configuration");
        }
        return profile;
    }

    static NereusKafkaObjectProvider open(KafkaConfig config, KafkaProtocolCellIdentity cell,
            OxiaPhysicalMetadataNamespaceV2 namespace) throws Exception {
        var profile = profile(config, cell);
        var core = config.nereusKafkaStorageConfig().core();
        var timeout = config.nereusKafkaStorageConfig().lifecycle().recoveryTimeout();
        var credentials = credentials(requiredPath(config, NereusKafkaConfigs.OBJECT_CREDENTIALS_FILE_CONFIG),
                profile.references().credentialScope());
        var builder = S3Client.builder().region(Region.of(core.objectRegion().orElseThrow()))
                .forcePathStyle(core.objectPathStyleAccess())
                .overrideConfiguration(settings -> settings.apiCallTimeout(timeout))
                .credentialsProvider(credentials);
        core.objectEndpoint().ifPresent(builder::endpointOverride);
        var client = builder.build();
        try {
            var bucket = core.objectBucket().orElseThrow();
            var expected = profile.physicalBinding().physicalNamespace();
            var physical = S3M5PhysicalNamespaceAuthorityV2.connect(client, bucket, expected);
            var admission = namespace.openCellProviderScopes(physical)
                    .thenCompose(registry -> registry.reopen(profile)).toCompletableFuture()
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            var physicalQuota = namespace.openAuthorityRoute(physical)
                    .thenCompose(route -> route.quota().snapshot().thenApply(ignored -> route))
                    .toCompletableFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            var transport = S3C1ObjectProviderTransport.admitVersionMatchDeleteV1(
                    client, bucket, expected.serviceIdentity().value(), true, profile.maximumListPageKeys(),
                    profile.references().allowedEncryptionScope());
            var capability = transport.capabilities();
            if (profile.maximumObjectBytes() > capability.maximumObjectBytes()
                    || profile.maximumRangeBytes() > capability.maximumRangeBytes()
                    || profile.maximumVersionTokenBytes() > transport.deleteCapabilities().maximumVersionTokenBytes()) {
                throw new IllegalArgumentException("native Kafka Object capability exceeds the actual provider limits");
            }
            var session = new C1ObjectProviderSession(transport, profile.scopeId(), profile.exclusiveNamespacePrefix(),
                    profile.maximumObjectBytes(), Math.min(profile.maximumRangeBytes(), 65_536));
            return new NereusKafkaObjectProvider(admission, transport, session, physicalQuota, config);
        } catch (Exception | Error failure) {
            try {
                client.close();
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    static Sha256Digest configurationSha256(NereusKafkaStorageConfig.Core core) throws IOException {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeUTF("NEREUS_KAFKA_S3_CONFIG_V1");
            out.writeUTF(core.objectProvider().orElseThrow());
            out.writeUTF(core.objectBucket().orElseThrow());
            out.writeUTF(core.objectRegion().orElseThrow());
            // The registered physical namespace, rather than an endpoint alias, identifies the bucket instance.
            out.writeBoolean(core.objectPathStyleAccess());
            out.writeUTF(S3C1ObjectProviderTransport.ADAPTER_VERSION);
        }
        return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
    }

    static Sha256Digest configurationSha256(KafkaConfig config) throws IOException {
        var core = config.nereusKafkaStorageConfig().core();
        var s3 = configurationSha256(core);
        if (core.profile() != NereusKafkaStorageConfig.Profile.OBJECT_WAL_SYNC_OBJECT) return s3;
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeUTF("NEREUS_KAFKA_OBJECT_WAL_CONFIG_V1");
            out.write(s3.bytes().toByteArray());
            out.writeUTF(NereusKafkaObjectWalKms.keyIdentity(config));
            // The same mount and key name on a different Vault endpoint are not the admitted wrapping route.
            out.writeUTF(NereusKafkaObjectWalKms.endpoint(config).toASCIIString());
        }
        return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
    }

    private static Path requiredPath(KafkaConfig config, String key) {
        String value = config.getString(key);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("native Kafka Object requires " + key);
        return Path.of(value);
    }

    private static StaticCredentialsProvider credentials(Path file, String scope) {
        Map<String, String> properties = ProfileFile.builder().content(file)
                .type(ProfileFile.Type.CREDENTIALS).build().profile(scope).orElseThrow(
                        () -> new IllegalArgumentException("native Kafka Object credential scope is absent"))
                .properties();
        if (properties.keySet().equals(Set.of(ACCESS_KEY, SECRET_KEY))) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(
                    properties.get(ACCESS_KEY), properties.get(SECRET_KEY)));
        }
        if (properties.keySet().equals(Set.of(ACCESS_KEY, SECRET_KEY, SESSION_TOKEN))) {
            return StaticCredentialsProvider.create(AwsSessionCredentials.create(
                    properties.get(ACCESS_KEY), properties.get(SECRET_KEY), properties.get(SESSION_TOKEN)));
        }
        throw new IllegalArgumentException("native Kafka Object credentials must be explicit for the admitted scope");
    }

    C1ObjectProviderSession session() {
        return session;
    }

    /** The WalRun owns the narrower session; the Cell owns the shared S3 transport. */
    C1ObjectProviderSession openWalRunProvider(String prefix, long maximumBodyBytes, int maximumPrefixBytes) {
        return openWalRunProvider(prefix, maximumBodyBytes, maximumPrefixBytes, null);
    }

    C1ObjectProviderSession openWalRunProvider(String prefix, long maximumBodyBytes, int maximumPrefixBytes,
            NereusKafkaObjectWalDispatchFence dispatch) {
        var profile = profile();
        if (!prefix.startsWith(profile.exclusiveNamespacePrefix() + "/object-wal/")
                || maximumBodyBytes > profile.maximumObjectBytes()
                || maximumPrefixBytes > profile.maximumRangeBytes()) {
            throw new IllegalArgumentException("native WalRun provider exceeds its admitted Cell");
        }
        var scoped = dispatch == null ? transport : new NereusKafkaObjectWalTransport(transport, dispatch);
        return new C1ObjectProviderSession(scoped, profile.scopeId(), prefix, maximumBodyBytes, maximumPrefixBytes);
    }

    VerifiedObjectConsumption<CanonicalBytes> readVersion(ObjectIdentity identity, CanonicalBytes version)
            throws IOException {
        int length = Math.toIntExact(identity.bodyLength());
        try {
            return session.consumeVerifiedObject(identity, Optional.of(version),
                    input -> CanonicalBytes.copyOf(input.readNBytes(length)));
        } catch (VerifiedReadCleanupException failure) {
            var drain = new CompletableFuture<Void>();
            synchronized (this) {
                readCleanup.addLast(new PendingRead(failure, drain));
            }
            throw new UnresolvedRead(failure, drain.thenApply(ignored -> null));
        }
    }

    CanonicalBytes readForFetch(GenerationObject object) throws IOException {
        try {
            return readVersion(object.identity(), object.immutableProviderVersionToken().orElseThrow()).value();
        } catch (UnresolvedRead failure) {
            throw failure;
        } catch (S3C1ProviderException failure) {
            var kind = switch (failure.kind()) {
                case NOT_FOUND -> FailureClass.MISSING;
                case OUTCOME_UNKNOWN, RETRYABLE -> FailureClass.UNAVAILABLE;
                case INTEGRITY -> FailureClass.CORRUPT_OR_FORMAT;
                case CAPACITY, DEFINITIVE_CONFLICT, UNSUPPORTED_OPERATION -> FailureClass.NOT_ELIGIBLE;
            };
            throw new ReadFailure(kind, "exact Kafka Object Fetch failed", failure);
        } catch (IOException failure) {
            throw new ReadFailure(FailureClass.CORRUPT_OR_FORMAT, "exact Kafka Object Fetch response failed validation", failure);
        }
    }

    M5CellProviderProfileV2 profile() {
        return admission.profile();
    }

    OxiaQuotaTargetDeleteStoreV2 physicalQuota() {
        return physicalQuota;
    }

    CompletionStage<Void> requireCurrent() {
        return admission.requireCurrent();
    }

    void beginDrain() {
        session.drain();
        fetchReads.shutdown();
    }

    Executor fetchExecutor() {
        return fetchReads;
    }

    void drainFetches() {
        beginDrain();
        try {
            if (!fetchReads.awaitTermination(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Kafka Object Fetch executor has not drained");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka Object Fetch drain was interrupted", failure);
        }
        reconcileOriginalReads();
    }

    /** Reconcile only the original SDK response; no subsequent GET can prove the former one closed. */
    synchronized void reconcileOriginalReads() {
        while (!readCleanup.isEmpty()) {
            var pending = readCleanup.getFirst();
            try {
                pending.failure().reconcileCleanup();
                readCleanup.removeFirst();
                pending.drain().complete(null);
            } catch (IOException failure) {
                throw new UncheckedIOException("Kafka Object still owns an original GET response", failure);
            }
        }
    }

    @Override
    public void close() {
        drainFetches();
        synchronized (this) {
            session.close();
            transport.close();
        }
    }
}
