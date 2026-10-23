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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1;
import com.nereusstream.metadata.oxia.v2.compaction.OxiaKafkaRunRootAuthorityV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaKafkaBookKeeperM4ReadRouteV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingAuthorityStateV1;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.UUID;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;

/** Owns one original native Binding scan and its cancellation; it grants no reference-free or delete proof. */
final class NereusKafkaM5SourceScanV1 implements AutoCloseable {
    private final NereusKafkaOwnedProviderRuntime.Opened opened;
    private final NereusKafkaOwnedProviderRuntime runtime;
    private CompletableFuture<NereusKafkaM5GcScopeV1> gcOpening;
    private final OxiaKafkaBookKeeperM4ReadRouteV2 route;
    private final OxiaKafkaRunRootAuthorityV2 roots;
    private final BooleanSupplier current;
    private final OxiaQuotaTargetDeleteStoreV2 physical;
    private final M5BookKeeperNativeCreateClientV2 nativeClient;
    private final Sha256Digest attempt;
    private final CompletableFuture<Void> lifetime = new CompletableFuture<>();
    private final Set<CompletableFuture<?>> accepted = ConcurrentHashMap.newKeySet();
    private CompletableFuture<Void> started;
    private CompletableFuture<Void> closing;
    private volatile boolean closed;
    private final KafkaOwnerIdentityV1 expectedOwner;
    private final NereusKafkaM4AdmissionV2 admission;
    private final com.nereusstream.metadata.oxia.v2.retention.OxiaBindingLifecycleMetadataStoreV2 lifecycle;
    private VersionedValue original;
    private CanonicalBytes candidate;
    private VersionedValue fenced;
    private OxiaKafkaRunRootAuthorityV2.OwnerAdmissionFact owner;

    NereusKafkaM5SourceScanV1(NereusKafkaOwnedProviderRuntime.Opened opened,
            OxiaKafkaBookKeeperM4ReadRouteV2 route, OxiaKafkaRunRootAuthorityV2 roots, BooleanSupplier current, KafkaOwnerIdentityV1 expectedOwner,
            OxiaQuotaTargetDeleteStoreV2 physical, M5BookKeeperNativeCreateClientV2 nativeClient, NereusKafkaM4AdmissionV2 admission, NereusKafkaOwnedProviderRuntime runtime,
            com.nereusstream.metadata.oxia.v2.retention.OxiaBindingLifecycleMetadataStoreV2 lifecycle) {
        this.opened = opened;
        this.runtime = runtime;
        this.lifecycle = lifecycle;
        this.admission = admission;
        this.route = route;
        this.roots = roots;
        this.current = current;
        this.expectedOwner = expectedOwner;
        this.physical = physical;
        this.nativeClient = nativeClient;
        var nonce = UUID.randomUUID();
        attempt = Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(16)
                .putLong(nonce.getMostSignificantBits()).putLong(nonce.getLeastSignificantBits()).array()));
    }

    NereusKafkaOwnedProviderRuntime.Opened opened() {
        return opened;
    }

    OxiaKafkaBookKeeperM4ReadRouteV2 route() {
        return route;
    }

    CompletionStage<Optional<VersionedValue>> readLifecycleFact(String key) {
        return requireCurrent().thenCompose(ignored -> lifecycle.read(key));
    }

    OxiaQuotaTargetDeleteStoreV2 physical() {
        return physical;
    }

    M5BookKeeperNativeCreateClientV2 nativeClient() {
        return nativeClient;
    }

    OxiaKafkaRunRootAuthorityV2.OwnerAdmissionFact owner() {
        if (closed || owner == null) throw new IllegalStateException("native source scan is not open");
        return owner;
    }

    CompletionStage<Void> lifetime() {
        return lifetime;
    }

    synchronized CompletionStage<NereusKafkaM5SourceScanV1> start() {
        if (closed) return CompletableFuture.failedFuture(new IllegalStateException("native source scan is closed"));
        if (started != null) throw new IllegalStateException("native source scan was already started");
        started = roots.readOwnerAdmissionFact().thenCompose(observed -> {
            owner = observed.orElseThrow();
            if (!current.getAsBoolean() || owner.admission().closed() || !owner.admission().owner().equals(expectedOwner))
                throw new IllegalStateException("native source scan lacks a current open Owner");
            return lifecycle.reconcileLogicalTrimWrites().thenCompose(ignored -> route.readBindingAuthority());
        }).thenCompose(observed -> {
            if (closed) throw new IllegalStateException("native source scan is closing");
            original = observed.orElseThrow();
            try {
                NereusKafkaM5WriterRegistryV1.requireEnrolled(original, admission);
            } catch (Exception failure) {
                throw new java.util.concurrent.CompletionException("native source scan writer admission changed", failure);
            }
            var authority = M5BindingAuthorityCodecV1.decodeAuthority(original.canonicalStoredBytes());
            candidate = M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.sourceScanSuccessor(authority, attempt));
            return route.logicalTrimCoordinator().beginSourceScan(original, attempt);
        }).thenCompose(result -> {
            if (result.exactAuthority().isEmpty())
                throw new IllegalStateException("original native source-scan fence remains unresolved: " + result.outcome());
            fenced = result.exactAuthority().orElseThrow();
            return requireCurrent();
        }).toCompletableFuture();
        return started.thenApply(ignored -> this);
    }

    CompletionStage<Void> requireCurrent() {
        if (closed || fenced == null || !current.getAsBoolean())
            return CompletableFuture.failedFuture(new IllegalStateException("native source scan is not open"));
        return route.logicalTrimCoordinator().requireSourceScanCurrent(fenced)
                .thenCompose(ignored -> roots.readOwnerAdmissionFact()).thenAccept(actual -> {
                    if (closed || !current.getAsBoolean() || !actual.equals(Optional.of(owner)))
                        throw new IllegalStateException("native source-scan Owner changed");
                });
    }

    VersionedValue exactAuthority() {
        if (closed || fenced == null) throw new IllegalStateException("native source scan is not open");
        return fenced;
    }

    CompletionStage<NereusKafkaUnpublishedScopeV2.Evidence> captureUnpublished(Sha256Digest task) {
        return track(() -> requireCurrent().thenCompose(ignored -> opened.captureNativeM5UnpublishedEvidence(task, this)));
    }

    CompletionStage<NereusKafkaReadScopeV2.ReplacementEvidence> captureReplacement() {
        return track(() -> requireCurrent().thenCompose(ignored -> opened.captureNativeM5ReplacementEvidence(this)));
    }

    private synchronized <T> CompletionStage<T> track(Supplier<CompletionStage<T>> operation) {
        if (closed) return CompletableFuture.failedFuture(new IllegalStateException("native source scan is closed"));
        var work = operation.get().thenCompose(value -> requireCurrent().thenApply(ignored -> value)).toCompletableFuture();
        accepted.add(work);
        work.whenComplete((value, failure) -> accepted.remove(work));
        return work.thenApply(value -> value);
    }

    <T> CompletionStage<T> observe(Supplier<T> operation) {
        return track(() -> CompletableFuture.supplyAsync(() -> {
            if (closed) throw new IllegalStateException("native source scan is closed");
            return operation.get();
        }, opened.nativeM5TaskControl()));
    }

    synchronized CompletionStage<Void> reclaimCancelledTask(Sha256Digest task) {
        return track(() -> {
            if (gcOpening == null) gcOpening = runtime.openNativeM5Gc(this, Optional.of(task)).toCompletableFuture();
            return gcOpening.thenCompose(gc -> gc.run(task));
        });
    }

    synchronized CompletionStage<Void> reclaimReplacedSources() {
        return track(() -> {
            if (gcOpening == null) gcOpening = runtime.openNativeM5Gc(this, Optional.empty()).toCompletableFuture();
            return gcOpening.thenCompose(gc -> gc.run(gc.scopeIdentity()));
        });
    }

    synchronized CompletableFuture<Void> closeAsync() {
        closed = true;
        if (gcOpening != null) gcOpening.thenAccept(NereusKafkaM5GcScopeV1::cancelWait);
        if (closing == null || closing.isCompletedExceptionally()) {
            closing = (started == null ? CompletableFuture.<Void>completedFuture(null) : started.handle((ignored, failure) -> null))
                    .thenCompose(ignored -> CompletableFuture.allOf(accepted.toArray(CompletableFuture[]::new))
                            .handle((done, failure) -> null))
                    .thenCompose(ignored -> gcOpening == null ? CompletableFuture.completedFuture(null)
                            : gcOpening.handle((gc, failure) -> gc).thenCompose(gc -> gc == null
                                    ? CompletableFuture.completedFuture(null) : gc.closeClient()))
                    .thenCompose(ignored -> closePass()).thenRun(() -> lifetime.complete(null)).toCompletableFuture();
        }
        return closing.copy();
    }

    private CompletionStage<Void> closePass() {
        if (candidate == null) return CompletableFuture.completedFuture(null); // no fence was dispatched
        return route.readBindingAuthority().thenCompose(actual -> {
            var exact = actual.orElseThrow();
            if (exact.canonicalStoredBytes().equals(candidate)) {
                return route.logicalTrimCoordinator().abortSourceScan(exact, attempt)
                        .thenCompose(ignored -> confirmClosed());
            }
            if (exact.equals(original)) {
                return route.logicalTrimCoordinator().cancelUnconfirmedSourceScan(original)
                        .thenCompose(ignored -> confirmClosed());
            }
            return confirmClosed();
        });
    }

    private CompletionStage<Void> confirmClosed() {
        return route.readBindingAuthority().thenAccept(actual -> {
            var exact = actual.orElseThrow();
            var value = M5BindingAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
            var before = M5BindingAuthorityCodecV1.decodeAuthority(original.canonicalStoredBytes());
            if (exact.equals(original) || exact.canonicalStoredBytes().equals(candidate)
                    || value.authorityGeneration() <= before.authorityGeneration()
                    || value.state() == BindingAuthorityStateV1.SOURCE_SCAN_FENCED_V2
                            && value.sourceScan().orElseThrow().attemptId().equals(attempt))
                throw new IllegalStateException("original native source-scan cancellation remains unresolved");
        });
    }

    @Override
    public void close() {
        closeAsync().join();
    }
}
