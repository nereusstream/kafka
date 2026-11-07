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

import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperNativeFallbackRetirementV2;
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperClosedHistoryRecoveryV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/** One actual Owner's original Object selection, until the old reads, checkpoint, and native GC settle. */
final class NereusKafkaObjectRetirement {
    private final BindingReadSelector original;
    private final long coveredEnd;
    private final boolean reclaimBookKeeper;
    private final long observedAt = System.nanoTime();
    private KafkaBookKeeperNativeFallbackRetirementV2 actor;
    private boolean admissionClosed;
    private KafkaBookKeeperNativeFallbackRetirementV2.Result released;
    private KafkaBookKeeperPartitionV1.ObjectBookKeeperSourceTransfer transfer;
    private boolean persisted;
    private boolean settled;
    private volatile CompletableFuture<Void> pending;

    NereusKafkaObjectRetirement(BindingReadSelector original, NereusKafkaObjectRecovery.Selected verified) {
        if (original.mode() != SelectorMode.PREFERRED_WITH_FALLBACK
                || !original.selectedViewSha256().equals(verified.prefix().manifestSha256())
                || verified.prefix().originalFallbackSources().stream().anyMatch(source ->
                        source.firstFallbackCapableReadAdmissionEpoch() != original.readAdmissionEpoch())) {
            throw new IllegalArgumentException("native Object retirement lacks one exact owned fallback epoch");
        }
        this.original = original;
        coveredEnd = verified.prefix().coverage().exclusiveEnd();
        reclaimBookKeeper = verified.prefix().rawFallbackEquivalent();
    }

    private NereusKafkaObjectRetirement(BindingReadSelector original, long coveredEnd) {
        this.original = original;
        this.coveredEnd = coveredEnd;
        reclaimBookKeeper = true;
        persisted = true;
    }

    static NereusKafkaObjectRetirement resumePersisted(BindingReadSelector current,
            KafkaBookKeeperClosedHistoryRecoveryV1.ObjectPrefix recovered) {
        recovered.scope().requireSelection(recovered.prefix(), current);
        if (!recovered.prefix().rawFallbackEquivalent()) {
            throw new IllegalStateException("native Object cleanup needs the exact byte-preserving replacement");
        }
        return new NereusKafkaObjectRetirement(current, recovered.scope().prefixEnd());
    }

    boolean sameSelection(BindingReadSelector observed) {
        return observed.binding().equals(original.binding()) && observed.ownerEpoch() == original.ownerEpoch()
                && observed.selectedViewSha256().equals(original.selectedViewSha256());
    }

    boolean settled() {
        return settled;
    }

    void advance(NereusKafkaOwnedProviderRuntime.Opened owner, BindingReadSelector selector, NereusKafkaObjectProvider provider,
            Duration sourceGrace, Duration timeout, Executor control) {
        if (!sameSelection(selector)) throw new IllegalStateException("native Object retirement changed its original manifest");
        if (!observeCompleted() || settled) return;
        var storage = owner.storage();
        if (persisted) {
            start(owner, () -> reclaim(owner).thenRunAsync(() -> {
                settled = true;
                actor = null;
                released = null;
                transfer = null;
            }, control));
        } else if (actor == null) {
            if (System.nanoTime() - observedAt < sourceGrace.toNanos()) return;
            start(owner, () -> storage.verifySelectedObjectPrefix(original.selectedViewSha256()).thenRunAsync(() -> {
                var current = owner.nativeM5BindingRoute().readSelector().orElseThrow();
                if (!sameSelection(current) || current.mode() != SelectorMode.PREFERRED_WITH_FALLBACK
                        || current.readAdmissionEpoch() != original.readAdmissionEpoch()) {
                    throw new IllegalStateException("native Object fallback changed during provider verification");
                }
                actor = storage.beginNativeObjectFallbackRetirement(original.selectedViewSha256());
            }, control));
        } else if (!admissionClosed) {
            start(owner, () -> storage.closeNativeObjectFallbackAdmission(actor, control)
                    .thenRunAsync(() -> admissionClosed = true, control));
        } else if (released == null) {
            start(owner, () -> storage.finishNativeObjectFallbackRetirement(actor, control)
                    .thenAcceptAsync(result -> {
                        released = result;
                        if (!reclaimBookKeeper) settled = true;
                    }, control));
        } else if (transfer == null) {
            if (retainedCheckpointAvailable(storage)) transfer = storage.transferRetiredObjectBookKeeperPrefix(released);
        } else if (transfer.durableCheckpoint().isPresent()) {
            start(owner, () -> NereusKafkaOwnedProviderRuntime.SelectedRecoveryMetadata
                    .persistObjectScope(owner, provider, transfer, timeout).thenRunAsync(() -> persisted = true, control));
        }
    }

    private boolean retainedCheckpointAvailable(KafkaBookKeeperPartitionV1 storage) {
        var input = storage.nativeInputSources();
        return input.recoveryCheckpoint().filter(checkpoint -> {
            var run = checkpoint.vector().runBinding().runId();
            return input.sources().sealedRoots().stream().noneMatch(root ->
                    root.kafkaStartOffset() < coveredEnd && root.runId().equals(run));
        }).isPresent();
    }

    CompletionStage<Void> drain() {
        var accepted = pending;
        return accepted == null ? CompletableFuture.completedFuture(null) : accepted.handle((ignored, failure) -> null);
    }

    private boolean observeCompleted() {
        if (pending == null) return true;
        if (!pending.isDone()) return false;
        var originalWork = pending;
        pending = null;
        originalWork.join();
        return true;
    }

    private void start(NereusKafkaOwnedProviderRuntime.Opened owner, Supplier<CompletionStage<Void>> work) {
        // Keep the operation with the actual Owner even while this Broker scheduler services a different Binding.
        pending = owner.ownInputOperation(work).toCompletableFuture();
    }

    private static CompletionStage<Void> reclaim(NereusKafkaOwnedProviderRuntime.Opened owner) {
        return owner.openNativeM5SourceScan().thenCompose(scan -> CompletableFuture.completedFuture(null)
                .thenCompose(ignored -> scan.reclaimReplacedSources())
                .handle((ignored, failure) -> scan.closeAsync().handle((closed, closeFailure) -> {
                    if (failure != null) {
                        if (closeFailure != null && closeFailure != failure) failure.addSuppressed(closeFailure);
                        throw new java.util.concurrent.CompletionException(failure);
                    }
                    if (closeFailure != null) throw new java.util.concurrent.CompletionException(closeFailure);
                    return (Void) null;
                })).thenCompose(result -> result));
    }
}
