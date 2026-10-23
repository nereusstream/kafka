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
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperDeleteIdentityReaderV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperDeleteObservationAuthorityV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteCellBudgetV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteIntentV2;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2;
import com.nereusstream.storage.object.gc.DeleteObservationContextV2;
import com.nereusstream.storage.object.gc.DeleteObservationAuthorityVerifierV2;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExactExternalIdentityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2;
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1;

import org.apache.kafka.server.util.KafkaScheduler;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;

/** One owned native physical cut's GC attempts. Original candidates survive observer failure and retry. */
final class NereusKafkaM5GcScopeV1 {
    private final NereusKafkaM5SourceScanV1 scan;
    private final Sha256Digest task;
    private final java.util.Map<PhysicalResourceIdV2, M5BookKeeperNativeCreateClientV2> clients;
    private final M5BookKeeperDeleteCellBudgetV2 budget;
    private final ExactMetadataTransactionStoreV1 rawFacts;
    private final Function<ExactMetadataTransactionStoreV1, CompletionStage<OxiaQuotaTargetDeleteStoreV2>> routes;
    private final KafkaScheduler scheduler;
    private final Executor openings;
    private final Runnable requireDispatch;
    private final List<Target> targets = new ArrayList<>();
    private CompletableFuture<Void> running;
    private volatile boolean closing;
    private CompletableFuture<Void> delay;
    private NereusKafkaM5GcInputsV1 inputs;

    NereusKafkaM5GcScopeV1(NereusKafkaM5SourceScanV1 scan, Sha256Digest task,
            NereusKafkaM5GcInputsV1 inputs, java.util.Map<PhysicalResourceIdV2, M5BookKeeperNativeCreateClientV2> clients,
            M5BookKeeperDeleteCellBudgetV2 budget, ExactMetadataTransactionStoreV1 rawFacts,
            Function<ExactMetadataTransactionStoreV1, CompletionStage<OxiaQuotaTargetDeleteStoreV2>> routes,
            KafkaScheduler scheduler, Executor openings, Runnable requireDispatch) {
        this.scan = scan;
        this.task = task;
        this.inputs = inputs;
        this.clients = java.util.Map.copyOf(clients);
        this.budget = budget;
        this.rawFacts = rawFacts;
        this.routes = routes;
        this.scheduler = scheduler;
        this.openings = openings;
        this.requireDispatch = requireDispatch;
        inputs.resources().stream().sorted().forEach(resource -> targets.add(new Target(resource)));
    }

    Sha256Digest scopeIdentity() {
        return task;
    }

    synchronized CompletionStage<Void> run(Sha256Digest expectedTask) {
        if (!task.equals(expectedTask)) return CompletableFuture.failedFuture(new IllegalArgumentException("GC scope belongs to another original Task"));
        if (closing) return CompletableFuture.failedFuture(new IllegalStateException("native GC scope is closing"));
        if (running != null && !running.isDone()) return running.copy();
        CompletionStage<Void> work = scan.requireCurrent().thenCompose(ignored -> settleCompleted());
        for (var target : targets) work = work.thenCompose(ignored -> target.run());
        running = work.toCompletableFuture();
        return running.copy();
    }

    private CompletionStage<Void> settleCompleted() {
        CompletionStage<Void> work = CompletableFuture.completedFuture(null);
        for (var completed : inputs.completed()) {
            var value = M5TargetDeleteStoredValueV2.decode(completed);
            work = work.thenCompose(ignored -> {
                CompletionStage<Void> compact = value.compactDone().isPresent() ? CompletableFuture.completedFuture(null)
                        : new M5TargetDeleteAuthorityCoordinatorV1(scan.physical()).compactDone(completed).thenAccept(result -> {
                            if (!result.exactTerminalIsAuthoritative())
                                throw new IllegalStateException("original completed Task compaction remains unresolved");
                        });
                return compact.thenCompose(done -> scan.physical().quota().settle(value.resource())).thenAccept(result -> {
                    if (result != M5GcQuotaCoordinatorV2.Result.SETTLED)
                        throw new IllegalStateException("completed native Task settlement remains unresolved");
                });
            });
        }
        return work;
    }

    synchronized void cancelWait() {
        closing = true;
        if (delay != null) delay.completeExceptionally(new IllegalStateException("native GC scope is closing"));
    }

    CompletionStage<Void> closeClient() {
        return CompletableFuture.runAsync(() -> {
            try {
                Exception closeFailure = null;
                for (var client : new java.util.HashSet<>(clients.values())) {
                    try {
                        client.close();
                    } catch (Exception failure) {
                        if (closeFailure == null) closeFailure = failure;
                        else closeFailure.addSuppressed(failure);
                    }
                }
                if (closeFailure != null) throw closeFailure;
            } catch (Exception failure) {
                throw new java.util.concurrent.CompletionException(failure);
            }
        }, openings);
    }

    private CompletionStage<Void> current() {
        if (closing) return CompletableFuture.failedFuture(new IllegalStateException("native GC scope is closing"));
        return scan.requireCurrent();
    }

    private synchronized CompletionStage<Void> waitGrace(long duration) {
        if (closing) return CompletableFuture.failedFuture(new IllegalStateException("native GC scope is closing"));
        delay = new CompletableFuture<>();
        var original = delay;
        scheduler.scheduleOnce("nereus-m5-gc-grace-" + task.toHex(), () -> original.complete(null), duration);
        return original.copy();
    }

    private CompletionStage<NereusKafkaM5GcInputsV1> rescan() {
        return current().thenCompose(ignored -> inputs.isReplacement() ? NereusKafkaM5GcInputsV1.captureReplacement(scan)
                : NereusKafkaM5GcInputsV1.capture(scan, task)).thenApply(actual -> {
                    if (!actual.scopeIdentity().equals(inputs.scopeIdentity())
                            || !actual.lifecycleAuthority().equals(inputs.lifecycleAuthority())
                            || !actual.policy().equals(inputs.policy()))
                        throw new IllegalStateException("original native GC Task or policy changed during grace");
                    inputs = actual;
                    return actual;
                });
    }

    private final class Target {
        private final PhysicalResourceIdV2 resource;
        private final M5BookKeeperNativeCreateClientV2 client;
        private final KafkaBookKeeperDeleteObservationAuthorityV2 gc;
        private final KafkaBookKeeperDeleteIdentityReaderV2 external;
        private Sha256Digest attempt;
        private OxiaQuotaTargetDeleteStoreV2 route;
        private M5TargetDeleteAuthorityCoordinatorV1 mutations;
        private VersionedValue original;
        private TargetDeleteAuthorityStateV1 initialState;
        private DeleteObservationContextV2 context;
        private List<VersionedValue> epochFacts = List.of();
        private M5TargetDeleteAuthorityCoordinatorV1.EpochGraceSession grace;
        private M5TargetDeleteAuthorityCoordinatorV1.EpochGraceReady ready;
        private DeleteEligibilitySnapshotV2 eligibility;
        private VersionedValue marked;
        private M5TargetDeleteAuthorityCoordinatorV1.MutationResultV1 refreshResult;
        private VersionedValue qualified;
        private VersionedValue fenced;
        private ExactExternalIdentityV1 identity;
        private VersionedValue intent;
        private M5BookKeeperNativeDeleteIntentV2 bound;
        private M5BookKeeperDeleteCellBudgetV2.Result dispatched;
        private VersionedValue done;
        private VersionedValue compact;
        private Optional<com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteAuthorityV2.Snapshot> previousEpoch;

        private Target(PhysicalResourceIdV2 resource) {
            this.resource = resource;
            client = clients.get(resource);
            var handle = inputs.handle(resource);
            gc = new KafkaBookKeeperDeleteObservationAuthorityV2(client, handle, UUID.randomUUID());
            external = new KafkaBookKeeperDeleteIdentityReaderV2(client, handle);
            var nonce = UUID.randomUUID();
            attempt = Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(16)
                    .putLong(nonce.getMostSignificantBits()).putLong(nonce.getLeastSignificantBits()).array()));
        }

        private CompletionStage<Void> initialize() {
            if (route != null) return CompletableFuture.completedFuture(null);
            var facts = new ExactMetadataTransactionStoreV1() {
                public CompletionStage<Optional<VersionedValue>> read(String key) {
                    return inputs.readOnlyFacts(gc.readOnlyFacts(rawFacts)).read(key);
                }
                public CompletionStage<MutationOutcome> compareAndSet(Optional<VersionedValue> old, String key, CanonicalBytes candidate) {
                    return CompletableFuture.failedFuture(new UnsupportedOperationException("native GC facts are read only"));
                }
                public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
                    return CompletableFuture.completedFuture(TransactionOutcome.UNSUPPORTED);
                }
                public boolean supportsAtomicMultiKeyTransactions() {
                    return false;
                }
            };
            return routes.apply(facts).thenAccept(actual -> {
                route = actual;
                mutations = new M5TargetDeleteAuthorityCoordinatorV1(route, new DeleteObservationAuthorityVerifierV2() {
                    public CompletionStage<Void> requireCurrent(PhysicalResourceIdV2 target, DeleteObservationContextV2 observed) {
                        return current().thenCompose(ignored -> gc.requireCurrent(target, observed));
                    }
                    public CompletionStage<Void> requirePredecessorFenced(PhysicalResourceIdV2 target,
                            DeleteObservationContextV2 prior, DeleteObservationContextV2 next) {
                        return current().thenCompose(ignored -> gc.requirePredecessorFenced(target, prior, next));
                    }
                }, external);
            });
        }

        private CompletionStage<Void> mark() {
            if (ready != null) return CompletableFuture.completedFuture(null);
            if (grace != null) return grace.confirmMark();
            return route.read(resource.authorityKey()).thenCompose(actual -> {
                original = actual.orElseThrow();
                var stored = M5TargetDeleteStoredValueV2.decode(original);
                initialState = stored.state();
                if (initialState == TargetDeleteAuthorityStateV1.DELETE_DONE_V1) {
                    compact = original;
                    return CompletableFuture.<Void>completedFuture(null);
                }
                if (initialState == TargetDeleteAuthorityStateV1.OPEN_V1) requireDispatch.run();
                var authority = stored.fullAuthority().orElseThrow();
                if (initialState == TargetDeleteAuthorityStateV1.READ_FENCED_V1) attempt = authority.readFence().orElseThrow().attemptIdSha256();
                if (initialState == TargetDeleteAuthorityStateV1.DELETE_INTENT_V1) attempt = authority.deleteIntent().orElseThrow().deleteAttemptIdSha256();
                var prior = initialState == TargetDeleteAuthorityStateV1.DELETE_INTENT_V1
                        ? Optional.of(com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1.dispatchContext(authority))
                        : authority.readFence().map(value -> value.observationContext());
                return reconcilePriorCell().thenCompose(ignored -> client.deleteAuthority(inputs.handle(resource)).read()).thenCompose(epoch -> {
                    previousEpoch = epoch;
                    return gc.claim(route, previousEpoch);
                }).thenCompose(epoch -> gc.observe(prior.map(value -> Math.addExact(value.observationEpoch(), 1)).orElse(1L), prior))
                        .thenCompose(observed -> {
                            context = observed;
                            return gc.readOnlyFacts(rawFacts).read(gc.factKey()).thenCompose(fact -> {
                                var value = fact.orElseThrow();
                                if (!NereusKafkaM5GcInputsV1.fact(value).equals(context.coordinatorOwner()))
                                    throw new IllegalStateException("native GC epoch changed before its initial scan");
                                epochFacts = List.of(value);
                                if (producer().initialObservations().stream().anyMatch(row -> row.disposition()
                                        != M5RetentionRecordsV1.ReferenceDispositionV1.ABSENT))
                                    throw new IllegalStateException("native GC initial observation retains a real reference or hold");
                                return floors().thenCompose(snapshot -> producer().assembler(gc.readOnlyFacts(rawFacts))
                                        .captureEpochReferences(request(snapshot)));
                            });
                        }).thenCompose(initial -> {
                            grace = mutations.prepareEpochGrace(original, initial, inputs.policy().graceDurationMillis());
                            return grace.confirmMark();
                        });
            });
        }

        private CompletionStage<Void> reconcilePriorCell() {
            if (initialState != TargetDeleteAuthorityStateV1.DELETE_INTENT_V1) return CompletableFuture.completedFuture(null);
            return budget.snapshot().thenCompose(snapshot -> {
                if (snapshot.reservations().stream().noneMatch(value -> value.resource().equals(resource.sha256())))
                    return CompletableFuture.completedFuture(null);
                var nativeAuthority = client.deleteAuthority(inputs.handle(resource));
                return nativeAuthority.readIntent().thenCompose(stored -> {
                    var prior = stored.orElseThrow();
                    return gc.reconcileBoundCellDeleteAbsence(route, budget, original, prior).thenAccept(result -> {
                        if (result.reservationRetained())
                            throw new IllegalStateException("original native Cell UNKNOWN is not reconciled");
                    });
                });
            });
        }

        private NereusKafkaM5GcReferencesV1 producer() {
            return new NereusKafkaM5GcReferencesV1(inputs, resource, epochFacts);
        }

        private CompletionStage<M5RetentionRecordsV1.RetentionFloorSnapshotV1> floors() {
            var fact = NereusKafkaM5GcInputsV1.fact(scan.exactAuthority());
            long generation = M5BindingAuthorityCodecV1.decodeAuthority(scan.exactAuthority().canonicalStoredBytes()).authorityGeneration();
            var request = new M5RetentionEvidenceAssemblerV1.FloorSnapshotRequestV1(inputs.sourceCut().identity(),
                    inputs.sourceCut().coverage().domain(), generation, inputs.retainedNativeFrontier(),
                    Sha256Digest.hash(inputs.policy().encode()), fact, fact);
            return producer().assembler(gc.readOnlyFacts(rawFacts)).buildFloorSnapshot(request);
        }

        private M5RetentionEvidenceAssemblerV1.ReferenceProofRequestV1 request(M5RetentionRecordsV1.RetentionFloorSnapshotV1 snapshot) {
            var source = NereusKafkaM5GcInputsV1.fact(scan.exactAuthority());
            return new M5RetentionEvidenceAssemblerV1.ReferenceProofRequestV1(inputs.sourceCut().identity(),
                    inputs.isReplacement() ? M5RetentionRecordsV1.ReferenceTargetKindV1.READABLE_SOURCE
                            : M5RetentionRecordsV1.ReferenceTargetKindV1.UNSELECTED_OUTPUT, resource.sha256(), inputs.coverage(resource),
                    source, NereusKafkaM5GcInputsV1.fact(inputs.manifestAuthority()), source,
                    snapshot.snapshotRootSha256(), inputs.releases(resource), source, source, source, context.capability(), 0, 0, Optional.of(context));
        }

        private CompletionStage<Void> qualify() {
            if (eligibility != null) return CompletableFuture.completedFuture(null);
            return grace.requireElapsed().thenCompose(elapsed -> {
                ready = elapsed;
                return rescan();
            }).thenCompose(actual -> route.read(resource.authorityKey()).thenCompose(value -> {
                marked = value.orElseThrow();
                return floors();
            })).thenCompose(snapshot -> producer().assembler(gc.readOnlyFacts(rawFacts))
                    .buildEpochReferenceFreeProof(request(snapshot), ready).thenApply(proof -> {
                        var member = inputs.member(resource, snapshot, proof);
                        eligibility = new DeleteEligibilitySnapshotV2(resource, inputs.reason(),
                                Math.addExact(M5TargetDeleteAuthorityCodecV1.decodeAuthority(marked.canonicalStoredBytes()).authorityRevision(), 1),
                                context.capability(), inputs.members().get(resource).authority(), List.of(member));
                        return null;
                    }));
        }

        private VersionedValue exact(M5TargetDeleteAuthorityCoordinatorV1.MutationResultV1 result) {
            if (!result.exactCandidateIsAuthoritative())
                throw new IllegalStateException("original native GC CAS remains unresolved: " + result.outcome());
            return result.observed().orElseThrow();
        }

        private CompletionStage<Void> admit() {
            if (fenced != null || intent != null) return CompletableFuture.completedFuture(null);
            return route.read(resource.authorityKey()).thenCompose(current -> {
                if (marked == null) marked = current.orElseThrow();
                if (initialState == TargetDeleteAuthorityStateV1.READ_FENCED_V1) {
                    return refresh().thenAccept(value -> fenced = value);
                }
                if (initialState == TargetDeleteAuthorityStateV1.DELETE_INTENT_V1) {
                    return refresh().thenAccept(value -> intent = value);
                }
                CompletionStage<Void> qualification = qualified != null ? CompletableFuture.completedFuture(null)
                        : mutations.qualifyEligibility(marked, eligibility, ready).thenAccept(value -> qualified = exact(value));
                return qualification.thenCompose(ignored -> mutations.prepareIdentityRead(qualified, attempt, context, ready))
                        .thenAccept(value -> fenced = exact(value));
            });
        }

        private CompletionStage<VersionedValue> refresh() {
            if (refreshResult != null) {
                return route.read(resource.authorityKey()).thenCompose(actual -> {
                    if (actual.filter(value -> value.canonicalStoredBytes().equals(refreshResult.exactCandidate())).isPresent())
                        return ready.requireAdmittedCurrent(resource, ready.mark()).thenApply(ignored -> actual.orElseThrow());
                    if (!actual.equals(Optional.of(marked)))
                        return CompletableFuture.failedFuture(new IllegalStateException("original native refresh candidate changed or remains unresolved"));
                    return refreshOriginal();
                });
            }
            return refreshOriginal();
        }

        private CompletionStage<VersionedValue> refreshOriginal() {
            var work = initialState == TargetDeleteAuthorityStateV1.READ_FENCED_V1
                    ? mutations.refreshIdentityRead(marked, context, eligibility, ready)
                    : mutations.refreshDispatch(marked, context, eligibility, ready);
            return work.thenApply(result -> {
                refreshResult = result;
                return exact(result);
            });
        }

        private CompletionStage<Void> bind() {
            if (intent != null) return CompletableFuture.completedFuture(null);
            CompletionStage<ExactExternalIdentityV1> capture = identity == null
                    ? external.capture(M5TargetDeleteAuthorityCodecV1.decodeAuthority(fenced.canonicalStoredBytes()))
                    : CompletableFuture.completedFuture(identity);
            return capture.thenCompose(actual -> {
                identity = actual;
                return mutations.bindDeleteIntent(fenced, identity, attempt, ready);
            }).thenAccept(value -> intent = exact(value));
        }

        private CompletionStage<Void> delete() {
            return external.rereadExact(M5TargetDeleteAuthorityCodecV1.decodeAuthority(intent.canonicalStoredBytes()).externalIdentity().orElseThrow())
                    .thenCompose(observed -> {
                        if (observed == com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExternalIdentityObservationV1.ABSENT_EXACT_V1) {
                            CompletionStage<Void> release = bound == null ? CompletableFuture.completedFuture(null)
                                    : gc.reconcileBoundCellDeleteAbsence(route, budget, intent, bound).thenAccept(result -> {
                                        if (result.reservationRetained()) throw new IllegalStateException("original native Cell UNKNOWN remains retained");
                                    });
                            return release.thenCompose(ignored -> mutations.completeAbsent(intent)).thenAccept(value -> done = exact(value));
                        }
                        requireDispatch.run();
                        if (dispatched != null)
                            return CompletableFuture.failedFuture(new IllegalStateException("original native delete remains UNKNOWN; its Cell hold is retained"));
                        return gc.bindIntent(route, intent, ready).thenCompose(actual -> {
                            bound = actual;
                            requireDispatch.run();
                            return gc.dispatchBoundDelete(route, budget, intent, bound, ready);
                        }).thenCompose(result -> {
                            dispatched = result;
                            if (result.reservationRetained())
                                return CompletableFuture.failedFuture(new IllegalStateException("original native delete remains UNKNOWN; its Cell hold is retained"));
                            return mutations.completeAbsent(intent).thenAccept(value -> done = exact(value));
                        });
                    });
        }

        private CompletionStage<Void> finish() {
            if (compact != null && M5TargetDeleteStoredValueV2.decode(compact).compactDone().isEmpty()) {
                done = compact;
                compact = null;
            }
            CompletionStage<Void> work = compact != null ? CompletableFuture.completedFuture(null)
                    : mutations.compactDone(done).thenAccept(value -> {
                        if (!value.exactTerminalIsAuthoritative()) throw new IllegalStateException("original native compact DONE remains unresolved");
                        compact = value.observed().orElseThrow();
                    });
            return work.thenCompose(ignored -> route.quota().settle(resource)).thenAccept(result -> {
                if (result != M5GcQuotaCoordinatorV2.Result.SETTLED)
                    throw new IllegalStateException("native compact DONE quota settlement is unresolved: " + result);
            });
        }

        private CompletionStage<Void> run() {
            if (compact != null) return finish();
            if (done != null) return finish();
            return current().thenCompose(ignored -> initialize()).thenCompose(ignored -> mark())
                    .thenCompose(ignored -> compact != null ? finish() : waitGrace(inputs.policy().graceDurationMillis())
                            .thenCompose(waited -> qualify()).thenCompose(qualifiedResult -> admit())
                            .thenCompose(admitted -> bind()).thenCompose(boundResult -> delete()).thenCompose(deleted -> finish()));
        }
    }
}
