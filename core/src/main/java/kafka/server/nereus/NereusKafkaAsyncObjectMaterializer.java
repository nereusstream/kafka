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

import org.apache.kafka.server.config.NereusKafkaStorageConfig;
import org.apache.kafka.server.util.KafkaScheduler;

import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperReadCellBudgetV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperRunSourceV2;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionAdmissionV1.CellCaps;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1;
import com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCoordinatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Runs one bounded native BK-to-Object job per Broker without using its append/rollover executor. */
final class NereusKafkaAsyncObjectMaterializer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(NereusKafkaAsyncObjectMaterializer.class);
    private static final long MIB = 1L << 20;

    private final NereusKafkaObjectProvider provider;
    private final NereusKafkaStorageConfig config;
    private final ExecutorService control;
    private final ScheduledFuture<?> scheduled;
    private final Map<NereusKafkaOwnedProviderRuntime.Opened, Job> owners = new LinkedHashMap<>();
    private CompletableFuture<Void> active = CompletableFuture.completedFuture(null);
    private boolean draining;

    NereusKafkaAsyncObjectMaterializer(NereusKafkaObjectProvider provider, NereusKafkaStorageConfig config,
            KafkaScheduler scheduler, int brokerId) {
        this.provider = provider;
        this.config = config;
        // One invocation owns this executor at a time. Its dependent metadata/SDK callbacks use the same executor.
        control = Executors.newSingleThreadExecutor(task -> {
            var thread = new Thread(task, "nereus-kafka-object-materializer-" + brokerId);
            thread.setDaemon(true);
            return thread;
        });
        long period = config.retentionCompaction().retentionCheckInterval().toMillis();
        scheduled = scheduler.schedule("nereus-kafka-object-materialization", this::tick, period, period);
    }

    synchronized void own(NereusKafkaOwnedProviderRuntime.Opened owner) {
        if (!draining && !owners.containsKey(owner)) owners.put(owner, null);
    }

    synchronized void forget(NereusKafkaOwnedProviderRuntime.Opened owner) {
        owners.remove(owner);
    }

    private synchronized void tick() {
        if (draining || !active.isDone() || owners.isEmpty()) return;
        var owner = owners.keySet().iterator().next();
        var job = owners.remove(owner);
        owners.put(owner, job);
        active = CompletableFuture.completedFuture(null).thenComposeAsync(ignored ->
                owner.ownInputOperation(() -> advance(owner, job)), control)
                .handle((ignored, failure) -> {
                    if (failure != null) LOG.warn("Native Kafka Object materialization did not finish its current task", failure);
                    return (Void) null;
                }).toCompletableFuture();
    }

    private CompletionStage<Void> advance(NereusKafkaOwnedProviderRuntime.Opened owner, Job job) {
        if (!owner.asyncMaterializationReady()) return CompletableFuture.completedFuture(null);
        provider.reconcileOriginalReads();
        if (job != null) return finish(owner, job);
        return candidate(owner).map(candidate -> owner.captureM5Inputs(candidate.readBudget(), candidate.bounds(),
                candidate.cell(), candidate.allowance(), control).thenCompose(captured -> {
                    var original = new Job(captured);
                    remember(owner, original);
                    return finish(owner, original);
                })).orElseGet(() -> CompletableFuture.completedFuture(null));
    }

    private CompletionStage<Void> finish(NereusKafkaOwnedProviderRuntime.Opened owner, Job job) {
        return job.advance(provider, config.lifecycle().recoveryTimeout())
                .thenRun(() -> remember(owner, null));
    }

    private synchronized void remember(NereusKafkaOwnedProviderRuntime.Opened owner, Job job) {
        if (owners.containsKey(owner)) owners.put(owner, job);
    }

    private Optional<Candidate> candidate(NereusKafkaOwnedProviderRuntime.Opened owner) {
        var raw = owner.storage().nativeReadSources();
        if (raw.sealedRoots().stream().noneMatch(root -> root.kafkaStartOffset() < root.kafkaEndOffsetExclusive().orElseThrow())) {
            return Optional.empty();
        }
        var route = owner.nativeM5BindingRoute();
        var selector = route.readSelector().orElseThrow();
        if (selector.mode() != SelectorMode.PREFERRED_ONLY || selector.admissionState() != AdmissionState.ADMITTING
                || !selector.selectedViewSha256().equals(raw.selectedViewSha256())) return Optional.empty();
        var profile = provider.profile();
        var metadata = route.materializationMetadata(profile.protocolCellSha256());
        var materialization = new M5MaterializationAdmissionV1(metadata, profile.protocolCellSha256()).read();
        var cell = owner.nativeM5CompactionAdmission().read();
        if (materialization.isEmpty() || cell.isEmpty()) return Optional.empty();
        var tasks = new M5MaterializationCoordinatorV1(metadata, raw.activeRoot().partitionId(), selector.binding());
        // An interrupted predecessor can retain an original registration before a worker grant became visible.
        // Never create a new task for this Binding while such an original reservation still needs reconciliation.
        if (materialization.orElseThrow().reservations().stream()
                .anyMatch(item -> tasks.readPlan(item.taskIdSha256()).isPresent())) return Optional.empty();
        return Optional.of(Candidate.admitted(raw.activeRoot().providerScopeId(), selector.binding(),
                config.retentionCompaction(), cell.orElseThrow().caps(), materialization.orElseThrow().caps()));
    }

    synchronized void beginDrain() {
        draining = true;
        scheduled.cancel(false);
    }

    @Override
    public void close() throws Exception {
        final CompletableFuture<Void> accepted;
        synchronized (this) {
            beginDrain();
            accepted = active;
        }
        long timeout = config.rollout().shutdownDrainTimeout().toMillis();
        accepted.get(timeout, TimeUnit.MILLISECONDS);
        control.shutdown();
        if (!control.awaitTermination(timeout, TimeUnit.MILLISECONDS)) {
            throw new IllegalStateException("native Kafka Object materialization executor did not drain");
        }
    }

    private record Candidate(KafkaBookKeeperReadCellBudgetV2 readBudget, KafkaBookKeeperRunSourceV2.Bounds bounds,
            CellCaps cell, NereusKafkaNativeInputV2.CaptureAllowance allowance) {
        private static Candidate admitted(com.nereusstream.storage.api.bookkeeper.CellProviderScopeId providerScope,
                com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity binding,
                NereusKafkaStorageConfig.RetentionCompaction config, CellCaps cell, M5MaterializationAdmissionV1.Caps output) {
            long bytes = Math.min(Math.min(config.compactionTaskMaxSourceBytes(), cell.maximumDirtyBytes()),
                    Math.min(output.maximumSourceBytes(), 128 * MIB));
            int batches = (int) Math.min(cell.maximumInputBatches(), KafkaCompactionRecordsV1.MAX_BATCHES);
            int records = (int) Math.min(Math.min(config.compactionTaskMaxRecords(), cell.maximumRecords()),
                    KafkaCompactionRecordsV1.MAX_RECORDS);
            long decoded = Math.min(config.compactionDecodeMaxUncompressedBytes(), 256 * MIB);
            var bounds = new KafkaBookKeeperRunSourceV2.Bounds(131072, batches, records, 256 * MIB, bytes, decoded);
            // Byte-preserving output creates no key history, tombstone or transaction planning tables.
            var plan = new KafkaCompactionRecordsV1.Caps(bytes, batches, records, 1, 1,
                    cell.maximumOutputBytes(), cell.maximumIndexBytes(), 1, 1);
            var allowance = new NereusKafkaNativeInputV2.CaptureAllowance(plan, 0, 0,
                    cell.maximumProviderOperations(), 0, cell.maximumMetadataOperations(), cell.maximumResponseUnknowns());
            var share = KafkaBookKeeperReadCellBudgetV2.Usage.forRunSource(bounds);
            return new Candidate(new KafkaBookKeeperReadCellBudgetV2(providerScope, share, Map.of(binding, share)),
                    bounds, cell, allowance);
        }
    }

    private static final class Job {
        private final NereusKafkaNativeInputV2.Captured input;
        private NereusKafkaObjectMaterialization.Prepared prepared;
        private NereusKafkaObjectMaterialization.Registered registered;
        private NereusKafkaObjectMaterialization.Verified verified;

        private Job(NereusKafkaNativeInputV2.Captured input) {
            this.input = input;
        }

        private CompletionStage<Void> advance(NereusKafkaObjectProvider provider, Duration timeout) {
            var preparation = prepared == null ? input.prepareObjectMaterialization(provider).thenAccept(value -> prepared = value)
                    : CompletableFuture.<Void>completedFuture(null);
            return preparation.thenCompose(ignored -> registered == null
                    ? input.registerObjectMaterialization(prepared, provider).thenAccept(value -> registered = value)
                    : CompletableFuture.completedFuture(null))
                    .thenCompose(ignored -> verified == null
                            ? input.storeObjectMaterialization(registered, provider, timeout).thenAccept(value -> verified = value)
                            : CompletableFuture.completedFuture(null))
                    .thenCompose(ignored -> input.publishObjectMaterialization(verified, provider, provider.fetchExecutor(), timeout))
                    .thenCompose(ignored -> input.discard());
        }
    }
}
