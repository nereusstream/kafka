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
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Caps;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Disposition;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSemanticCompactorV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.InputBatch;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Member;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2.BirthResult;
import com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.PhysicalDeleteTargetV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterEnrollmentV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1;
import com.nereusstream.storage.object.materialization.M5BytePreservingMaterializerV1;
import com.nereusstream.storage.object.materialization.M5CellProviderProfileV2;
import com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCoordinatorV1;
import com.nereusstream.storage.object.materialization.M5MaterializationKeysV1;
import com.nereusstream.storage.object.materialization.M5MaterializationObjectSessionV1;
import com.nereusstream.storage.object.materialization.M5MaterializationObjectSessionV1.Candidate;
import com.nereusstream.storage.object.materialization.M5MaterializationPlannerV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.GenerationObject;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PayloadKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.SemanticValidationProof;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.ValidatedGeneration;
import com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtection;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import com.nereusstream.storage.object.retention.M5MaterializationWorkerCoordinatorV1.Admission;
import com.nereusstream.storage.object.retention.M5MaterializationWorkerCoordinatorV1.Proposal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Plans byte-preserving Object output from a retained, already validated native Kafka source cut. */
final class NereusKafkaObjectMaterialization {
    private static final Sha256Digest KEEP_KAFKA_ENCODING = digest("NEREUS_KAFKA_NATIVE_BATCH_ENCODING_V1");
    private static final Sha256Digest SHA256_AND_KAFKA_CRC = digest("NEREUS_KAFKA_SHA256_AND_KAFKA_CRC32C_V1");

    private NereusKafkaObjectMaterialization() { }

    record Prepared(MaterializationPlan plan, List<Candidate> candidates,
            M5BytePreservingMaterializerV1.SourceReader sourceReader, Optional<SemanticValidationProof> semantic) { }

    record Registered(Prepared prepared, Admission worker, M5PendingOutputInventoryV2 pending,
            CanonicalControlMetadataStore metadata, String pendingKey, int partition) { }

    record Stored(Registered task, List<GenerationObject> outputs, List<M5ObjectBirthCreditV3> credits) { }

    record Verified(Stored stored, ValidatedGeneration generation, List<SourceProtectionIdentity> protections) { }

    /** The reserve-all phase stopped without dispatching any of this attempt's Object writes. */
    static final class QuotaExhaustedException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private QuotaExhaustedException() {
            super("Kafka Object birth reservations exceed the admitted physical metadata quota");
        }
    }

    static Prepared prepare(NereusKafkaNativeInputV2.Captured capture, Proposal proposal,
            M5CellProviderProfileV2 provider, Caps caps, long decodedBytes) {
        var worker = proposal.identity();
        if (!worker.protocolCellSha256().equals(provider.protocolCellSha256())
                || !worker.providerScopeSha256().equals(provider.scopeId().digest())) {
            throw new IllegalArgumentException("Kafka materialization worker belongs to another Object Cell or scope");
        }
        if (capture.predecessorSelector().fallbackSetSha256().isPresent()) {
            throw new IllegalStateException("first Kafka Object generation requires the raw BK predecessor");
        }
        var sources = capture.extents().stream().collect(Collectors.toUnmodifiableMap(
                SourceExtent::sourceIdentitySha256, Function.identity()));
        if (sources.values().stream().anyMatch(source -> source.kind() != SourceKind.BOOKKEEPER_LEDGER
                || source.canonicalLength() >= provider.maximumObjectBytes())) {
            throw new IllegalStateException("Kafka byte-preserving materialization requires complete bounded BK sources");
        }
        var batches = capture.batches().stream().collect(Collectors.groupingBy(InputBatch::sourceIdentitySha256));
        M5BytePreservingMaterializerV1.SourceReader reader = source -> {
            capture.requireOpen();
            if (!source.equals(sources.get(source.sourceIdentitySha256()))) {
                throw new IOException("Kafka Object build attempted to read an unretained BK source");
            }
            return sourceBytes(source, batches.getOrDefault(source.sourceIdentitySha256(), List.of()));
        };
        var prepared = capture.topicPolicy().config().compact
                ? prepareSemantic(capture, worker, provider, reader, caps, decodedBytes)
                : preparePreserving(capture, worker, provider, reader);
        if (prepared.candidates().stream().anyMatch(candidate ->
                candidate.descriptor().identity().bodyLength() > provider.maximumObjectBytes())) {
            throw new IllegalStateException("Kafka materialized Object exceeds the admitted provider limit");
        }
        return prepared;
    }

    private static Prepared preparePreserving(NereusKafkaNativeInputV2.Captured capture,
            com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope worker,
            M5CellProviderProfileV2 provider, M5BytePreservingMaterializerV1.SourceReader reader) {
        var cut = NereusKafkaCompactionPlanV2.sourceCut(capture, worker, provider.scopeId().digest());
        var plan = new M5MaterializationPlannerV1().plan(cut, new M5MaterializationPlannerV1.PlanningPolicy(
                PayloadKind.KAFKA_BATCH_PRESERVING_V1, true, digest(provider.references().allowedEncryptionScope()),
                KEEP_KAFKA_ENCODING, SHA256_AND_KAFKA_CRC,
                List.of(IndexKind.OFFSET_OR_POSITION, IndexKind.PAYLOAD_LOCATOR, IndexKind.TIMESTAMP, IndexKind.CHECKSUM_COVERAGE),
                provider.exclusiveNamespacePrefix()));
        try {
            var output = new M5BytePreservingMaterializerV1().build(plan, reader);
            return new Prepared(plan, Stream.concat(output.payloadCandidates().stream(), output.indexCandidates().stream()).toList(),
                    reader, Optional.empty());
        } catch (IOException failure) {
            throw new UncheckedIOException("Kafka Object candidate could not read its exact retained source", failure);
        }
    }

    private static Prepared prepareSemantic(NereusKafkaNativeInputV2.Captured capture,
            com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope worker,
            M5CellProviderProfileV2 provider, M5BytePreservingMaterializerV1.SourceReader reader, Caps caps, long decodedBytes) {
        var plan = NereusKafkaCompactionPlanV2.buildForTask(capture, worker, caps, decodedBytes);
        capture.recoveryState().requirePlan(plan);
        var result = new KafkaSemanticCompactorV1().compactForObjectCell(plan, provider.exclusiveNamespacePrefix(),
                digest(provider.references().allowedEncryptionScope()));
        var candidate = result.candidate();
        if (candidate.dispositions().stream().anyMatch(row -> row.disposition() == Disposition.DROP_EXPIRED_TOMBSTONE)) {
            throw new IllegalStateException("native Kafka Object compaction cannot retire an expired tombstone");
        }
        return new Prepared(candidate.materializationPlan(),
                Stream.concat(Stream.of(candidate.payloadCandidate()), candidate.indexCandidates().stream()).toList(),
                reader, Optional.of(result.semanticProof()));
    }

    static Registered register(Prepared prepared, M5CellProviderProfileV2 provider, int partition,
            CanonicalControlMetadataStore metadata, Runnable requireCurrent, Supplier<Admission> admitWorker) {
        var plan = prepared.plan();
        var workerIdentity = plan.sourceCut().identity();
        if (!workerIdentity.protocolCellSha256().equals(provider.protocolCellSha256())
                || !workerIdentity.providerScopeSha256().equals(provider.scopeId().digest())) {
            throw new IllegalArgumentException("Kafka Object task belongs to another provider scope");
        }
        var registration = new M5MaterializationCoordinatorV1(metadata, partition, workerIdentity.binding());
        requireCurrent.run();
        var registered = registration.register(plan);
        if (registered != PublicationOutcome.APPLIED_EXACT && registered != PublicationOutcome.EXISTING_EXACT) {
            throw new IllegalStateException("Kafka Object task registration is unresolved: " + registered);
        }
        var candidates = prepared.candidates();
        long bytes = candidates.stream().mapToLong(candidate -> candidate.descriptor().identity().bodyLength())
                .reduce(0, Math::addExact);
        requireCurrent.run();
        var reserved = new M5MaterializationAdmissionV1(metadata, workerIdentity.protocolCellSha256())
                .reserve(plan, bytes, candidates.size());
        if (reserved != M5MaterializationAdmissionV1.Outcome.APPLIED_EXACT
                && reserved != M5MaterializationAdmissionV1.Outcome.EXISTING_EXACT) {
            throw new IllegalStateException("Kafka Object task Cell capacity is unavailable or unresolved: " + reserved);
        }
        requireCurrent.run();
        // Task and Cell writes can advance the Binding revision through their own mutation tickets.
        // Refresh that revision only while the selector and the intended next worker identity are unchanged.
        var worker = admitWorker.get();
        if (!worker.worker().identity().equals(workerIdentity)) {
            throw new IllegalStateException("Kafka Object registration did not adopt its original worker");
        }
        var pending = M5PendingOutputInventoryV2.prepare(plan, provider, worker.worker());
        var key = new M5MaterializationKeysV1(partition, workerIdentity.binding()).pendingOutputs(plan.taskIdSha256());
        var encoded = pending.encode();
        metadata.putIfAbsent(key, encoded);
        if (!metadata.get(key).equals(Optional.of(encoded))) {
            throw new IllegalStateException("Kafka Object pending inventory is unresolved");
        }
        worker.requireCurrentPlan(plan);
        requireCurrent.run();
        return new Registered(prepared, worker, pending, metadata, key, partition);
    }

    /** Writes only original inventoried candidates. Any unresolved result leaves the original birth and Cell charges. */
    static Stored store(Registered task, NereusKafkaObjectProvider provider, Runnable requireCurrent, Duration timeout)
            throws Exception {
        var prepared = task.prepared();
        var plan = prepared.plan();
        var profile = provider.profile();
        if (!task.pending().profile().equals(profile)) {
            throw new IllegalArgumentException("Kafka Object output has another admitted Provider profile");
        }
        task.pending().requirePlan(plan);
        var quota = provider.physicalQuota().quota();
        var worker = task.worker();
        var scope = await(quota.snapshot(), timeout).head().scope();
        var slots = task.pending().outputs();
        var candidates = prepared.candidates();
        if (slots.size() != candidates.size()) {
            throw new IllegalStateException("Kafka Object output does not cover the complete pending inventory");
        }
        for (int index = 0; index < slots.size(); index++) {
            var slot = slots.get(index);
            if (!slot.objectKey().equals(candidates.get(index).descriptor().identity().key())) {
                throw new IllegalStateException("Kafka Object output order differs from the pending inventory");
            }
            requireWriteAuthority(task, provider, requireCurrent, timeout);
            var outcome = await(quota.reserveBirth(worker.worker(), slot), timeout);
            if (outcome == BirthResult.EXHAUSTED) throw new QuotaExhaustedException();
            if (outcome != BirthResult.RESERVED && outcome != BirthResult.WRITE_ARMED && outcome != BirthResult.ASSIGNED) {
                throw new IllegalStateException("Kafka Object birth reservation is unavailable or unresolved: " + outcome);
            }
        }

        var session = new M5MaterializationObjectSessionV1(provider.session(), 2, profile.maximumListPageKeys(),
                1024L * profile.maximumListPageKeys(), 1024);
        var outputs = new ArrayList<GenerationObject>(slots.size());
        var credits = new ArrayList<M5ObjectBirthCreditV3>(slots.size());
        var enrollment = objectWriterEnrollment(profile);
        for (int index = 0; index < slots.size(); index++) {
            String key = M5ObjectBirthCreditV3.key(scope, worker.worker(), slots.get(index));
            var candidate = candidates.get(index);
            var output = storeOriginal(task, provider, requireCurrent, timeout, quota, session, key, candidate);
            var credit = await(quota.readBirth(key), timeout).orElseThrow();
            if (credit.state() != M5ObjectBirthCreditV3.State.ASSIGNED || !credit.resource().equals(Optional.of(output.resource()))) {
                throw new IllegalStateException("Kafka Object birth did not retain its exact immutable version");
            }
            requireWriteAuthority(task, provider, requireCurrent, timeout);
            registerOriginalMember(task, provider, credit, enrollment, timeout);
            outputs.add(output.object());
            credits.add(credit);
        }
        requireWriteAuthority(task, provider, requireCurrent, timeout);
        return new Stored(task, List.copyOf(outputs), List.copyOf(credits));
    }

    private static ProofBoundWriterEnrollmentV1 objectWriterEnrollment(M5CellProviderProfileV2 profile)
            throws IOException {
        try (var input = NereusKafkaObjectMaterialization.class.getResourceAsStream("NereusKafkaObjectMaterialization.class")) {
            var implementation = Sha256Digest.hash(CanonicalBytes.copyOf(
                    Objects.requireNonNull(input, "Kafka Object materialization implementation").readAllBytes()));
            return new ProofBoundWriterEnrollmentV1(List.of(ProofBoundWriterClassV1.values()),
                    profile.providerConfigurationSha256(), implementation, Sha256Digest.hash(profile.encode()));
        }
    }

    private static void registerOriginalMember(Registered task, NereusKafkaObjectProvider provider,
            M5ObjectBirthCreditV3 credit, ProofBoundWriterEnrollmentV1 enrollment, Duration timeout) throws Exception {
        var profile = provider.profile();
        var physical = provider.physicalQuota();
        var resource = credit.resource().orElseThrow();
        var target = PhysicalDeleteTargetV1.create(resource);
        var coordinator = new M5TargetDeleteAuthorityCoordinatorV1(physical);
        var existing = await(coordinator.inspect(resource.authorityKey()), timeout);
        if (existing.isEmpty()) {
            var initial = M5TargetDeleteAuthorityStateMachineV1.open(target, enrollment, Sha256Digest.hash(credit.encode()));
            if (!await(coordinator.create(initial), timeout).exactCandidateIsAuthoritative()) {
                throw new IllegalStateException("Kafka Object birth authority remains unresolved");
            }
        } else if (existing.orElseThrow().fullAuthority().filter(value ->
                value.state() == TargetDeleteAuthorityStateV1.OPEN_V1 && value.target().equals(target)
                        && value.writerEnrollment().equals(enrollment)).isEmpty()) {
            throw new IllegalStateException("Kafka Object birth authority was fenced or belongs to another writer");
        }
        var worker = task.worker().worker();
        var member = new Member(PositionDomain.KAFKA_OFFSET, profile.scopeId().digest(), worker.identity().binding());
        await(physical.members().register(resource, profile.protocolCellSha256(), member,
                profile.providerConfigurationSha256(), worker.nativeWorkerIdentitySha256()), timeout);
        await(physical.readResourceFact(resource), timeout);
    }

    static Verified protectAndVerify(Stored stored, NereusKafkaObjectProvider provider,
            Runnable requireCurrent, Duration timeout) throws Exception {
        var task = stored.task();
        var prepared = task.prepared();
        var plan = prepared.plan();
        var predecessor = plan.sourceCut().predecessorSelector();
        long generation = Math.addExact(predecessor.sourceGeneration(), 1);
        long epoch = Math.addExact(predecessor.readAdmissionEpoch(), 1);
        var protections = plan.sourceCut().sources().stream().map(source -> new SourceProtectionIdentity(
                source.sourceIdentitySha256(), generation, epoch, generation, predecessor.capability()))
                .sorted(Comparator.comparing(value -> value.sourceIdentitySha256().toHex())).toList();
        var coordinator = new M4ReadControlCoordinatorV1(task.metadata(), task.partition(), predecessor.binding());
        for (var protection : protections) {
            requireWriteAuthority(task, provider, requireCurrent, timeout);
            var outcome = coordinator.createProtection(new SourceProtection(predecessor.binding(), protection,
                    ProtectionState.PROTECTED, Optional.empty(), Optional.empty()));
            if (outcome != M4ReadControlCoordinatorV1.Outcome.APPLIED
                    && outcome != M4ReadControlCoordinatorV1.Outcome.EXISTING_EXACT) {
                throw new IllegalStateException("Kafka Object fallback protection is unresolved: " + outcome);
            }
        }
        var payloads = stored.outputs().stream().filter(GenerationObject::payload).toList();
        var indexes = stored.outputs().stream().filter(output -> !output.payload()).toList();
        var exactObjects = stored.outputs().stream().collect(Collectors.toUnmodifiableMap(
                GenerationObject::identity, value -> value.immutableProviderVersionToken().orElseThrow()));
        var dataReader = new M5MaterializationValidatorV1.MaterializationDataReader() {
            @Override
            public M5MaterializationValidatorV1.VerifiedObjectRead readObject(ObjectIdentity identity)
                    throws IOException {
                var version = exactObjects.get(identity);
                if (version == null) throw new IOException("Kafka materialization validation received another Object");
                var read = provider.readVersion(identity, version);
                return new M5MaterializationValidatorV1.VerifiedObjectRead(read.value(), read.immutableVersionToken());
            }

            @Override
            public CanonicalBytes readSource(SourceExtent source) throws IOException {
                return prepared.sourceReader().readExact(source);
            }
        };
        var validator = new M5MaterializationValidatorV1();
        var validated = prepared.semantic().isPresent()
                ? validator.validateSemantic(plan, predecessor, protections, payloads, indexes, dataReader, prepared.semantic().orElseThrow())
                : validator.validate(plan, predecessor, protections, payloads, indexes, dataReader);
        requireWriteAuthority(task, provider, requireCurrent, timeout);
        return new Verified(stored, validated, protections);
    }

    private record StoredObject(GenerationObject object, PhysicalResourceIdV2.ObjectVersion resource) { }

    private static StoredObject storeOriginal(Registered task, NereusKafkaObjectProvider provider,
            Runnable requireCurrent, Duration timeout, M5GcQuotaCoordinatorV2 quota,
            M5MaterializationObjectSessionV1 session, String key, Candidate candidate) throws Exception {
        requireWriteAuthority(task, provider, requireCurrent, timeout);
        var original = candidate.descriptor();
        var armed = await(quota.armBirth(key, original.identity()), timeout);
        if (armed != BirthResult.WRITE_ARMED && armed != BirthResult.ASSIGNED) {
            throw new IllegalStateException("Kafka Object original write is not durably armed: " + armed);
        }
        requireWriteAuthority(task, provider, requireCurrent, timeout);
        var version = armed == BirthResult.ASSIGNED
                ? await(quota.readBirth(key), timeout).orElseThrow().resource().orElseThrow().immutableIdentity().bytes()
                : session.createExact(task.prepared().plan(), List.of(candidate)).exactObjects().get(0)
                        .immutableProviderVersionToken().orElseThrow();
        provider.readVersion(original.identity(), version);
        var resource = new PhysicalResourceIdV2.ObjectVersion(
                provider.profile().physicalBinding().physicalNamespace(),
                CanonicalUtf8.fromString(original.identity().key()),
                PhysicalResourceIdV2.ObjectIdentityKind.IMMUTABLE_VERSION, CanonicalUtf8.fromBytes(version.toByteArray()));
        if (await(quota.assignBirth(key, resource), timeout) != BirthResult.ASSIGNED) {
            throw new IllegalStateException("Kafka Object birth assignment is unresolved");
        }
        return new StoredObject(new GenerationObject(original.ordinal(), original.indexKind(), original.coverage(),
                original.identity(), Optional.of(version)), resource);
    }

    static void requireWriteAuthority(Registered task, NereusKafkaObjectProvider provider,
            Runnable requireCurrent, Duration timeout) throws Exception {
        requireCurrent.run();
        await(provider.requireCurrent(), timeout);
        task.worker().requireCurrentPlan(task.prepared().plan());
        if (!task.metadata().get(task.pendingKey()).equals(Optional.of(task.pending().encode()))) {
            throw new IllegalStateException("Kafka Object pending inventory has changed");
        }
        requireCurrent.run();
    }

    private static <T> T await(CompletionStage<T> operation, Duration timeout) throws Exception {
        return operation.toCompletableFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static CanonicalBytes sourceBytes(SourceExtent source, List<InputBatch> batches) throws IOException {
        var output = ByteBuffer.allocate(Math.toIntExact(source.canonicalLength()));
        for (int ordinal = 0; ordinal < batches.size(); ordinal++) {
            var batch = batches.get(ordinal);
            if (batch.sourceBatchOrdinal() != ordinal || batch.canonicalBody().length() > output.remaining()) {
                throw new IOException("Kafka materialization input lost a native batch boundary");
            }
            output.put(batch.canonicalBody().toByteArray());
        }
        if (output.hasRemaining()) {
            throw new IOException("Kafka materialization input has incomplete BK coverage");
        }
        // The existing M5 materializer and validator both verify the exact native extent digest.
        return CanonicalBytes.copyOf(output.array());
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalUtf8.fromString(value).bytes());
    }
}
