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

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Time;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Caps;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.CleanupPolicy;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.CompactionPlan;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.ControlKind;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.LatestKeyProof;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.LeaderEpochRange;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.ParsedBatch;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Policy;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.ProtocolRoots;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.RecordValue;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.TransactionOutcome;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.TransactionRange;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaRecordBatchCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationSourceCut;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

/** Derives semantic facts from the actual retained native input. No worker, output or publication is admitted here. */
final class NereusKafkaCompactionPlanV2 {
    private NereusKafkaCompactionPlanV2() { }

    static CompactionPlan build(NereusKafkaNativeInputV2.Captured capture, IdentityEnvelope identity, Caps caps, long maximumDecodedBytes) {
        return build(capture, identity, caps, maximumDecodedBytes, false);
    }

    static CompactionPlan buildForTask(NereusKafkaNativeInputV2.Captured capture, IdentityEnvelope identity, Caps caps, long maximumDecodedBytes) {
        return build(capture, identity, caps, maximumDecodedBytes, true);
    }

    private static CompactionPlan build(NereusKafkaNativeInputV2.Captured capture, IdentityEnvelope identity, Caps caps,
            long maximumDecodedBytes, boolean preparingTask) {
        var frontiers = capture.frontiers();
        var topic = capture.topicPolicy();
        var config = topic.config();
        if (!config.compact) throw new IllegalStateException("native M5 topic does not enable compaction");
        if (frontiers.candidateStartOffset() != frontiers.logStartOffset())
            throw new IllegalStateException("native M5 complete key domain does not start at logical trim");
        var protocol = capture.nativeSources().protocol();
        var transactions = transactions(capture, caps);
        var parsed = parse(capture, caps, maximumDecodedBytes);
        long nowMs = Time.SYSTEM.milliseconds();
        if (preparingTask) requireEligible(capture, parsed, nowMs);
        var keys = latestKeys(parsed, transactions, caps);
        var references = protocol.root().references();
        var roots = new ProtocolRoots(references.committedProducerState().contentDigest(),
                references.speculativeProducerQueue().contentDigest(), references.transactionIndex().contentDigest(),
                hash("NEREUS_NATIVE_ABORTED_TRANSACTIONS_V1", out -> writeTransactions(out,
                        transactions.stream().filter(tx -> tx.outcome() == TransactionOutcome.ABORTED).toList())),
                references.leaderEpochIndex().contentDigest(), timestampRoot(parsed),
                references.checkpointVector().contentDigest(), references.activeTail().contentDigest(),
                keyDomainRoot(capture, keys, transactions));
        var policy = new Policy(topic.kafkaFeatureLevel(), (byte) 2,
                config.delete ? CleanupPolicy.COMPACT_DELETE : CleanupPolicy.COMPACT,
                (int) Math.ceil(config.minCleanableRatio * 1_000_000), config.deleteRetentionMs, nowMs,
                topic.generation(), KafkaCompactionRecordsV1.NATIVE_REWRITE_POLICY_VERSION,
                AppInfoParser.getVersion(), topic.compressionPolicy(), caps);
        var cut = new MaterializationSourceCut(identity, capture.predecessorSelector(),
                Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(capture.predecessorSelector())),
                capture.predecessorSelector().selectedViewSha256(), new ProtocolCoverage(PositionDomain.KAFKA_OFFSET,
                        frontiers.candidateStartOffset(), frontiers.candidateEndOffsetExclusive()),
                frontiers.durableOffset(), frontiers.logEndOffset(), frontiers.highWatermark(),
                frontiers.lastStableOffset(), frontiers.logStartOffset(), protocolRoot(capture),
                roots.recoveryCheckpointSha256(), policyRoot(topic),
                hash("NEREUS_NATIVE_KAFKA_OUTPUT_POLICY_V1", out -> {
                    writeText(out, policy.rewritePolicyVersion());
                    writeText(out, policy.nativeKafkaOracleVersion());
                    out.writeInt(policy.kafkaFeatureLevel());
                    out.writeByte(policy.messageMagic());
                    out.writeByte(policy.compressionPolicy().ordinal());
                }), M5MaterializationCodecV1.calculateSourceSetSha256(capture.extents()), capture.extents());
        return new CompactionPlan(cut, policy, frontiers, roots, capture.batches(), keys, transactions,
                leaderEpochs(capture), List.of(), List.of());
    }

    /** Only the sealed raw suffix is dirty when the complete key domain starts in a selected generation. */
    private static void requireEligible(NereusKafkaNativeInputV2.Captured capture, List<ParsedBatch> batches, long nowMs) {
        var config = capture.topicPolicy().config();
        long minLag = Math.max(config.compactionLagMs, 0L);
        long totalBytes = 0;
        long dirtyBytes = 0;
        long earliestFirstTimestamp = Long.MAX_VALUE;
        for (int index = 0; index < batches.size(); index++) {
            var input = capture.batches().get(index);
            totalBytes = Math.addExact(totalBytes, input.canonicalBody().length());
            if (!capture.dirtyInput(input)) continue;
            dirtyBytes = Math.addExact(dirtyBytes, input.canonicalBody().length());
            var batch = batches.get(index);
            if (minLag > 0 && batch.maxTimestamp() > nowMs - minLag)
                throw new IllegalStateException("native M5 sealed prefix has not reached min.compaction.lag.ms");
            if (input.sourceBatchOrdinal() == 0 && batch.maxTimestamp() > 0)
                earliestFirstTimestamp = Math.min(earliestFirstTimestamp, batch.maxTimestamp());
        }
        if (dirtyBytes == 0) throw new IllegalStateException("native M5 task has no cleanable bytes");
        boolean overdue = earliestFirstTimestamp < nowMs - Math.max(config.maxCompactionLagMs, 0L);
        double ratio = (double) dirtyBytes / totalBytes;
        if (!overdue && !(ratio > config.minCleanableRatio))
            throw new IllegalStateException("native M5 sealed prefix does not exceed min.cleanable.dirty.ratio");
    }

    private static List<ParsedBatch> parse(NereusKafkaNativeInputV2.Captured capture, Caps caps, long maximumDecodedBytes) {
        var result = new ArrayList<ParsedBatch>();
        int records = 0;
        long bytes = 0;
        long decoded = 0;
        long tombstones = 0;
        for (var input : capture.batches()) {
            bytes = Math.addExact(bytes, input.canonicalBody().length());
            if (result.size() == caps.maximumInputBatches() || bytes > caps.maximumDirtyBytes())
                throw new IllegalStateException("native M5 plan input exceeds its cap");
            var parsed = KafkaRecordBatchCodecV1.parseBounded(input.canonicalBody(), caps.maximumRecords() - records,
                    maximumDecodedBytes - decoded);
            records = Math.addExact(records, parsed.batch().records().size());
            decoded = Math.addExact(decoded, parsed.decodedBytes());
            tombstones = Math.addExact(tombstones, parsed.batch().records().stream().filter(RecordValue::tombstone).count());
            if (tombstones > caps.maximumTombstones()) throw new IllegalStateException("native M5 tombstones exceed their cap");
            result.add(parsed.batch());
        }
        return List.copyOf(result);
    }

    private static List<TransactionRange> transactions(NereusKafkaNativeInputV2.Captured capture, Caps caps) {
        var state = capture.nativeSources().protocol().transactionState();
        var frontiers = capture.frontiers();
        var result = state.completedTransactions().stream()
                .filter(tx -> tx.firstOffset() < frontiers.candidateEndOffsetExclusive()
                        && tx.markerEndOffsetExclusive() > frontiers.candidateStartOffset())
                .map(tx -> new TransactionRange(tx.producerId(), tx.firstOffset(), tx.markerEndOffsetExclusive(),
                        tx.aborted() ? TransactionOutcome.ABORTED : TransactionOutcome.COMMITTED, tx.coordinatorEpoch()))
                .sorted(Comparator.comparingLong(TransactionRange::firstOffset).thenComparingLong(TransactionRange::producerId))
                .toList();
        if (result.size() > caps.maximumTransactions() || state.ongoingTransactions().values().stream()
                .anyMatch(tx -> tx.firstOffset() < frontiers.candidateEndOffsetExclusive()))
            throw new IllegalStateException("native M5 transaction domain is open or exceeds its cap");
        return result;
    }

    private record Latest(RecordValue record, long horizon, boolean hasHorizon) { }

    private static List<LatestKeyProof> latestKeys(List<ParsedBatch> batches, List<TransactionRange> transactions, Caps caps) {
        var latest = new TreeMap<String, Latest>();
        long keyBytes = 0;
        for (var batch : batches) {
            if (batch.controlKind() != ControlKind.NONE || !committedForLatest(batch, transactions)) continue;
            var horizon = MemoryRecords.readableRecords(ByteBuffer.wrap(batch.canonicalBody().toByteArray()))
                    .batches().iterator().next().deleteHorizonMs();
            for (var record : batch.records()) {
                if (record.key().isEmpty()) continue;
                var key = record.key().orElseThrow();
                if (!latest.containsKey(key.toHex())) {
                    keyBytes = Math.addExact(keyBytes, key.length());
                    if (latest.size() == caps.maximumDistinctKeys() || keyBytes > caps.maximumKeyBytes())
                        throw new IllegalStateException("native M5 complete key domain exceeds its cap");
                }
                latest.put(key.toHex(), new Latest(record, horizon.orElse(0), horizon.isPresent()));
            }
        }
        // A raw tombstone has no previously established delete horizon. Zero carries no expiry authority;
        // retaining it allows the native rewrite to establish its first-clean header, without a timestamp guess.
        return latest.values().stream().map(value -> new LatestKeyProof(value.record().key().orElseThrow(),
                value.record().offset(), true, true, value.record().tombstone() && !value.hasHorizon(),
                value.record().tombstone() ? value.horizon() : 0)).toList();
    }

    private static boolean committedForLatest(ParsedBatch batch, List<TransactionRange> transactions) {
        if (!batch.transactional()) return true;
        var matching = transactions.stream().filter(tx -> tx.producerId() == batch.producerId()
                && tx.firstOffset() <= batch.baseOffset() && tx.endOffsetExclusive() >= batch.endOffsetExclusive()).toList();
        if (matching.size() != 1) {
            throw new IllegalStateException("native M5 transactional input lacks an exact closed outcome");
        }
        return matching.get(0).outcome() == TransactionOutcome.COMMITTED;
    }

    private static List<LeaderEpochRange> leaderEpochs(NereusKafkaNativeInputV2.Captured capture) {
        var starts = capture.nativeSources().protocol().leaderEpochIndex().startOffsets();
        long end = capture.frontiers().candidateEndOffsetExclusive();
        var result = new ArrayList<LeaderEpochRange>();
        for (var entry : starts.entrySet()) {
            var next = starts.higherEntry(entry.getKey());
            long exclusiveEnd = next == null ? end : Math.min(end, next.getValue());
            if (entry.getValue() < exclusiveEnd)
                result.add(new LeaderEpochRange(entry.getKey(), entry.getValue(), exclusiveEnd));
        }
        return List.copyOf(result);
    }

    private static Sha256Digest timestampRoot(List<ParsedBatch> batches) {
        return hash("NEREUS_NATIVE_KAFKA_TIMESTAMP_INPUT_V1", out -> {
            out.writeInt(batches.size());
            for (var batch : batches) {
                writeDigest(out, batch.bodySha256());
                out.writeInt(batch.records().size());
                for (var record : batch.records()) {
                    out.writeLong(record.offset());
                    out.writeLong(record.timestamp());
                }
            }
        });
    }

    private static Sha256Digest keyDomainRoot(NereusKafkaNativeInputV2.Captured capture, List<LatestKeyProof> keys,
            List<TransactionRange> transactions) {
        return hash("NEREUS_NATIVE_KAFKA_COMPLETE_KEY_DOMAIN_V1", out -> {
            out.writeLong(capture.frontiers().logStartOffset());
            out.writeLong(capture.frontiers().candidateEndOffsetExclusive());
            writeDigest(out, capture.nativeSources().sources().selectedViewSha256());
            writeDigest(out, M5MaterializationCodecV1.calculateSourceSetSha256(capture.extents()));
            out.writeInt(capture.batches().size());
            for (var batch : capture.batches()) writeDigest(out, Sha256Digest.hash(batch.canonicalBody()));
            writeTransactions(out, transactions);
            out.writeInt(keys.size());
            for (var key : keys) {
                out.writeInt(key.key().length());
                out.write(key.key().toByteArray());
                out.writeLong(key.latestEligibleOffset());
                out.writeBoolean(key.olderValueMayReappear());
                out.writeLong(key.tombstoneDeadlineMs());
            }
        });
    }

    private static Sha256Digest protocolRoot(NereusKafkaNativeInputV2.Captured capture) {
        return hash("NEREUS_NATIVE_KAFKA_COHERENT_CUT_V1", out -> {
            var root = capture.nativeSources().protocol().root();
            var fence = root.fence();
            writeDigest(out, fence.bindingId().digest());
            var incarnation = TopicIncarnationIdentityCodecV1.encode(fence.topicIncarnation());
            out.writeInt(incarnation.length());
            out.write(incarnation.toByteArray());
            out.writeInt(fence.partitionId());
            out.writeLong(fence.bindingGeneration());
            writeDigest(out, fence.storageEpochId().digest());
            out.writeLong(fence.ownerEpoch());
            out.writeInt(fence.kafkaLeaderEpoch());
            out.writeLong(root.stateVersion());
            var refs = root.references();
            for (var reference : List.of(refs.runTable(), refs.activeTail(), refs.sourceMap(), refs.committedProducerState(),
                    refs.speculativeProducerQueue(), refs.transactionIndex(), refs.leaderEpochIndex(),
                    refs.checkpointVector(), refs.sourceProtection())) {
                out.writeLong(reference.generation());
                writeDigest(out, reference.contentDigest());
            }
            var frontiers = root.frontiers();
            out.writeLong(frontiers.trimStartOffset());
            out.writeLong(frontiers.allocatedEndOffset());
            out.writeLong(frontiers.durableEndOffset());
            out.writeLong(frontiers.readableEndOffset());
            out.writeLong(frontiers.highWatermark());
            out.writeLong(frontiers.lastStableOffset());
        });
    }

    private static Sha256Digest policyRoot(NereusKafkaNativeInputV2.TopicPolicy policy) {
        return hash("NEREUS_NATIVE_KAFKA_EFFECTIVE_POLICY_V1", out -> {
            out.writeLong(policy.generation());
            out.writeShort(policy.kafkaFeatureLevel());
            var values = new TreeMap<>(policy.config().values());
            out.writeInt(values.size());
            for (var entry : values.entrySet()) {
                writeText(out, entry.getKey());
                writeText(out, String.valueOf(entry.getValue()));
            }
            // Preserve the actual IEEE value; the existing integer projection is not a native threshold proof.
            out.writeDouble(policy.config().minCleanableRatio);
        });
    }

    private static void writeTransactions(DataOutputStream out, List<TransactionRange> transactions) throws IOException {
        out.writeInt(transactions.size());
        for (var tx : transactions) {
            out.writeLong(tx.producerId());
            out.writeLong(tx.firstOffset());
            out.writeLong(tx.endOffsetExclusive());
            out.writeByte(tx.outcome().ordinal());
            out.writeInt(tx.coordinatorEpoch());
        }
    }

    private static void writeDigest(DataOutputStream out, Sha256Digest digest) throws IOException {
        out.write(digest.bytes().toByteArray());
    }

    private static void writeText(DataOutputStream out, String text) throws IOException {
        var bytes = text.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @FunctionalInterface
    private interface Writer {
        void write(DataOutputStream out) throws IOException;
    }

    private static Sha256Digest hash(String domain, Writer writer) {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                writeText(out, domain);
                writer.write(out);
            }
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }
}
