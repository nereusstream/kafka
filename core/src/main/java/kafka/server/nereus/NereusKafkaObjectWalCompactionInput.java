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
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaLeaderEpochIndexV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Caps;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Frontiers;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.InputBatch;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecoveryStateV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaRecordBatchCodecV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectAuthorizationV1.Grant;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectAuthorizationV1.Head;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectCoherentProtocolSnapshotV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionProtocolStateV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ObjectIdentityKind;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ObjectVersion;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** One protected native Object WAL prefix and its actual Kafka protocol checkpoint. */
record NereusKafkaObjectWalCompactionInput(
        KafkaObjectCoherentProtocolSnapshotV1 snapshot,
        KafkaCompactionRecoveryStateV1 recoveryState,
        NereusKafkaNativeInputV2.TopicPolicy topicPolicy,
        BindingReadSelector predecessorSelector,
        Sha256Digest rawViewSha256,
        Frontiers frontiers,
        List<SourceExtent> extents,
        List<InputBatch> batches) implements NereusKafkaCompactionPlanV2.Input {

    @Override
    public KafkaPartitionProtocolStateV1 protocolRoot() {
        return snapshot.root();
    }

    @Override
    public KafkaTransactionStateV1 transactionState() {
        return snapshot.transactionState();
    }

    @Override
    public KafkaLeaderEpochIndexV1 leaderEpochIndex() {
        return snapshot.leaderEpochIndex();
    }

    @Override
    public boolean dirtyInput(InputBatch batch) {
        return true;
    }

    /** Caller owns the native read admission and the Cell's retained-input reservation. */
    static Optional<NereusKafkaObjectWalCompactionInput> capture(
            KafkaObjectCoherentProtocolSnapshotV1 snapshot, KafkaProtocolCheckpointStateV1 checkpoint,
            Head authorization, Map<Sha256Digest, NereusKafkaObjectWalPartition.Source> sources,
            NereusKafkaNativeInputV2.TopicPolicy policy, BindingReadSelector selector, Sha256Digest rawView,
            NereusKafkaObjectProvider provider, Caps caps, long maximumDecodedBytes) throws IOException {
        var actual = snapshot.root().frontiers();
        var end = checkpoint.vector().recoveryCoveredThrough();
        if (!policy.config().compact || end <= actual.trimStartOffset() || end > actual.lastStableOffset()) {
            return Optional.empty();
        }
        if (authorization.closed() || !authorization.fence().equals(snapshot.root().fence())
                || authorization.startOffset() != actual.trimStartOffset()
                || authorization.endOffset() != actual.durableEndOffset()
                || !authorization.grants().stream().map(Grant::locator).toList().equals(snapshot.activeTail().locators())) {
            return Optional.empty();
        }
        var grants = authorization.grants().stream().takeWhile(grant -> grant.locator().endOffsetExclusive() <= end).toList();
        if (grants.isEmpty() || grants.get(grants.size() - 1).locator().endOffsetExclusive() != end
                || grants.size() > M5MaterializationRecordsV1.MAX_SOURCES) {
            return Optional.empty();
        }
        if (!eligible(grants, sources, caps)) return Optional.empty();
        var recovery = KafkaCompactionRecoveryStateV1.capture(snapshot, checkpoint);
        var frontiers = new Frontiers(actual.trimStartOffset(), actual.durableEndOffset(), actual.allocatedEndOffset(),
                actual.highWatermark(), actual.lastStableOffset(), actual.trimStartOffset(), end);
        var captured = new Frames(caps, maximumDecodedBytes);
        var extents = new ArrayList<SourceExtent>(grants.size());
        var batches = new ArrayList<InputBatch>();
        for (var grant : grants) {
            var source = sources.get(grant.locator().extent().walRunRootSha());
            extents.add(captured.read(grant, source, provider, batches));
        }
        return Optional.of(new NereusKafkaObjectWalCompactionInput(snapshot, recovery, policy, selector, rawView,
                frontiers, List.copyOf(extents), List.copyOf(batches)));
    }

    private static boolean eligible(List<Grant> grants, Map<Sha256Digest, NereusKafkaObjectWalPartition.Source> sources,
            Caps caps) {
        long bytes = 0;
        long batches = 0;
        for (var grant : grants) {
            var source = sources.get(grant.locator().extent().walRunRootSha());
            if (source == null || !source.sealed().getAsBoolean()) return false;
            bytes = Math.addExact(bytes, grant.object().bodyLength());
            batches = Math.addExact(batches, grant.locator().directoryRowCount());
            if (bytes > caps.maximumDirtyBytes() || batches > caps.maximumInputBatches()) return false;
        }
        return true;
    }

    private record Batch(CanonicalBytes body, int records, long minimum, long maximum) { }

    private static final class Frames {
        private final Caps caps;
        private final long maximumDecodedBytes;
        private long nativeBytes;
        private long decodedBytes;
        private int records;

        private Frames(Caps caps, long maximumDecodedBytes) {
            this.caps = caps;
            this.maximumDecodedBytes = maximumDecodedBytes;
        }

        private SourceExtent read(Grant grant, NereusKafkaObjectWalPartition.Source source,
                NereusKafkaObjectProvider provider, List<InputBatch> input) throws IOException {
            var profile = provider.profile();
            var exact = grant.object();
            var locator = grant.locator();
            var frames = new ArrayList<Batch>(locator.directoryRowCount());
            var original = provider.readOriginal(exact, Optional.empty());
            var version = original.immutableVersionToken().orElseThrow(
                    () -> new IOException("materialization source has no immutable Provider version"));
            source.capture().capture(grant, original.value(), (frame, bytes) -> frames.add(readBatch(bytes)));
            var physical = new ObjectVersion(profile.physicalBinding().physicalNamespace(),
                    CanonicalUtf8.fromString(exact.key()), ObjectIdentityKind.IMMUTABLE_VERSION,
                    CanonicalUtf8.fromBytes(version.toByteArray()));
            int count = 0;
            long minimum = Long.MAX_VALUE;
            long maximum = -1;
            for (var frame : frames) {
                count = Math.addExact(count, frame.records());
                if (frame.records() != 0) {
                    minimum = Math.min(minimum, frame.minimum());
                    maximum = Math.max(maximum, frame.maximum());
                }
            }
            for (int ordinal = 0; ordinal < frames.size(); ordinal++) {
                input.add(new InputBatch(physical.sha256(), ordinal, frames.get(ordinal).body()));
            }
            return new SourceExtent(SourceKind.OBJECT_WAL_NWG1, physical.sha256(),
                    new ProtocolCoverage(PositionDomain.KAFKA_OFFSET, locator.startOffset(), locator.endOffsetExclusive()),
                    exact.key(), exact.bodyLength(), count, count == 0 ? -1 : minimum, maximum, exact.bodySha256(),
                    Optional.of(version), Optional.empty(), locator.extent().walRunRootSha(),
                    Sha256Digest.hash(CanonicalUtf8.fromString(profile.references().allowedEncryptionScope()).bytes()),
                    true, false, List.of(locator.binding().bindingId().digest()));
        }

        private Batch readBatch(ByteBuffer borrowed) throws IOException {
            nativeBytes = Math.addExact(nativeBytes, borrowed.remaining());
            if (nativeBytes > caps.maximumDirtyBytes()) throw new IOException("Kafka Object capture exceeds its byte limit");
            byte[] raw = new byte[borrowed.remaining()];
            borrowed.get(raw);
            var canonical = CanonicalBytes.copyOf(raw);
            var parsed = KafkaRecordBatchCodecV1.parseBounded(canonical, caps.maximumRecords() - records,
                    maximumDecodedBytes - decodedBytes);
            var batch = parsed.batch();
            records = Math.addExact(records, batch.records().size());
            decodedBytes = Math.addExact(decodedBytes, parsed.decodedBytes());
            long minimum = Long.MAX_VALUE;
            long maximum = -1;
            for (var record : batch.records()) {
                minimum = Math.min(minimum, record.timestamp());
                maximum = Math.max(maximum, record.timestamp());
            }
            return new Batch(canonical, batch.records().size(), minimum, maximum);
        }
    }
}
