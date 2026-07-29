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
package kafka.log.nereus;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.storage.internals.log.OffsetMap;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadSourceRef;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.materialization.ExactSourceSet;
import com.nereusstream.materialization.SourceGeneration;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

final class KafkaCompactionOracleSupport {
    private KafkaCompactionOracleSupport() {
    }

    static OffsetMap offsetMap(int slots) {
        return new InMemoryOffsetMap(slots);
    }

    static MemoryRecords tombstoneWithDeleteHorizon(
        long offset,
        long timestamp,
        byte[] key,
        long deleteHorizonMillis
    ) {
        try (MemoryRecordsBuilder builder =
            builder(
                offset,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                false,
                false,
                deleteHorizonMillis)) {
            builder.append(new SimpleRecord(timestamp, key, null));
            return builder.build();
        }
    }

    static MemoryRecords markerWithDeleteHorizon(
        long offset,
        long timestamp,
        long producerId,
        short producerEpoch,
        EndTransactionMarker marker,
        long deleteHorizonMillis
    ) {
        try (MemoryRecordsBuilder builder =
            builder(
                offset,
                producerId,
                producerEpoch,
                RecordBatch.NO_SEQUENCE,
                true,
                true,
                deleteHorizonMillis)) {
            builder.appendEndTxnMarker(timestamp, marker);
            return builder.build();
        }
    }

    private static MemoryRecordsBuilder builder(
        long offset,
        long producerId,
        short producerEpoch,
        int baseSequence,
        boolean transactional,
        boolean control,
        long deleteHorizonMillis
    ) {
        ByteBufferOutputStream output =
            new ByteBufferOutputStream(ByteBuffer.allocate(1 << 10));
        return new MemoryRecordsBuilder(
            output,
            RecordBatch.CURRENT_MAGIC_VALUE,
            Compression.NONE,
            TimestampType.CREATE_TIME,
            offset,
            RecordBatch.NO_TIMESTAMP,
            producerId,
            producerEpoch,
            baseSequence,
            transactional,
            control,
            0,
            1 << 10,
            deleteHorizonMillis);
    }

    static ExactSourceSet sourceSet(List<ReadBatch> batches) {
        ArrayList<SourceGeneration> sources = new ArrayList<>();
        long cumulativeBytes = 0;
        for (ReadBatch batch : batches) {
            long nextCumulative =
                Math.addExact(cumulativeBytes, batch.payload().length);
            sources.add(
                new SourceGeneration(
                    ReadView.COMMITTED,
                    batch.source().resolvedRange(),
                    batch.source().generation(),
                    batch.source().commitVersion(),
                    "test/f9-compaction-oracle/" + batch.range().startOffset(),
                    batch.range().startOffset(),
                    new Checksum(ChecksumType.SHA256, "d".repeat(64)),
                    batch.source().target(),
                    batch.source().targetIdentity(),
                    Optional.empty(),
                    batch.payloadFormat(),
                    batch.projectionRef(),
                    Math.toIntExact(batch.range().recordCount()),
                    1,
                    batch.payload().length,
                    batch.schemaRefs(),
                    cumulativeBytes,
                    nextCumulative));
            cumulativeBytes = nextCumulative;
        }
        return ExactSourceSet.create(
            ReadView.COMMITTED,
            new OffsetRange(
                batches.get(0).range().startOffset(),
                batches.get(batches.size() - 1).range().endOffset()),
            sources);
    }

    static ReadBatch readBatch(
        OffsetRange range,
        byte[] payload,
        String suffix
    ) {
        Checksum checksum =
            new Checksum(ChecksumType.CRC32C, "00000000");
        EntryIndexRef index =
            new EntryIndexRef(
                EntryIndexLocation.OBJECT_FOOTER,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                0,
                1,
                checksum);
        ObjectSliceReadTarget target =
            new ObjectSliceReadTarget(
                1,
                new ObjectId("kafka-compaction-oracle-" + suffix),
                new ObjectKey("f9/kafka-compaction-oracle-" + suffix),
                ObjectType.MULTI_STREAM_WAL_OBJECT,
                "WAL_OBJECT_V1",
                "KAFKA_RECORD_BATCH_V1",
                "slice-" + suffix,
                0,
                payload.length,
                checksum,
                index);
        ReadSourceRef source =
            new ReadSourceRef(
                range,
                0,
                1,
                target,
                ReadTargetIdentities.sha256(target));
        return new ReadBatch(
            range,
            PayloadFormat.KAFKA_RECORD_BATCH,
            payload,
            List.of(),
            Optional.empty(),
            source);
    }

    private static String base64(ByteBuffer value) {
        ByteBuffer copy = value.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static final class InMemoryOffsetMap implements OffsetMap {
        private final int slots;
        private final Map<String, Long> offsets = new HashMap<>();
        private long latestOffset = -1;

        private InMemoryOffsetMap(int slots) {
            this.slots = slots;
        }

        @Override
        public int slots() {
            return slots;
        }

        @Override
        public void put(ByteBuffer key, long offset) {
            offsets.put(base64(key), offset);
            latestOffset = offset;
        }

        @Override
        public long get(ByteBuffer key) {
            return offsets.getOrDefault(base64(key), -1L);
        }

        @Override
        public void updateLatestOffset(long offset) {
            latestOffset = offset;
        }

        @Override
        public void clear() {
            offsets.clear();
            latestOffset = -1;
        }

        @Override
        public int size() {
            return offsets.size();
        }

        @Override
        public long latestOffset() {
            return latestOffset;
        }
    }
}
