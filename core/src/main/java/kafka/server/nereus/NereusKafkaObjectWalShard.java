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
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/** An immutable assignment prevents two Kafka partitions from sharing the same run/key namespace. */
final class NereusKafkaObjectWalShard {
    private static final int FORMAT = 0x4b575331; // KWS1
    private static final int ASSIGNMENT_BYTES = 2 * Integer.BYTES + 2 * Sha256Digest.LENGTH;
    private static final int MAX_PROBES = 32;

    private NereusKafkaObjectWalShard() { }

    record Assignment(String cellRoot, int id) { }

    static Assignment reserve(ExactMetadataTransactionStoreV1 metadata, Sha256Digest cell,
            KafkaPartitionFenceV1 binding, Duration timeout) throws Exception {
        String root = "/nereus-kafka-object-wal/" + cell.toHex();
        var assigned = CanonicalBytes.copyOf(ByteBuffer.allocate(ASSIGNMENT_BYTES)
                .putInt(FORMAT).put(binding.bindingId().digest().bytes().toByteArray())
                .put(binding.storageEpochId().digest().bytes().toByteArray()).putInt(binding.partitionId()).array());
        long deadline = System.nanoTime() + timeout.toNanos();
        for (int probe = 0; probe < MAX_PROBES; probe++) {
            int id = candidate(cell, assigned, probe);
            String key = root + "/shard-bindings/" + id;
            var current = await(metadata.read(key), deadline);
            if (current.isEmpty()) {
                await(metadata.compareAndSet(Optional.empty(), key, assigned), deadline);
                current = await(metadata.read(key), deadline);
                // No candidate beyond this key can be considered until this exact assignment is resolved.
                if (current.isEmpty()) throw new IllegalStateException("Object WAL shard assignment remains unresolved");
            }
            var value = current.orElseThrow().canonicalStoredBytes();
            if (value.equals(assigned)) return new Assignment(root, id);
            if (value.length() != ASSIGNMENT_BYTES || ByteBuffer.wrap(value.toByteArray()).getInt() != FORMAT) {
                throw new IllegalStateException("Object WAL shard assignment has an invalid durable format");
            }
        }
        throw new IllegalStateException("Object WAL shard assignment reached its bounded collision limit");
    }

    private static int candidate(Sha256Digest cell, CanonicalBytes assigned, int probe) {
        var input = ByteBuffer.allocate(Sha256Digest.LENGTH + assigned.length() + Integer.BYTES)
                .put(cell.bytes().toByteArray()).put(assigned.toByteArray()).putInt(probe);
        var digest = Sha256Digest.hash(CanonicalBytes.copyOf(input.array()));
        return ByteBuffer.wrap(digest.bytes().toByteArray()).getInt() & Integer.MAX_VALUE;
    }

    private static <T> T await(CompletionStage<T> value, long deadline) throws Exception {
        return value.toCompletableFuture().get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
    }
}
