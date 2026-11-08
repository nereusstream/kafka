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
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunRootRecord;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/** Binds each immutable physical run to the exact native election that could write into it. */
final class NereusKafkaObjectWalOwners {
    private static final int FORMAT = 0x4b574f31; // KWO1
    private static final int BYTES = 3 * Integer.BYTES + 3 * Long.BYTES;

    private final NereusKafkaObjectWalShard.Assignment shard;
    private final ExactMetadataTransactionStoreV1 metadata;
    private final Duration timeout;

    NereusKafkaObjectWalOwners(NereusKafkaObjectWalShard.Assignment shard,
            ExactMetadataTransactionStoreV1 metadata, Duration timeout) {
        this.shard = shard;
        this.metadata = metadata;
        this.timeout = timeout;
    }

    void admit(KafkaOwnerIdentityV1 candidate) throws Exception {
        String key = shard.cellRoot() + "/native-owner/" + shard.id();
        var before = await(metadata.read(key));
        if (before.isPresent()) {
            var previous = decode(before.orElseThrow().canonicalStoredBytes());
            if (previous.equals(candidate)) return;
            if (!candidate.succeeds(previous)) {
                throw new IllegalStateException("native Object WAL election regressed its durable owner");
            }
        }
        var bytes = encode(candidate);
        if (await(metadata.compareAndSet(before, key, bytes)) != MutationOutcome.APPLIED_EXACT
                && !await(metadata.read(key)).map(value -> value.canonicalStoredBytes()).equals(Optional.of(bytes))) {
            throw new IllegalStateException("native Object WAL election did not resolve to the exact candidate");
        }
    }

    void requireCurrent(KafkaOwnerIdentityV1 owner) throws Exception {
        var actual = await(metadata.read(shard.cellRoot() + "/native-owner/" + shard.id()))
                .map(value -> value.canonicalStoredBytes());
        if (!actual.equals(Optional.of(encode(owner)))) {
            throw new IllegalStateException("native Object WAL election has been superseded");
        }
    }

    void recordCreator(WalRunRootRecord root, KafkaOwnerIdentityV1 owner) throws Exception {
        requireCurrent(owner);
        var key = creatorKey(root);
        var bytes = encode(owner);
        if (await(metadata.compareAndSet(Optional.empty(), key, bytes)) != MutationOutcome.APPLIED_EXACT
                && !await(metadata.read(key)).map(value -> value.canonicalStoredBytes()).equals(Optional.of(bytes))) {
            throw new IllegalStateException("native Object WAL Root creator did not resolve to the original election");
        }
    }

    KafkaOwnerIdentityV1 creator(WalRunRootRecord root) throws Exception {
        return decode(await(metadata.read(creatorKey(root))).orElseThrow(
                () -> new IllegalStateException("native Object WAL Root has no exact creator election"))
                .canonicalStoredBytes());
    }

    private String creatorKey(WalRunRootRecord root) {
        if (root.shardId() != shard.id()) throw new IllegalArgumentException("Object WAL Root belongs to another shard");
        return shard.cellRoot() + "/run-owner/" + WalRunControlCodec.rootSha256(root).toHex();
    }

    private static CanonicalBytes encode(KafkaOwnerIdentityV1 owner) {
        return CanonicalBytes.copyOf(ByteBuffer.allocate(BYTES).putInt(FORMAT).putLong(owner.ownerEpoch())
                .putInt(owner.kafkaLeaderEpoch()).putInt(owner.brokerId()).putLong(owner.brokerEpoch())
                .putLong(owner.metadataOffset()).array());
    }

    private static KafkaOwnerIdentityV1 decode(CanonicalBytes bytes) {
        if (bytes.length() != BYTES) throw new IllegalStateException("invalid native Object WAL election length");
        var input = ByteBuffer.wrap(bytes.toByteArray());
        if (input.getInt() != FORMAT) throw new IllegalStateException("unknown native Object WAL election format");
        return new KafkaOwnerIdentityV1(input.getLong(), input.getInt(), input.getInt(), input.getLong(), input.getLong());
    }

    private <T> T await(CompletionStage<T> operation) throws Exception {
        return operation.toCompletableFuture().get(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }
}
