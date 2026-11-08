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
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.ObjectIdentity;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** The native writer records the exact Provider mutation before sending it. Closing the same key excludes later sends. */
final class NereusKafkaObjectWalDispatchFence {
    private static final int FORMAT = 0x4b574431; // KWD1
    private static final int MAX_KEY_BYTES = 1024;
    private static final int MAX_MUTATION_RACES = 4;
    private static final int HEADER_BYTES = Integer.BYTES + 2;
    private static final int PENDING_FIELDS_BYTES = Short.BYTES + Long.BYTES + Sha256Digest.LENGTH;

    private final ExactMetadataTransactionStoreV1 metadata;
    private final WalRunRootRecord root;
    private final Duration timeout;
    private final String key;

    NereusKafkaObjectWalDispatchFence(ExactMetadataTransactionStoreV1 metadata,
            NereusKafkaObjectWalShard.Assignment shard, WalRunRootRecord root, Duration timeout) {
        if (shard.id() != root.shardId()) throw new IllegalArgumentException("Object WAL dispatch fence differs from shard");
        this.metadata = metadata;
        this.root = root;
        this.timeout = timeout;
        key = shard.cellRoot() + "/dispatch/" + WalRunControlCodec.rootSha256(root).toHex();
    }

    /** Only an exact fresh Root authority may install the original writer gate. Never opens a closed gate. */
    void initializeFresh() throws IOException {
        var deadline = deadline();
        var empty = new State(false, Optional.empty());
        var before = read(deadline);
        if (before.isEmpty()) {
            if (update(before, empty, deadline) == MutationOutcome.APPLIED_EXACT) return;
            before = read(deadline);
        }
        if (!before.map(Value::state).equals(Optional.of(empty))) {
            throw new IOException("native Object WAL original dispatch admission was not confirmed");
        }
    }

    void arm(ObjectIdentity object) throws IOException {
        requireObject(object);
        var deadline = deadline();
        var before = read(deadline).orElseThrow(() -> new IOException("Object WAL dispatch gate is absent"));
        if (before.state().closed() || before.state().pending().isPresent()) {
            throw new IOException("Object WAL dispatch gate is closed or retains an unresolved mutation");
        }
        var armed = new State(false, Optional.of(object));
        if (update(Optional.of(before), armed, deadline) == MutationOutcome.APPLIED_EXACT) return;
        var observed = read(deadline);
        if (!observed.map(Value::state).equals(Optional.of(armed))) {
            throw new IOException("Object WAL dispatch admission did not select the original candidate");
        }
    }

    /** An actual terminal Provider reply, or a complete exact positive reread, settles only the armed identity. */
    void complete(ObjectIdentity object) throws IOException {
        complete(object, false);
    }

    void completeVerified(ObjectIdentity object) throws IOException {
        complete(object, true);
    }

    private void complete(ObjectIdentity object, boolean independentlyVerified) throws IOException {
        var deadline = deadline();
        for (int attempt = 0; attempt < MAX_MUTATION_RACES; attempt++) {
            var before = read(deadline).orElseThrow(() -> new IOException("Object WAL dispatch gate is absent"));
            if ((independentlyVerified || before.state().closed()) && before.state().pending().isEmpty()) return;
            if (!before.state().pending().equals(Optional.of(object))) {
                throw new IOException("Object WAL terminal reply differs from the exact armed mutation");
            }
            var next = new State(before.state().closed(), Optional.empty());
            var outcome = update(Optional.of(before), next, deadline);
            if (outcome == MutationOutcome.APPLIED_EXACT) return;
            var after = read(deadline).orElseThrow(() -> new IOException("Object WAL dispatch gate disappeared"));
            if (after.state().pending().isEmpty()) return;
            if (!after.state().pending().equals(Optional.of(object))) {
                throw new IOException("Object WAL terminal reply encountered another mutation");
            }
        }
        throw new IOException("Object WAL terminal mutation did not converge");
    }

    /** Idempotently forbids dispatch without discarding a candidate which the original process may still send. */
    Optional<ObjectIdentity> closeAdmission() throws IOException {
        var deadline = deadline();
        for (int attempt = 0; attempt < MAX_MUTATION_RACES; attempt++) {
            var before = read(deadline);
            var state = before.map(Value::state).orElseGet(() -> new State(false, Optional.empty()));
            if (state.closed()) return state.pending();
            var next = new State(true, state.pending());
            if (update(before, next, deadline) == MutationOutcome.APPLIED_EXACT) return next.pending();
            var observed = read(deadline);
            if (observed.filter(value -> value.state().closed()).isPresent()) {
                return observed.orElseThrow().state().pending();
            }
        }
        throw new IOException("Object WAL dispatch closure did not converge");
    }

    /**
     * Proves only a positive exact immutable body. An absent/unavailable body does not establish that a previously
     * armed request cannot arrive later. The caller cannot start physical LIST recovery in that case.
     */
    void closeAndRequireDrained(C1ObjectProviderSession readOnlyProvider) throws IOException {
        var pending = closeAdmission();
        if (pending.isEmpty()) return;
        var exact = pending.orElseThrow();
        readOnlyProvider.consumeVerifiedObject(exact, Optional.empty(),
                stream -> stream.transferTo(java.io.OutputStream.nullOutputStream()));
        completeVerified(exact);
    }

    private record State(boolean closed, Optional<ObjectIdentity> pending) { }
    private record Value(VersionedValue version, State state) { }

    private Optional<Value> read(long deadline) throws IOException {
        return await(metadata.read(key), deadline).map(value -> new Value(value, decode(value.canonicalStoredBytes())));
    }

    private MutationOutcome update(Optional<Value> before, State next, long deadline) throws IOException {
        return await(metadata.compareAndSet(before.map(Value::version), key, encode(next)), deadline);
    }

    private CanonicalBytes encode(State state) {
        var object = state.pending();
        byte[] path = object.map(value -> value.key().getBytes(StandardCharsets.US_ASCII)).orElseGet(() -> new byte[0]);
        var value = ByteBuffer.allocate(HEADER_BYTES + (object.isPresent() ? PENDING_FIELDS_BYTES + path.length : 0))
                .putInt(FORMAT).put((byte) (state.closed() ? 1 : 0)).put((byte) (object.isPresent() ? 1 : 0));
        object.ifPresent(identity -> value.putShort((short) path.length).put(path).putLong(identity.bodyLength())
                .put(identity.bodySha256().bytes().toByteArray()));
        return CanonicalBytes.copyOf(value.array());
    }

    private State decode(CanonicalBytes bytes) {
        if (bytes.length() < HEADER_BYTES || bytes.length() > HEADER_BYTES + PENDING_FIELDS_BYTES + MAX_KEY_BYTES) {
            throw new IllegalStateException("native Object WAL dispatch value exceeds its wire bounds");
        }
        var value = ByteBuffer.wrap(bytes.toByteArray());
        if (value.getInt() != FORMAT) throw new IllegalStateException("unknown native Object WAL dispatch format");
        boolean closed = flag(value);
        boolean pending = flag(value);
        if (!pending && !value.hasRemaining()) return new State(closed, Optional.empty());
        if (!pending || value.remaining() <= PENDING_FIELDS_BYTES) {
            throw new IllegalStateException("invalid native Object WAL dispatch value");
        }
        int length = Short.toUnsignedInt(value.getShort());
        if (length <= 0 || value.remaining() != length + Long.BYTES + Sha256Digest.LENGTH) {
            throw new IllegalStateException("invalid native Object WAL dispatch candidate key length");
        }
        byte[] path = new byte[length];
        value.get(path);
        long bodyLength = value.getLong();
        byte[] sha = new byte[Sha256Digest.LENGTH];
        value.get(sha);
        var object = new ObjectIdentity(new String(path, StandardCharsets.US_ASCII), bodyLength, Sha256Digest.copyOf(sha));
        requireObject(object);
        return new State(closed, Optional.of(object));
    }

    private static boolean flag(ByteBuffer value) {
        return switch (value.get()) {
            case 0 -> false;
            case 1 -> true;
            default -> throw new IllegalStateException("unknown native Object WAL dispatch flag");
        };
    }

    private void requireObject(ObjectIdentity object) {
        String path = object.key();
        if (!path.startsWith(root.providerConfiguration().exclusiveNamespacePrefix() + "/")
                || path.length() > MAX_KEY_BYTES || !StandardCharsets.US_ASCII.newEncoder().canEncode(path)
                || object.bodyLength() > root.providerConfiguration().maxObjectBodyBytes()) {
            throw new IllegalArgumentException("Object WAL dispatch candidate exceeds the exact Root");
        }
    }

    private long deadline() {
        return System.nanoTime() + timeout.toNanos();
    }

    private static <T> T await(CompletionStage<T> value, long deadline) throws IOException {
        try {
            return value.toCompletableFuture().get(Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("native Object WAL dispatch metadata wait was interrupted", interrupted);
        } catch (ExecutionException | TimeoutException unresolved) {
            throw new IOException("native Object WAL dispatch metadata outcome is unresolved", unresolved);
        }
    }
}
