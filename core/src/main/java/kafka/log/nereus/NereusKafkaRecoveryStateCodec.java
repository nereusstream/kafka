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

import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.recovery.KafkaRecoveryStateCodec;
import com.nereusstream.kafka.recovery.KafkaReplayBatch;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointHeader;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSection;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** Creates exactly one fresh Kafka state and delegates all replay invariants to it. */
public final class NereusKafkaRecoveryStateCodec
        implements KafkaRecoveryStateCodec<NereusKafkaRecoveredState> {
    private final KafkaPartitionIdentity identity;
    private final int leaderEpoch;
    private final long logStartOffset;
    private final long stableEndOffset;
    private final NereusProducerStateManager producerStateManager;
    private final AtomicBoolean created = new AtomicBoolean();

    public NereusKafkaRecoveryStateCodec(
            KafkaPartitionIdentity identity,
            int leaderEpoch,
            long logStartOffset,
            long stableEndOffset,
            NereusProducerStateManager producerStateManager
    ) {
        this.identity = Objects.requireNonNull(identity, "identity");
        if (leaderEpoch < 0
                || logStartOffset < 0
                || stableEndOffset < logStartOffset) {
            throw new IllegalArgumentException("invalid Nereus Kafka recovery codec bounds");
        }
        this.leaderEpoch = leaderEpoch;
        this.logStartOffset = logStartOffset;
        this.stableEndOffset = stableEndOffset;
        this.producerStateManager = Objects.requireNonNull(
                producerStateManager, "producerStateManager");
    }

    @Override
    public NereusKafkaRecoveredState freshState() {
        if (!created.compareAndSet(false, true)) {
            throw new IllegalStateException("Kafka recovery state codec is one-shot");
        }
        return new NereusKafkaRecoveredState(
                identity,
                leaderEpoch,
                logStartOffset,
                stableEndOffset,
                producerStateManager);
    }

    @Override
    public void hydrateCheckpoint(
            NereusKafkaRecoveredState state,
            KafkaCheckpointHeader header,
            List<KafkaCheckpointSection> sections
    ) {
        exact(state).hydrateCheckpoint(header, sections);
    }

    @Override
    public void replayBatch(
            NereusKafkaRecoveredState state,
            KafkaReplayBatch batch
    ) {
        exact(state).replay(batch);
    }

    @Override
    public void validateRecoveredState(
            NereusKafkaRecoveredState state,
            KafkaCheckpointSourceState frozenSource
    ) {
        exact(state).freeze(frozenSource);
    }

    private NereusKafkaRecoveredState exact(NereusKafkaRecoveredState state) {
        NereusKafkaRecoveredState exact = Objects.requireNonNull(state, "state");
        if (!exact.identity().equals(identity)) {
            throw new IllegalArgumentException("Kafka recovery state belongs to another partition");
        }
        return exact;
    }
}
