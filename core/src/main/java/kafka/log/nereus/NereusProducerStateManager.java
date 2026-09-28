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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.AbortedTxn;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.CompletedTxn;
import org.apache.kafka.storage.internals.log.ProducerAppendInfo;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** Stock producer state loaded from the shared checkpoint and validated tail. */
public final class NereusProducerStateManager extends ProducerStateManager {
    private final NereusTransactionIndex transactionIndex;

    public NereusProducerStateManager(
            TopicPartition topicPartition,
            File logDir,
            int maxTransactionTimeoutMs,
            ProducerStateManagerConfig config,
            Time time,
            NereusTransactionIndex transactionIndex)
            throws IOException {
        super(topicPartition, logDir, maxTransactionTimeoutMs, config, time);
        this.transactionIndex = transactionIndex;
    }

    public void resetForRecovery(long logStartOffset) throws IOException {
        truncateFullyAndStartAt(logStartOffset);
        transactionIndex.reset();
    }

    public void loadSharedState(com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentProtocolSnapshotV1 state)
            throws IOException {
        resetForRecovery(state.root().frontiers().trimStartOffset());
        for (var producer : state.committedProducerState().producers().values()) {
            var ongoing = state.transactionState().ongoingTransactions().get(producer.producerId());
            var batches = producer.recentBatches().stream()
                    .map(batch -> new org.apache.kafka.storage.internals.log.BatchMetadata(
                            batch.identity().lastSequence(),
                            batch.endOffsetExclusive() - 1,
                            Math.toIntExact(batch.endOffsetExclusive() - batch.startOffset() - 1),
                            batch.maxTimestamp()))
                    .toList();
            loadProducerEntry(org.apache.kafka.storage.internals.log.ProducerStateEntry.fromBatchMetadata(
                    producer.producerId(),
                    producer.producerEpoch(),
                    producer.coordinatorEpoch(),
                    producer.lastTimestamp(),
                    ongoing == null ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(ongoing.firstOffset()),
                    batches));
        }
        long end = state.root().frontiers().durableEndOffset();
        for (var transaction : state.transactionState().abortedTransactions()) {
            long stable = transaction.markerEndOffsetExclusive();
            for (var other : state.transactionState().ongoingTransactions().values()) {
                if (other.firstOffset() < transaction.markerEndOffsetExclusive())
                    stable = Math.min(stable, other.firstOffset());
            }
            for (var other : state.transactionState().completedTransactions()) {
                if (other.producerId() != transaction.producerId()
                        && other.firstOffset() < transaction.markerEndOffsetExclusive()
                        && other.markerEndOffsetExclusive() > transaction.markerEndOffsetExclusive())
                    stable = Math.min(stable, other.firstOffset());
            }
            transactionIndex.append(new AbortedTxn(
                    transaction.producerId(),
                    transaction.firstOffset(),
                    transaction.markerEndOffsetExclusive() - 1,
                    stable));
        }
        updateMapEndOffset(end);
        onHighWatermarkUpdated(end);
    }

    public void replayBatch(RecordBatch batch) throws IOException {
        if (batch.baseOffset() != mapEndOffset()) {
            throw new IllegalArgumentException("Kafka producer replay batch does not start at map end");
        }
        if (batch.hasProducerId()) {
            ProducerAppendInfo appendInfo = prepareUpdate(batch.producerId(), AppendOrigin.REPLICATION);
            Optional<CompletedTxn> completed = appendInfo.append(batch, Optional.empty(), (short) 0);
            update(appendInfo);
            if (completed.isPresent()) {
                CompletedTxn transaction = completed.orElseThrow();
                long lastStableOffset = lastStableOffset(transaction);
                if (transaction.isAborted()) {
                    transactionIndex.append(new AbortedTxn(transaction, lastStableOffset));
                }
                completeTxn(transaction);
            }
        }
        updateMapEndOffset(Math.addExact(batch.lastOffset(), 1));
    }

    public List<AbortedTxn> collectAbortedTransactions(long fetchOffset, long upperBoundOffset) {
        return transactionIndex
                .collectAbortedTxns(fetchOffset, upperBoundOffset)
                .abortedTransactions();
    }

    @Override
    public void takeSnapshot() {
        // Cold recovery installs the durable shared checkpoint and its validated tail.
    }

    @Override
    public Optional<File> takeSnapshot(boolean sync) {
        return Optional.empty();
    }
}
