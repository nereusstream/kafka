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
import org.apache.kafka.storage.internals.log.BatchMetadata;
import org.apache.kafka.storage.internals.log.CompletedTxn;
import org.apache.kafka.storage.internals.log.ProducerAppendInfo;
import org.apache.kafka.storage.internals.log.ProducerStateEntry;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;

import com.nereusstream.kafka.checkpoint.KafkaProducerTransactionState;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/** Stock producer state with NKC1 import/export and exact committed-batch replay instead of local snapshots. */
public final class NereusProducerStateManager extends ProducerStateManager {
    private final TopicPartition topicPartition;
    private final NereusTransactionIndex transactionIndex;

    public NereusProducerStateManager(
            TopicPartition topicPartition,
            File logDir,
            int maxTransactionTimeoutMs,
            ProducerStateManagerConfig config,
            Time time,
            NereusTransactionIndex transactionIndex
    ) throws IOException {
        super(topicPartition, logDir, maxTransactionTimeoutMs, config, time);
        this.topicPartition = topicPartition;
        this.transactionIndex = transactionIndex;
    }

    public void resetForRecovery(long logStartOffset) throws IOException {
        truncateFullyAndStartAt(logStartOffset);
        transactionIndex.reset();
    }

    public void restoreCanonical(KafkaProducerTransactionState state) throws IOException {
        state.requireCheckpointOffset(state.mapEndOffset());
        if (!activeProducers().isEmpty()
                || firstUndecidedOffset().isPresent()
                || !transactionIndex.isEmpty()) {
            throw new IllegalStateException(
                    "NKC1 producer state can only be imported into a fresh manager");
        }
        for (KafkaProducerTransactionState.ProducerState producer : state.producers()) {
            ProducerStateEntry imported = ProducerStateEntry.fromBatchMetadata(
                    producer.producerId(),
                    producer.producerEpoch(),
                    producer.coordinatorEpoch(),
                    producer.lastTimestamp(),
                    producer.currentTransactionFirstOffset(),
                    producer.batches().stream()
                            .map(NereusProducerStateManager::importBatch)
                            .toList());
            loadProducerEntry(imported);
        }
        for (KafkaProducerTransactionState.AbortedTransaction transaction
                : state.abortedTransactions()) {
            transactionIndex.append(new AbortedTxn(
                    transaction.producerId(),
                    transaction.firstOffset(),
                    transaction.lastOffset(),
                    transaction.lastStableOffset()));
        }
        updateMapEndOffset(state.mapEndOffset());
        KafkaProducerTransactionState imported = exportCanonical(state.mapEndOffset());
        if (!imported.equals(state)) {
            throw new IllegalArgumentException(
                    "NKC1 producer state cannot be represented exactly by the stock manager");
        }
    }

    public void replayBatch(RecordBatch batch) throws IOException {
        if (batch.baseOffset() != mapEndOffset()) {
            throw new IllegalArgumentException(
                    "Kafka producer replay batch does not start at map end");
        }
        if (batch.hasProducerId()) {
            ProducerAppendInfo appendInfo = prepareUpdate(
                    batch.producerId(), AppendOrigin.REPLICATION);
            Optional<CompletedTxn> completed = appendInfo.append(
                    batch, Optional.empty(), (short) 0);
            update(appendInfo);
            if (completed.isPresent()) {
                CompletedTxn transaction = completed.orElseThrow();
                long lastStableOffset = lastStableOffset(transaction);
                if (transaction.isAborted()) {
                    transactionIndex.append(new AbortedTxn(
                            transaction, lastStableOffset));
                }
                completeTxn(transaction);
            }
        }
        updateMapEndOffset(Math.addExact(batch.lastOffset(), 1));
    }

    public KafkaProducerTransactionState freezeCanonical(long stableEndOffset) {
        if (mapEndOffset() != stableEndOffset) {
            throw new IllegalStateException(
                    "Kafka producer map end does not match stable end");
        }
        onHighWatermarkUpdated(stableEndOffset);
        return exportCanonical(stableEndOffset);
    }

    public KafkaProducerTransactionState exportCanonical(long expectedMapEndOffset) {
        if (mapEndOffset() != expectedMapEndOffset) {
            throw new IllegalArgumentException(
                    "Kafka producer map end does not match checkpoint offset");
        }
        ArrayList<KafkaProducerTransactionState.ProducerState> producers =
                new ArrayList<>();
        activeProducers().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> producers.add(exportProducer(entry.getValue())));
        ArrayList<KafkaProducerTransactionState.OpenTransaction> open =
                new ArrayList<>();
        for (KafkaProducerTransactionState.ProducerState producer : producers) {
            producer.currentTransactionFirstOffset().ifPresent(firstOffset ->
                    open.add(new KafkaProducerTransactionState.OpenTransaction(
                            producer.producerId(), firstOffset, OptionalLong.empty())));
        }
        open.sort(Comparator
                .comparingLong(KafkaProducerTransactionState.OpenTransaction::firstOffset)
                .thenComparingLong(
                        KafkaProducerTransactionState.OpenTransaction::producerId));
        List<KafkaProducerTransactionState.AbortedTransaction> aborted =
                transactionIndex.allAbortedTxns().stream()
                        .map(transaction ->
                                new KafkaProducerTransactionState.AbortedTransaction(
                                        transaction.version(),
                                        transaction.producerId(),
                                        transaction.firstOffset(),
                                        transaction.lastOffset(),
                                        transaction.lastStableOffset()))
                        .toList();
        return new KafkaProducerTransactionState(
                expectedMapEndOffset, producers, open, aborted);
    }

    public List<AbortedTxn> collectAbortedTransactions(
            long fetchOffset,
            long upperBoundOffset
    ) {
        return transactionIndex.collectAbortedTxns(
                fetchOffset, upperBoundOffset).abortedTransactions();
    }

    @Override
    public void takeSnapshot() {
        // The partition checkpoint coordinator owns durable NKC1 publication.
    }

    @Override
    public Optional<File> takeSnapshot(boolean sync) {
        return Optional.empty();
    }

    private static KafkaProducerTransactionState.ProducerState exportProducer(
            ProducerStateEntry producer
    ) {
        List<KafkaProducerTransactionState.BatchMetadata> batches =
                producer.batchMetadata().stream()
                        .map(NereusProducerStateManager::exportBatch)
                        .toList();
        return new KafkaProducerTransactionState.ProducerState(
                producer.producerId(),
                producer.producerEpoch(),
                producer.coordinatorEpoch(),
                producer.lastTimestamp(),
                producer.currentTxnFirstOffset(),
                batches);
    }

    private static KafkaProducerTransactionState.BatchMetadata exportBatch(
            BatchMetadata batch
    ) {
        return new KafkaProducerTransactionState.BatchMetadata(
                batch.lastSeq(),
                batch.lastOffset(),
                batch.offsetDelta(),
                batch.timestamp());
    }

    private static BatchMetadata importBatch(
            KafkaProducerTransactionState.BatchMetadata batch
    ) {
        return new BatchMetadata(
                batch.lastSequence(),
                batch.lastOffset(),
                batch.offsetDelta(),
                batch.timestamp());
    }
}
