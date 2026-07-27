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

import org.apache.kafka.storage.internals.log.AbortedTxn;
import org.apache.kafka.storage.internals.log.CorruptIndexException;
import org.apache.kafka.storage.internals.log.TransactionIndex;
import org.apache.kafka.storage.internals.log.TxnIndexSearchResult;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/** Ephemeral ordered aborted-transaction index whose restart image comes only from NKC1/replay. */
public final class NereusTransactionIndex extends TransactionIndex {
    private final long startOffset;
    private final ArrayList<AbortedTxn> transactions = new ArrayList<>();
    private File file;

    public NereusTransactionIndex(long startOffset, File file) throws IOException {
        super(startOffset, file);
        if (startOffset < 0) {
            throw new IllegalArgumentException("Nereus transaction index start offset must be non-negative");
        }
        super.close();
        Files.deleteIfExists(file.toPath());
        this.startOffset = startOffset;
        this.file = file;
    }

    @Override
    public synchronized File file() {
        return file;
    }

    @Override
    public synchronized void updateParentDir(File parentDir) {
        file = new File(parentDir, file.getName());
    }

    @Override
    public synchronized void append(AbortedTxn abortedTxn) {
        AbortedTxn exact = copy(abortedTxn);
        if (exact.version() != AbortedTxn.CURRENT_VERSION) {
            throw new IllegalArgumentException("Unsupported Kafka aborted transaction version");
        }
        if (!transactions.isEmpty()
                && transactions.get(transactions.size() - 1).lastOffset() >= exact.lastOffset()) {
            throw new IllegalArgumentException(
                    "Nereus aborted transaction marker offsets must be strictly increasing");
        }
        transactions.add(exact);
    }

    @Override
    public void flush() {
        // NKC1 is the only durable snapshot. There is no local index to flush.
    }

    @Override
    public synchronized void reset() {
        transactions.clear();
    }

    @Override
    public void close() {
        // No file/channel ownership.
    }

    @Override
    public synchronized boolean deleteIfExists() throws IOException {
        transactions.clear();
        return Files.deleteIfExists(file.toPath());
    }

    @Override
    public synchronized void renameTo(File renamed) throws IOException {
        Files.deleteIfExists(file.toPath());
        file = renamed;
        Files.deleteIfExists(renamed.toPath());
    }

    @Override
    public synchronized void truncateTo(long offset) {
        transactions.removeIf(transaction -> transaction.lastOffset() >= offset);
    }

    @Override
    public synchronized List<AbortedTxn> allAbortedTxns() {
        return transactions.stream().map(NereusTransactionIndex::copy).toList();
    }

    @Override
    public synchronized TxnIndexSearchResult collectAbortedTxns(
            long fetchOffset,
            long upperBoundOffset
    ) {
        ArrayList<AbortedTxn> selected = new ArrayList<>();
        boolean complete = false;
        for (AbortedTxn transaction : transactions) {
            if (transaction.lastOffset() >= fetchOffset
                    && transaction.firstOffset() < upperBoundOffset) {
                selected.add(copy(transaction));
            }
            if (transaction.lastStableOffset() >= upperBoundOffset) {
                complete = true;
                break;
            }
        }
        return new TxnIndexSearchResult(selected, complete);
    }

    @Override
    public synchronized void sanityCheck() {
        long previousLastOffset = -1;
        for (AbortedTxn transaction : transactions) {
            if (transaction.lastOffset() < startOffset
                    || transaction.lastOffset() <= previousLastOffset) {
                throw new CorruptIndexException("Invalid Nereus aborted transaction index order");
            }
            previousLastOffset = transaction.lastOffset();
        }
    }

    @Override
    public synchronized boolean isEmpty() {
        return transactions.isEmpty();
    }

    private static AbortedTxn copy(AbortedTxn transaction) {
        if (transaction == null) {
            throw new NullPointerException("abortedTxn");
        }
        return new AbortedTxn(
                transaction.producerId(),
                transaction.firstOffset(),
                transaction.lastOffset(),
                transaction.lastStableOffset());
    }
}
