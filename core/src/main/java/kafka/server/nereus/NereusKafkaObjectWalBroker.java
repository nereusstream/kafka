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

import kafka.server.storage.BrokerStorageRuntimeContext;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.metadata.TopicBindingAggregateRecord;
import org.apache.kafka.server.config.NereusKafkaStorageConfig.Profile;

import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionPublicationObserver;
import com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/** Owns only Object WAL resources; the broker keeps using the same BookKeeper runtime for internal topics. */
public final class NereusKafkaObjectWalBroker implements AutoCloseable {
    private final BrokerStorageRuntimeContext context;
    private final NereusKafkaOwnedProviderRuntime bookKeeper;
    private volatile NereusKafkaObjectWalCell cell;
    private boolean fenced;

    NereusKafkaObjectWalBroker(BrokerStorageRuntimeContext context, NereusKafkaOwnedProviderRuntime bookKeeper) {
        this.context = context;
        this.bookKeeper = bookKeeper;
    }

    synchronized void start() {
        if (context.config().nereusKafkaStorageConfig().core().profile() != Profile.OBJECT_WAL_SYNC_OBJECT) return;
        if (fenced) throw new IllegalStateException("native Object WAL Broker is draining");
        try {
            cell = NereusKafkaObjectWalCell.start(context, bookKeeper.physicalNamespace(), bookKeeper.controlClient(),
                    bookKeeper.recoveryExecutor());
        } catch (Exception failure) {
            throw new CompletionException(failure);
        }
    }

    public static boolean selectsObjectWal(TopicBindingAggregateRecord aggregate) {
        return NereusKafkaObjectWalCell.selectsObjectWal(aggregate);
    }

    public CompletionStage<NereusKafkaObjectWalPartition> open(
            TopicIdPartition identity, TopicBindingAggregateRecord aggregate, int leaderEpoch,
            long brokerEpoch, long metadataOffset, NereusKafkaObjectWalPartition.NativeAssignment assignment,
            KafkaPartitionPublicationObserver observer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                var admitted = cell;
                if (admitted == null) throw new IllegalStateException("native Object WAL Binding lacks its admitted Cell");
                var owner = new KafkaOwnerIdentityV1(Math.addExact((long) leaderEpoch, 1), leaderEpoch,
                        context.config().brokerId(), brokerEpoch, metadataOffset);
                return admitted.open(identity, aggregate, owner, assignment, observer);
            } catch (Exception failure) {
                throw new CompletionException(failure);
            }
        }, bookKeeper.recoveryExecutor());
    }

    public CompletableFuture<Void> closePartition(NereusKafkaObjectWalPartition partition) {
        return partition.closeAsync().thenRun(() -> {
            var admitted = cell;
            if (admitted != null) admitted.forget(partition);
        });
    }

    public CompletableFuture<Void> sealBeforeHandoff(NereusKafkaObjectWalPartition partition) {
        return partition.sealBeforeHandoff(bookKeeper.recoveryExecutor());
    }

    synchronized void fence() {
        fenced = true;
        var admitted = cell;
        if (admitted != null) admitted.fence();
    }

    @Override
    public void close() throws Exception {
        fence();
        var admitted = cell;
        if (admitted != null) {
            admitted.close();
            cell = null;
        }
    }
}
