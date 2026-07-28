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

import com.nereusstream.kafka.partition.KafkaPartitionStorageManager;
import com.nereusstream.kafka.runtime.DrainReason;
import com.nereusstream.kafka.runtime.KafkaStorageAdmission;
import com.nereusstream.kafka.runtime.KafkaStorageHealth;
import com.nereusstream.kafka.runtime.NereusKafkaRuntime;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Fork-owned resource wrapper around a product runtime.
 *
 * <p>The product graph closes first because it borrows the provider resource. The wrapper then
 * closes that provider exactly once, even when the product close fails.
 */
final class NereusKafkaOwnedProviderRuntime implements NereusKafkaRuntime {
    private final NereusKafkaRuntime delegate;
    private final AutoCloseable ownedProvider;
    private final AtomicBoolean closed = new AtomicBoolean();

    NereusKafkaOwnedProviderRuntime(
            NereusKafkaRuntime delegate,
            AutoCloseable ownedProvider
    ) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.ownedProvider = Objects.requireNonNull(ownedProvider, "ownedProvider");
    }

    @Override
    public CompletionStage<Void> start() {
        return delegate.start();
    }

    @Override
    public KafkaStorageAdmission admission() {
        return delegate.admission();
    }

    @Override
    public KafkaPartitionStorageManager partitionStorageManager() {
        return delegate.partitionStorageManager();
    }

    @Override
    public KafkaStorageHealth health() {
        return delegate.health();
    }

    @Override
    public CompletionStage<Void> beginDrain(DrainReason reason) {
        return delegate.beginDrain(reason);
    }

    @Override
    public CompletionStage<Void> awaitDrained(Duration timeout) {
        return delegate.awaitDrained(timeout);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        Throwable failure = null;
        try {
            delegate.close();
        } catch (Throwable closeFailure) {
            failure = closeFailure;
        }
        try {
            ownedProvider.close();
        } catch (Throwable closeFailure) {
            if (failure == null) {
                failure = closeFailure;
            } else {
                failure.addSuppressed(closeFailure);
            }
        }
        if (failure != null) {
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException(
                    "failed to close a fork-owned Nereus provider", failure);
        }
    }
}
