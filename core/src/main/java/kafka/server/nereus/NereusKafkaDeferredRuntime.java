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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.KafkaScheduler;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.partition.KafkaPartitionLeaderOpenRequest;
import com.nereusstream.kafka.partition.KafkaPartitionStorage;
import com.nereusstream.kafka.partition.KafkaPartitionStorageManager;
import com.nereusstream.kafka.recovery.KafkaRecoveryStateFactory;
import com.nereusstream.kafka.runtime.DrainReason;
import com.nereusstream.kafka.runtime.KafkaStorageAdmission;
import com.nereusstream.kafka.runtime.KafkaStorageHealth;
import com.nereusstream.kafka.runtime.NereusKafkaRuntime;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledFuture;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;

/**
 * Defers provider I/O until KRaft has assigned the broker registration epoch.
 *
 * <p>The manager proxy is available before the first metadata publisher is installed. Lifecycle operations wait for
 * activation-backed runtime readiness and re-check the real runtime admission gate before dispatch.
 */
public final class NereusKafkaDeferredRuntime implements NereusKafkaRuntime {
    private static final long BROKER_EPOCH_POLL_MILLIS = 25;

    private final Object guard = new Object();
    private final LongSupplier brokerEpochSupplier;
    private final KafkaScheduler scheduler;
    private final Time time;
    private final Duration brokerEpochWaitTimeout;
    private final LongFunction<NereusKafkaRuntime> runtimeCreator;
    private final NereusKafkaRecoveryStateFactoryBridge recoveryBridge;
    private final NereusKafkaOwnedPartitionSourceBridge ownedPartitions;
    private final KafkaStorageAdmission admission = new KafkaStorageAdmission();
    private final CompletableFuture<NereusKafkaRuntime> readyRuntime =
            new CompletableFuture<>();
    private final KafkaPartitionStorageManager manager =
            new DeferredPartitionStorageManager();
    private CompletableFuture<Void> startOperation;
    private ScheduledFuture<?> pendingEpochPoll;
    private NereusKafkaRuntime delegate;
    private long brokerEpochDeadlineMillis;
    private boolean creating;
    private boolean draining;
    private boolean closed;

    public NereusKafkaDeferredRuntime(
            LongSupplier brokerEpochSupplier,
            KafkaScheduler scheduler,
            Time time,
            Duration brokerEpochWaitTimeout,
            LongFunction<NereusKafkaRuntime> runtimeCreator,
            NereusKafkaRecoveryStateFactoryBridge recoveryBridge
    ) {
        this(
                brokerEpochSupplier,
                scheduler,
                time,
                brokerEpochWaitTimeout,
                runtimeCreator,
                recoveryBridge,
                new NereusKafkaOwnedPartitionSourceBridge());
    }

    public NereusKafkaDeferredRuntime(
            LongSupplier brokerEpochSupplier,
            KafkaScheduler scheduler,
            Time time,
            Duration brokerEpochWaitTimeout,
            LongFunction<NereusKafkaRuntime> runtimeCreator,
            NereusKafkaRecoveryStateFactoryBridge recoveryBridge,
            NereusKafkaOwnedPartitionSourceBridge ownedPartitions
    ) {
        this.brokerEpochSupplier = Objects.requireNonNull(
                brokerEpochSupplier, "brokerEpochSupplier");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.time = Objects.requireNonNull(time, "time");
        this.brokerEpochWaitTimeout = positive(
                brokerEpochWaitTimeout, "brokerEpochWaitTimeout");
        this.runtimeCreator = Objects.requireNonNull(
                runtimeCreator, "runtimeCreator");
        this.recoveryBridge = Objects.requireNonNull(
                recoveryBridge, "recoveryBridge");
        this.ownedPartitions = Objects.requireNonNull(
                ownedPartitions, "ownedPartitions");
    }

    public void bindRecoveryStateFactory(KafkaRecoveryStateFactory stateFactory) {
        recoveryBridge.bind(stateFactory);
    }

    public void bindReplicaManager(kafka.server.ReplicaManager replicaManager) {
        ownedPartitions.bind(replicaManager);
    }

    @Override
    public CompletionStage<Void> start() {
        synchronized (guard) {
            if (closed || draining) {
                return CompletableFuture.failedFuture(closedFailure());
            }
            if (startOperation != null) {
                return startOperation.copy();
            }
            startOperation = new CompletableFuture<>();
            try {
                brokerEpochDeadlineMillis = Math.addExact(
                        time.milliseconds(),
                        brokerEpochWaitTimeout.toMillis());
            } catch (ArithmeticException failure) {
                startOperation.completeExceptionally(new IllegalArgumentException(
                        "broker epoch wait deadline overflows", failure));
                return startOperation.copy();
            }
            scheduleEpochAttempt(0);
            return startOperation.copy();
        }
    }

    private void scheduleEpochAttempt(long delayMillis) {
        try {
            pendingEpochPoll = scheduler.scheduleOnce(
                    "nereus-kafka-broker-epoch",
                    this::attemptRuntimeCreation,
                    delayMillis);
        } catch (Throwable failure) {
            failStart(failure);
        }
    }

    private void attemptRuntimeCreation() {
        if (!beginCreationAttempt()) {
            return;
        }
        long brokerEpoch;
        try {
            brokerEpoch = brokerEpochSupplier.getAsLong();
        } catch (Throwable failure) {
            failStart(failure);
            return;
        }
        if (brokerEpoch < 0) {
            waitForBrokerEpoch();
            return;
        }
        createRuntime(brokerEpoch);
    }

    private boolean beginCreationAttempt() {
        synchronized (guard) {
            pendingEpochPoll = null;
            return !closed
                    && !draining
                    && !creating
                    && startOperation != null
                    && !startOperation.isDone();
        }
    }

    private void waitForBrokerEpoch() {
        long remaining = brokerEpochDeadlineMillis - time.milliseconds();
        if (remaining <= 0) {
            failStart(new NereusException(
                    ErrorCode.TIMEOUT,
                    true,
                    "timed out waiting for KRaft broker registration epoch"));
            return;
        }
        synchronized (guard) {
            if (!closed && !draining && !startOperation.isDone()) {
                scheduleEpochAttempt(Math.min(
                        BROKER_EPOCH_POLL_MILLIS, remaining));
            }
        }
    }

    private void createRuntime(long brokerEpoch) {
        synchronized (guard) {
            if (closed || draining || startOperation.isDone()) {
                return;
            }
            creating = true;
        }
        NereusKafkaRuntime created;
        try {
            created = Objects.requireNonNull(
                    runtimeCreator.apply(brokerEpoch),
                    "Nereus product runtime creator returned null");
        } catch (Throwable failure) {
            synchronized (guard) {
                creating = false;
            }
            failStart(failure);
            return;
        }
        installAndStart(created);
    }

    private void installAndStart(NereusKafkaRuntime created) {
        boolean discard;
        synchronized (guard) {
            creating = false;
            discard = closed || draining || startOperation.isDone();
            if (!discard) {
                delegate = created;
            }
        }
        if (discard) {
            created.close();
            return;
        }
        CompletionStage<Void> startup;
        try {
            startup = Objects.requireNonNull(
                    created.start(), "Nereus product runtime start returned null");
        } catch (Throwable failure) {
            failStart(failure);
            return;
        }
        startup.whenComplete((ignored, failure) -> {
            if (failure != null) {
                failStart(unwrap(failure));
                return;
            }
            completeStart(created);
        });
    }

    private void completeStart(NereusKafkaRuntime created) {
        synchronized (guard) {
            if (closed || draining || startOperation.isDone()) {
                return;
            }
            admission.markReady();
            readyRuntime.complete(created);
            startOperation.complete(null);
        }
    }

    private void failStart(Throwable supplied) {
        Throwable failure = unwrap(supplied);
        NereusKafkaRuntime failedDelegate = null;
        synchronized (guard) {
            admission.markNotReady(
                    "deferred runtime start failed: "
                            + failure.getClass().getSimpleName());
            if (pendingEpochPoll != null) {
                pendingEpochPoll.cancel(false);
                pendingEpochPoll = null;
            }
            readyRuntime.completeExceptionally(failure);
            if (startOperation != null
                    && startOperation.completeExceptionally(failure)) {
                failedDelegate = delegate;
            }
        }
        closeFailedDelegate(failedDelegate);
    }

    private static void closeFailedDelegate(
            NereusKafkaRuntime failedDelegate
    ) {
        if (failedDelegate == null) {
            return;
        }
        try {
            failedDelegate.beginDrain(DrainReason.STARTUP_FAILURE);
        } finally {
            failedDelegate.close();
        }
    }

    @Override
    public KafkaStorageAdmission admission() {
        synchronized (guard) {
            return delegate == null ? admission : delegate.admission();
        }
    }

    @Override
    public KafkaPartitionStorageManager partitionStorageManager() {
        return manager;
    }

    @Override
    public KafkaStorageHealth health() {
        return admission().health();
    }

    @Override
    public CompletionStage<Void> beginDrain(DrainReason reason) {
        DrainReason exact = Objects.requireNonNull(reason, "reason");
        NereusKafkaRuntime current;
        synchronized (guard) {
            draining = true;
            admission.beginDrain(exact);
            if (pendingEpochPoll != null) {
                pendingEpochPoll.cancel(false);
                pendingEpochPoll = null;
            }
            current = delegate;
            NereusException failure = closedFailure();
            readyRuntime.completeExceptionally(failure);
            if (startOperation != null && !startOperation.isDone()) {
                startOperation.completeExceptionally(failure);
            }
        }
        return current == null
                ? CompletableFuture.completedFuture(null)
                : Objects.requireNonNull(
                        current.beginDrain(exact),
                        "Nereus product runtime drain returned null");
    }

    @Override
    public CompletionStage<Void> awaitDrained(Duration timeout) {
        positive(timeout, "timeout");
        NereusKafkaRuntime current;
        synchronized (guard) {
            current = delegate;
        }
        return current == null
                ? CompletableFuture.completedFuture(null)
                : Objects.requireNonNull(
                        current.awaitDrained(timeout),
                        "Nereus product runtime drained future is null");
    }

    @Override
    public void close() {
        NereusKafkaRuntime current;
        synchronized (guard) {
            if (closed) {
                return;
            }
            closed = true;
            draining = true;
            admission.close();
            if (pendingEpochPoll != null) {
                pendingEpochPoll.cancel(false);
                pendingEpochPoll = null;
            }
            NereusException failure = closedFailure();
            readyRuntime.completeExceptionally(failure);
            if (startOperation != null && !startOperation.isDone()) {
                startOperation.completeExceptionally(failure);
            }
            current = delegate;
        }
        if (current != null) {
            current.close();
        }
    }

    private <T> CompletableFuture<T> withReadyRuntime(
            String operation,
            java.util.function.Function<NereusKafkaRuntime, CompletableFuture<T>> action
    ) {
        return readyRuntime.thenCompose(runtime -> {
            runtime.admission().requireReady(operation);
            return Objects.requireNonNull(
                    action.apply(runtime), "deferred manager action returned null");
        });
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative() || value.toMillis() <= 0) {
            throw new IllegalArgumentException(
                    name + " must be positive and millisecond-representable");
        }
        return value;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException closedFailure() {
        return new NereusException(
                ErrorCode.STORAGE_CLOSED,
                false,
                "deferred Nereus Kafka runtime is draining or closed");
    }

    private final class DeferredPartitionStorageManager
            implements KafkaPartitionStorageManager {
        @Override
        public CompletableFuture<KafkaPartitionStorage> openLeader(
                KafkaPartitionLeaderOpenRequest request
        ) {
            Objects.requireNonNull(request, "request");
            return withReadyRuntime(
                    "open leader",
                    runtime -> runtime.partitionStorageManager()
                            .openLeader(request));
        }

        @Override
        public CompletableFuture<Void> resign(
                KafkaPartitionIdentity identity,
                int observedLeaderEpoch,
                Duration timeout
        ) {
            Objects.requireNonNull(identity, "identity");
            positive(timeout, "timeout");
            return withReadyRuntime(
                    "resign leader",
                    runtime -> runtime.partitionStorageManager()
                            .resign(identity, observedLeaderEpoch, timeout));
        }

        @Override
        public CompletableFuture<Void> delete(
                KafkaPartitionIdentity identity,
                long metadataOffset,
                Duration timeout
        ) {
            Objects.requireNonNull(identity, "identity");
            positive(timeout, "timeout");
            return withReadyRuntime(
                    "delete partition",
                    runtime -> runtime.partitionStorageManager()
                            .delete(identity, metadataOffset, timeout));
        }

        @Override
        public Optional<KafkaPartitionStorage> current(
                KafkaPartitionIdentity identity
        ) {
            Objects.requireNonNull(identity, "identity");
            NereusKafkaRuntime current;
            synchronized (guard) {
                current = delegate;
            }
            if (current == null || !current.admission().ready()) {
                return Optional.empty();
            }
            return current.partitionStorageManager().current(identity);
        }

        @Override
        public CompletableFuture<Void> shutdown() {
            return withReadyRuntime(
                    "shutdown partitions",
                    runtime -> runtime.partitionStorageManager().shutdown())
                    .exceptionallyCompose(failure -> {
                        Throwable exact = unwrap(failure);
                        if (exact instanceof NereusException nereus
                                && nereus.code() == ErrorCode.STORAGE_CLOSED) {
                            return CompletableFuture.completedFuture(null);
                        }
                        return CompletableFuture.failedFuture(exact);
                    });
        }

        @Override
        public void close() {
            NereusKafkaDeferredRuntime.this.close();
        }
    }
}
