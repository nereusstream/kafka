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

import kafka.server.storage.ControllerStorageRuntime;

import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.server.fault.FaultHandler;

import com.nereusstream.api.NereusException;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Coalesces stock metadata/leadership callbacks into at most one controller activation attempt.
 *
 * <p>Missing images, broker registrations and capabilities are retried only while this node is the current controller.
 * Durable contradictions are reported once per controller epoch and require a later leadership epoch before retry.
 */
final class NereusControllerStorageRuntime implements ControllerStorageRuntime {
    private final int nodeId;
    private final Supplier<NereusKafkaControllerActivation> activationCreator;
    private final Supplier<ScheduledExecutorService> executorCreator;
    private final Duration retryInterval;
    private final FaultHandler faultHandler;

    private boolean started;
    private boolean closed;
    private boolean localController;
    private boolean pending;
    private boolean terminalFailure;
    private int controllerEpoch = -1;
    private NereusKafkaControllerActivation activation;
    private ScheduledExecutorService executor;
    private ScheduledFuture<?> scheduled;
    private CompletableFuture<Void> inFlight;

    NereusControllerStorageRuntime(
            int nodeId,
            Supplier<NereusKafkaControllerActivation> activationCreator,
            Duration retryInterval,
            FaultHandler faultHandler
    ) {
        this(
                nodeId,
                activationCreator,
                () -> Executors.newSingleThreadScheduledExecutor(
                        daemonThreadFactory("nereus-kafka-controller-activation-" + nodeId)),
                retryInterval,
                faultHandler);
    }

    NereusControllerStorageRuntime(
            int nodeId,
            Supplier<NereusKafkaControllerActivation> activationCreator,
            Supplier<ScheduledExecutorService> executorCreator,
            Duration retryInterval,
            FaultHandler faultHandler
    ) {
        if (nodeId < 0) {
            throw new IllegalArgumentException("nodeId must be non-negative");
        }
        this.nodeId = nodeId;
        this.activationCreator =
                Objects.requireNonNull(activationCreator, "activationCreator");
        this.executorCreator =
                Objects.requireNonNull(executorCreator, "executorCreator");
        this.retryInterval = positive(retryInterval, "retryInterval");
        this.faultHandler = Objects.requireNonNull(faultHandler, "faultHandler");
    }

    @Override
    public String name() {
        return "NereusControllerStorageRuntime";
    }

    @Override
    public synchronized CompletionStage<Void> start() {
        if (closed) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            "controller storage runtime is closed"));
        }
        if (started) {
            return CompletableFuture.completedFuture(null);
        }
        ScheduledExecutorService createdExecutor =
                Objects.requireNonNull(
                        executorCreator.get(),
                        "executorCreator returned null");
        try {
            NereusKafkaControllerActivation createdActivation =
                    Objects.requireNonNull(
                            activationCreator.get(),
                            "activationCreator returned null");
            executor = createdExecutor;
            activation = createdActivation;
            started = true;
            if (localController) {
                requestAttempt(Duration.ZERO);
            }
            return CompletableFuture.completedFuture(null);
        } catch (Throwable failure) {
            createdExecutor.shutdownNow();
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public synchronized void onControllerChange(
            LeaderAndEpoch newLeaderAndEpoch
    ) {
        LeaderAndEpoch exact =
                Objects.requireNonNull(
                        newLeaderAndEpoch,
                        "newLeaderAndEpoch");
        boolean wasLocal = localController;
        localController = exact.isLeader(nodeId);
        if (!localController) {
            pending = false;
            cancelScheduled();
            return;
        }
        if (!wasLocal || controllerEpoch != exact.epoch()) {
            controllerEpoch = exact.epoch();
            terminalFailure = false;
        }
        if (started) {
            requestAttempt(Duration.ZERO);
        }
    }

    @Override
    public synchronized void onMetadataUpdate(
            MetadataDelta delta,
            MetadataImage newImage,
            LoaderManifest manifest
    ) {
        Objects.requireNonNull(delta, "delta");
        Objects.requireNonNull(newImage, "newImage");
        Objects.requireNonNull(manifest, "manifest");
        if (started && localController) {
            requestAttempt(Duration.ZERO);
        }
    }

    private void requestAttempt(Duration delay) {
        if (closed
                || !started
                || !localController
                || terminalFailure) {
            return;
        }
        pending = true;
        if (inFlight != null || scheduled != null) {
            return;
        }
        scheduled = executor.schedule(
                this::runAttempt,
                delay.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    private void runAttempt() {
        NereusKafkaControllerActivation exactActivation;
        synchronized (this) {
            scheduled = null;
            if (closed
                    || !started
                    || !localController
                    || terminalFailure) {
                return;
            }
            if (inFlight != null) {
                pending = true;
                return;
            }
            pending = false;
            exactActivation = activation;
        }

        final CompletableFuture<Void> attempt;
        try {
            CompletionStage<Void> supplied = Objects.requireNonNull(
                    exactActivation.activate(),
                    "activation returned null CompletionStage");
            attempt = supplied.toCompletableFuture();
        } catch (Throwable failure) {
            completeAttempt(null, failure);
            return;
        }
        synchronized (this) {
            if (closed) {
                return;
            }
            if (inFlight != null) {
                throw new IllegalStateException(
                        "controller activation already has an in-flight attempt");
            }
            inFlight = attempt;
        }
        attempt.whenComplete(
                (ignored, failure) -> completeAttempt(attempt, failure));
    }

    private void completeAttempt(
            CompletableFuture<Void> attempt,
            Throwable suppliedFailure
    ) {
        Throwable failure =
                suppliedFailure == null ? null : unwrap(suppliedFailure);
        boolean reportFailure = false;
        synchronized (this) {
            if (attempt != null) {
                if (inFlight != attempt) {
                    return;
                }
                inFlight = null;
            }
            if (closed || !started || !localController) {
                return;
            }
            if (failure == null) {
                if (pending) {
                    requestAttempt(Duration.ZERO);
                }
            } else if (isRetriable(failure)) {
                requestAttempt(retryInterval);
            } else if (!terminalFailure) {
                terminalFailure = true;
                pending = false;
                reportFailure = true;
            }
        }
        if (reportFailure) {
            faultHandler.handleFault(
                    "Nereus Kafka first activation failed durably",
                    failure);
        }
    }

    @Override
    public void close() {
        NereusKafkaControllerActivation exactActivation;
        ScheduledExecutorService exactExecutor;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            localController = false;
            pending = false;
            cancelScheduled();
            exactActivation = activation;
            exactExecutor = executor;
            activation = null;
            executor = null;
        }
        Throwable failure = null;
        if (exactActivation != null) {
            try {
                exactActivation.close();
            } catch (Throwable closeFailure) {
                failure = closeFailure;
            }
        }
        if (exactExecutor != null) {
            exactExecutor.shutdownNow();
        }
        if (failure != null) {
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new RuntimeException(
                    "failed to close controller storage runtime",
                    failure);
        }
    }

    private void cancelScheduled() {
        if (scheduled != null) {
            scheduled.cancel(false);
            scheduled = null;
        }
    }

    private static boolean isRetriable(Throwable failure) {
        return failure instanceof NereusException nereus
                && nereus.retriable();
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = Objects.requireNonNull(supplied, "supplied");
        while ((current instanceof CompletionException
                        || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isNegative()
                || value.isZero()
                || value.toMillis() <= 0) {
            throw new IllegalArgumentException(
                    name + " must be positive and millisecond-representable");
        }
        return value;
    }

    private static ThreadFactory daemonThreadFactory(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        };
    }
}
