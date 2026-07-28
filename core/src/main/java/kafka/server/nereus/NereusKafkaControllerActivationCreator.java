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

import com.nereusstream.kafka.activation.KafkaStorageBindingAwareClusterSnapshotProvider;
import com.nereusstream.kafka.activation.KafkaStorageClusterSnapshotProvider;
import com.nereusstream.kafka.activation.KafkaStorageFirstActivationCoordinator;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.KafkaStorageActivationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaKafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/** Opens the controller's minimal shared-Oxia graph and owns it independently of the broker runtime. */
final class NereusKafkaControllerActivationCreator {
    NereusKafkaControllerActivation create(
            NereusKafkaControllerRuntimeConfiguration configuration,
            KafkaStorageClusterSnapshotProvider clusterSnapshots,
            Clock clock
    ) {
        NereusKafkaControllerRuntimeConfiguration exact =
                Objects.requireNonNull(configuration, "configuration");
        KafkaStorageClusterSnapshotProvider exactSnapshots =
                Objects.requireNonNull(clusterSnapshots, "clusterSnapshots");
        Clock exactClock = Objects.requireNonNull(clock, "clock");
        List<AutoCloseable> resources = new ArrayList<>();
        try {
            SharedOxiaClientRuntime oxiaRuntime =
                    SharedOxiaClientRuntime.connect(exact.oxia(), exactClock);
            resources.add(oxiaRuntime);
            KafkaPartitionMetadataStore partitionStore =
                    OxiaJavaKafkaPartitionMetadataStore.usingSharedRuntime(
                            exact.oxia(),
                            oxiaRuntime,
                            exact.nereusCluster(),
                            exact.kafkaClusterId());
            resources.add(partitionStore);
            KafkaStorageActivationMetadataStore activationStore =
                    KafkaStorageActivationMetadataStore.usingSharedRuntime(
                            exact.oxia(),
                            oxiaRuntime,
                            exact.nereusCluster(),
                            exact.kafkaClusterId());
            resources.add(activationStore);
            KafkaStorageBindingAwareClusterSnapshotProvider bindingAwareSnapshots =
                    new KafkaStorageBindingAwareClusterSnapshotProvider(
                            exactSnapshots, partitionStore);
            KafkaStorageFirstActivationCoordinator coordinator =
                    new KafkaStorageFirstActivationCoordinator(
                            activationStore,
                            bindingAwareSnapshots,
                            exact.activationPolicy(),
                            exactClock);
            return new OwnedActivation(coordinator, resources);
        } catch (Throwable failure) {
            closeReverse(resources, failure);
            throw failure;
        }
    }

    private static final class OwnedActivation
            implements NereusKafkaControllerActivation {
        private final KafkaStorageFirstActivationCoordinator coordinator;
        private final List<AutoCloseable> resources;
        private final AtomicBoolean closed = new AtomicBoolean();

        private OwnedActivation(
                KafkaStorageFirstActivationCoordinator coordinator,
                List<AutoCloseable> resources
        ) {
            this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
            this.resources = List.copyOf(resources);
        }

        @Override
        public CompletionStage<Void> activate() {
            if (closed.get()) {
                return java.util.concurrent.CompletableFuture.failedFuture(
                        new IllegalStateException(
                                "controller activation resources are closed"));
            }
            return coordinator.activate().thenApply(ignored -> null);
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            Throwable failure = closeReverse(resources, null);
            if (failure != null) {
                if (failure instanceof RuntimeException runtime) {
                    throw runtime;
                }
                throw new RuntimeException(
                        "failed to close controller activation resources",
                        failure);
            }
        }
    }

    private static Throwable closeReverse(
            List<? extends AutoCloseable> resources,
            Throwable suppliedFailure
    ) {
        Throwable failure = suppliedFailure;
        for (int index = resources.size() - 1; index >= 0; index--) {
            try {
                resources.get(index).close();
            } catch (Throwable closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        return failure;
    }
}
