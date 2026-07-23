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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.partition.KafkaPartitionLeaderOpenRequest;
import com.nereusstream.kafka.partition.KafkaPartitionStorageManager;
import com.nereusstream.kafka.runtime.DrainReason;
import com.nereusstream.kafka.runtime.KafkaStorageAdmission;
import com.nereusstream.kafka.runtime.NereusKafkaRuntime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.KafkaScheduler;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class NereusKafkaDeferredRuntimeTest {
    @Test
    void waitsForExactBrokerEpochWithoutProviderIoAtConstruction() throws Exception {
        KafkaScheduler scheduler = startedScheduler();
        try {
            AtomicLong brokerEpoch = new AtomicLong(-1);
            AtomicLong observedEpoch = new AtomicLong(-1);
            AtomicInteger creations = new AtomicInteger();
            KafkaPartitionStorageManager manager =
                    mock(KafkaPartitionStorageManager.class);
            when(manager.shutdown()).thenReturn(
                    CompletableFuture.completedFuture(null));
            NereusKafkaRuntime product = readyRuntime(manager);
            NereusKafkaDeferredRuntime deferred = deferred(
                    scheduler,
                    brokerEpoch,
                    epoch -> {
                        observedEpoch.set(epoch);
                        creations.incrementAndGet();
                        return product;
                    });

            assertEquals(0, creations.get());
            CompletableFuture<Void> start = deferred.start().toCompletableFuture();
            CompletableFuture<Void> shutdown =
                    deferred.partitionStorageManager().shutdown();
            assertFalse(start.isDone());
            assertFalse(shutdown.isDone());

            brokerEpoch.set(37);
            start.get(10, TimeUnit.SECONDS);
            shutdown.get(10, TimeUnit.SECONDS);

            assertEquals(1, creations.get());
            assertEquals(37, observedEpoch.get());
            assertSame(product.admission(), deferred.admission());
            verify(manager).shutdown();

            deferred.close();
            verify(product).close();
        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    void closeCancelsEpochWaitAndPreventsLateCreation() throws Exception {
        KafkaScheduler scheduler = startedScheduler();
        try {
            AtomicLong brokerEpoch = new AtomicLong(-1);
            AtomicInteger creations = new AtomicInteger();
            NereusKafkaDeferredRuntime deferred = deferred(
                    scheduler,
                    brokerEpoch,
                    ignored -> {
                        creations.incrementAndGet();
                        return readyRuntime(mock(
                                KafkaPartitionStorageManager.class));
                    });

            CompletableFuture<Void> start = deferred.start().toCompletableFuture();
            deferred.close();
            brokerEpoch.set(41);

            CompletionException failure = assertThrows(
                    CompletionException.class, start::join);
            assertTrue(failure.getCause() instanceof NereusException);
            assertEquals(
                    ErrorCode.STORAGE_CLOSED,
                    ((NereusException) failure.getCause()).code());
            deferred.partitionStorageManager().shutdown().get(
                    10, TimeUnit.SECONDS);
            Thread.sleep(100);
            assertEquals(0, creations.get());
        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    void managerRechecksProductAdmissionAfterDeferredStartup() throws Exception {
        KafkaScheduler scheduler = startedScheduler();
        try {
            KafkaPartitionStorageManager manager =
                    mock(KafkaPartitionStorageManager.class);
            NereusKafkaRuntime product = readyRuntime(manager);
            NereusKafkaDeferredRuntime deferred = deferred(
                    scheduler,
                    new AtomicLong(43),
                    ignored -> product);
            deferred.start().toCompletableFuture().get(10, TimeUnit.SECONDS);
            product.admission().markNotReady("heartbeat failed");
            KafkaPartitionLeaderOpenRequest request =
                    mock(KafkaPartitionLeaderOpenRequest.class);

            CompletionException failure = assertThrows(
                    CompletionException.class,
                    () -> deferred.partitionStorageManager()
                            .openLeader(request)
                            .join());

            assertTrue(failure.getCause() instanceof NereusException);
            assertEquals(
                    ErrorCode.METADATA_UNAVAILABLE,
                    ((NereusException) failure.getCause()).code());
            verify(manager, never()).openLeader(request);
            deferred.close();
        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    void productStartupFailureDrainsAndClosesCreatedRuntime() throws Exception {
        KafkaScheduler scheduler = startedScheduler();
        try {
            IllegalStateException expected =
                    new IllegalStateException("product startup failed");
            NereusKafkaRuntime product = readyRuntime(
                    mock(KafkaPartitionStorageManager.class));
            when(product.start()).thenReturn(
                    CompletableFuture.failedFuture(expected));
            when(product.beginDrain(DrainReason.STARTUP_FAILURE)).thenReturn(
                    CompletableFuture.completedFuture(null));
            NereusKafkaDeferredRuntime deferred = deferred(
                    scheduler,
                    new AtomicLong(47),
                    ignored -> product);

            CompletionException failure = assertThrows(
                    CompletionException.class,
                    () -> deferred.start().toCompletableFuture().join());

            assertSame(expected, failure.getCause());
            verify(product).beginDrain(DrainReason.STARTUP_FAILURE);
            verify(product).close();
        } finally {
            scheduler.shutdown();
        }
    }

    private static KafkaScheduler startedScheduler() {
        KafkaScheduler scheduler = new KafkaScheduler(1);
        scheduler.startup();
        return scheduler;
    }

    private static NereusKafkaDeferredRuntime deferred(
            KafkaScheduler scheduler,
            AtomicLong brokerEpoch,
            java.util.function.LongFunction<NereusKafkaRuntime> creator
    ) {
        return new NereusKafkaDeferredRuntime(
                brokerEpoch::get,
                scheduler,
                Time.SYSTEM,
                Duration.ofSeconds(5),
                creator,
                new NereusKafkaPartitionRecoveryLauncherBridge());
    }

    private static NereusKafkaRuntime readyRuntime(
            KafkaPartitionStorageManager manager
    ) {
        NereusKafkaRuntime runtime = mock(NereusKafkaRuntime.class);
        KafkaStorageAdmission admission = new KafkaStorageAdmission();
        admission.markReady();
        when(runtime.admission()).thenReturn(admission);
        when(runtime.partitionStorageManager()).thenReturn(manager);
        when(runtime.start()).thenReturn(CompletableFuture.completedFuture(null));
        return runtime;
    }
}
