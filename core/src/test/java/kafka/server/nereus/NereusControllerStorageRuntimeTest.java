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

import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.server.fault.FaultHandler;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class NereusControllerStorageRuntimeTest {
    private static final int NODE_ID = 7;

    @Test
    void retriesRetriableFailureOnlyWhileCurrentController() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch first = new CountDownLatch(1);
        CountDownLatch second = new CountDownLatch(1);
        TestActivation activation = new TestActivation(() -> {
            int current = attempts.incrementAndGet();
            if (current == 1) {
                first.countDown();
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.METADATA_UNAVAILABLE,
                        true,
                        "capability not published"));
            }
            second.countDown();
            return CompletableFuture.completedFuture(null);
        });
        NereusControllerStorageRuntime runtime = runtime(
                activation,
                Duration.ofMillis(20),
                (message, failure) -> {
                    throw new AssertionError(
                            "retriable failure reached fault handler",
                            failure);
                });

        runtime.start().toCompletableFuture().join();
        runtime.onControllerChange(leader(NODE_ID, 3));

        assertTrue(first.await(5, TimeUnit.SECONDS));
        assertTrue(second.await(5, TimeUnit.SECONDS));
        assertEquals(2, attempts.get());

        runtime.close();
        assertEquals(1, activation.closes.get());
    }

    @Test
    void leadershipLossCancelsScheduledRetry() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch first = new CountDownLatch(1);
        TestActivation activation = new TestActivation(() -> {
            attempts.incrementAndGet();
            first.countDown();
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.METADATA_UNAVAILABLE,
                    true,
                    "broker registration set is empty"));
        });
        NereusControllerStorageRuntime runtime = runtime(
                activation,
                Duration.ofMillis(500),
                (message, failure) ->
                        new RuntimeException(message, failure));

        runtime.start().toCompletableFuture().join();
        runtime.onControllerChange(leader(NODE_ID, 4));
        assertTrue(first.await(5, TimeUnit.SECONDS));
        runtime.onControllerChange(leader(NODE_ID + 1, 5));

        Thread.sleep(700);
        assertEquals(1, attempts.get());
        runtime.close();
    }

    @Test
    void metadataCallbacksCoalesceBehindOneInFlightAttempt() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        CompletableFuture<Void> firstAttempt = new CompletableFuture<>();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        TestActivation activation = new TestActivation(() -> {
            int current = attempts.incrementAndGet();
            if (current == 1) {
                firstStarted.countDown();
                return firstAttempt;
            }
            secondStarted.countDown();
            return CompletableFuture.completedFuture(null);
        });
        NereusControllerStorageRuntime runtime = runtime(
                activation,
                Duration.ofMillis(20),
                (message, failure) ->
                        new RuntimeException(message, failure));

        runtime.start().toCompletableFuture().join();
        runtime.onControllerChange(leader(NODE_ID, 6));
        assertTrue(firstStarted.await(5, TimeUnit.SECONDS));
        runtime.onMetadataUpdate(
                mock(MetadataDelta.class),
                mock(MetadataImage.class),
                mock(LoaderManifest.class));
        runtime.onMetadataUpdate(
                mock(MetadataDelta.class),
                mock(MetadataImage.class),
                mock(LoaderManifest.class));
        assertEquals(1, attempts.get());

        firstAttempt.complete(null);
        assertTrue(secondStarted.await(5, TimeUnit.SECONDS));
        assertEquals(2, attempts.get());
        runtime.close();
    }

    @Test
    void durableFailureIsReportedOncePerControllerEpoch() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger faults = new AtomicInteger();
        CountDownLatch firstFault = new CountDownLatch(1);
        CountDownLatch secondFault = new CountDownLatch(1);
        TestActivation activation = new TestActivation(() -> {
            attempts.incrementAndGet();
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "activation policy mismatch"));
        });
        FaultHandler handler = (message, failure) -> {
            int current = faults.incrementAndGet();
            if (current == 1) {
                firstFault.countDown();
            } else if (current == 2) {
                secondFault.countDown();
            }
            return new RuntimeException(message, failure);
        };
        NereusControllerStorageRuntime runtime = runtime(
                activation,
                Duration.ofMillis(20),
                handler);

        runtime.start().toCompletableFuture().join();
        runtime.onControllerChange(leader(NODE_ID, 7));
        assertTrue(firstFault.await(5, TimeUnit.SECONDS));
        publishMetadata(runtime);
        runtime.onControllerChange(leader(NODE_ID, 7));
        Thread.sleep(100);
        assertEquals(1, attempts.get());
        assertEquals(1, faults.get());

        runtime.onControllerChange(leader(NODE_ID + 1, 8));
        runtime.onControllerChange(leader(NODE_ID, 9));
        assertTrue(secondFault.await(5, TimeUnit.SECONDS));
        assertEquals(2, attempts.get());
        assertEquals(2, faults.get());
        runtime.close();
    }

    private static NereusControllerStorageRuntime runtime(
            TestActivation activation,
            Duration retryInterval,
            FaultHandler faultHandler
    ) {
        return new NereusControllerStorageRuntime(
                NODE_ID,
                () -> activation,
                retryInterval,
                faultHandler);
    }

    private static LeaderAndEpoch leader(int nodeId, int epoch) {
        return new LeaderAndEpoch(OptionalInt.of(nodeId), epoch);
    }

    private static void publishMetadata(
            NereusControllerStorageRuntime runtime
    ) {
        runtime.onMetadataUpdate(
                mock(MetadataDelta.class),
                mock(MetadataImage.class),
                mock(LoaderManifest.class));
    }

    @FunctionalInterface
    private interface Attempt {
        CompletionStage<Void> run();
    }

    private static final class TestActivation
            implements NereusKafkaControllerActivation {
        private final Attempt attempt;
        private final AtomicInteger closes = new AtomicInteger();

        private TestActivation(Attempt attempt) {
            this.attempt = attempt;
        }

        @Override
        public CompletionStage<Void> activate() {
            return attempt.run();
        }

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }
}
