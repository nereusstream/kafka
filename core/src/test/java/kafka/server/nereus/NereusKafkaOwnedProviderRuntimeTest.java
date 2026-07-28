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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NereusKafkaOwnedProviderRuntimeTest {
    @Test
    void closesProductGraphBeforeOwnedProviderExactlyOnce() {
        List<String> closes = new ArrayList<>();
        NereusKafkaOwnedProviderRuntime runtime =
                new NereusKafkaOwnedProviderRuntime(
                        new StubRuntime(() -> closes.add("runtime")),
                        () -> closes.add("provider"));

        runtime.close();
        runtime.close();

        assertEquals(List.of("runtime", "provider"), closes);
    }

    @Test
    void attemptsProviderCloseWhenProductCloseFails() {
        List<String> closes = new ArrayList<>();
        IllegalStateException productFailure =
                new IllegalStateException("runtime close failed");
        NereusKafkaOwnedProviderRuntime runtime =
                new NereusKafkaOwnedProviderRuntime(
                        new StubRuntime(() -> {
                            closes.add("runtime");
                            throw productFailure;
                        }),
                        () -> closes.add("provider"));

        IllegalStateException thrown =
                assertThrows(IllegalStateException.class, runtime::close);

        assertSame(productFailure, thrown);
        assertEquals(List.of("runtime", "provider"), closes);
    }

    private static final class StubRuntime implements NereusKafkaRuntime {
        private final Runnable close;

        private StubRuntime(Runnable close) {
            this.close = close;
        }

        @Override
        public CompletionStage<Void> start() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public KafkaStorageAdmission admission() {
            return null;
        }

        @Override
        public KafkaPartitionStorageManager partitionStorageManager() {
            return null;
        }

        @Override
        public KafkaStorageHealth health() {
            return null;
        }

        @Override
        public CompletionStage<Void> beginDrain(DrainReason reason) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> awaitDrained(Duration timeout) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
            close.run();
        }
    }
}
