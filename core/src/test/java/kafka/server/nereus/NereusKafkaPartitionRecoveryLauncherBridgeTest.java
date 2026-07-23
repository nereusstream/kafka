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

import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.recovery.KafkaCheckpointRecoveryRequest;
import com.nereusstream.kafka.recovery.KafkaPartitionRecoveryLauncher;
import com.nereusstream.kafka.recovery.KafkaPartitionRecoveryRequest;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NereusKafkaPartitionRecoveryLauncherBridgeTest {
    @Test
    void failsRetriablyBeforeBinding() {
        NereusKafkaPartitionRecoveryLauncherBridge bridge =
                new NereusKafkaPartitionRecoveryLauncherBridge();

        CompletionException failure = assertThrows(
                CompletionException.class,
                () -> bridge.recover(request()).join());

        assertFalse(bridge.bound());
        assertTrue(failure.getCause() instanceof NereusException);
        assertTrue(((NereusException) failure.getCause()).retriable());
    }

    @Test
    void bindsExactlyOneLauncherAndPreservesReturnedFuture() {
        NereusKafkaPartitionRecoveryLauncherBridge bridge =
                new NereusKafkaPartitionRecoveryLauncherBridge();
        CompletableFuture<com.nereusstream.kafka.recovery.KafkaRecoveredPartition<?>>
                expected = new CompletableFuture<>();
        KafkaPartitionRecoveryLauncher launcher = ignored -> expected;

        bridge.bind(launcher);
        bridge.bind(launcher);

        assertTrue(bridge.bound());
        assertSame(expected, bridge.recover(request()));
        assertThrows(
                IllegalStateException.class,
                () -> bridge.bind(ignored -> expected));
    }

    private static KafkaPartitionRecoveryRequest request() {
        return new KafkaPartitionRecoveryRequest(
                org.mockito.Mockito.mock(KafkaCheckpointRecoveryRequest.class),
                Duration.ofSeconds(1));
    }
}
