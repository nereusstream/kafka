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
import com.nereusstream.kafka.recovery.KafkaPartitionRecoveryRequest;
import com.nereusstream.kafka.recovery.KafkaRecoveryState;
import com.nereusstream.kafka.recovery.KafkaRecoveryStateFactory;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NereusKafkaRecoveryStateFactoryBridgeTest {
    @Test
    void failsRetriablyBeforeBinding() {
        NereusKafkaRecoveryStateFactoryBridge bridge =
                new NereusKafkaRecoveryStateFactoryBridge();

        NereusException failure = assertThrows(
                NereusException.class,
                () -> bridge.create(request()));

        assertFalse(bridge.bound());
        assertTrue(failure.retriable());
    }

    @Test
    void bindsExactlyOneFactoryAndPreservesReturnedState() {
        NereusKafkaRecoveryStateFactoryBridge bridge =
                new NereusKafkaRecoveryStateFactoryBridge();
        KafkaRecoveryState<?> expected =
                org.mockito.Mockito.mock(KafkaRecoveryState.class);
        KafkaRecoveryStateFactory factory = ignored -> expected;

        bridge.bind(factory);
        bridge.bind(factory);

        assertTrue(bridge.bound());
        assertSame(expected, bridge.create(request()));
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
