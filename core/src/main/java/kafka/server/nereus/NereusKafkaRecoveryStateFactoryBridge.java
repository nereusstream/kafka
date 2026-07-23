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

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.recovery.KafkaPartitionRecoveryRequest;
import com.nereusstream.kafka.recovery.KafkaRecoveryState;
import com.nereusstream.kafka.recovery.KafkaRecoveryStateFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * One-time bridge between pre-ReplicaManager provider construction and fork-owned fresh Kafka state.
 */
public final class NereusKafkaRecoveryStateFactoryBridge
        implements KafkaRecoveryStateFactory {
    private final AtomicReference<KafkaRecoveryStateFactory> delegate =
            new AtomicReference<>();

    public void bind(KafkaRecoveryStateFactory supplied) {
        KafkaRecoveryStateFactory exact =
                Objects.requireNonNull(supplied, "recoveryStateFactory");
        KafkaRecoveryStateFactory current = delegate.get();
        if (current == exact) {
            return;
        }
        if (!delegate.compareAndSet(null, exact)) {
            throw new IllegalStateException(
                    "Nereus Kafka recovery state factory is already bound");
        }
    }

    public boolean bound() {
        return delegate.get() != null;
    }

    @Override
    public KafkaRecoveryState<?> create(
            KafkaPartitionRecoveryRequest request
    ) {
        Objects.requireNonNull(request, "request");
        KafkaRecoveryStateFactory current = delegate.get();
        if (current == null) {
            throw new NereusException(
                    ErrorCode.METADATA_UNAVAILABLE,
                    true,
                    "Kafka recovery state factory is not bound to ReplicaManager");
        }
        return Objects.requireNonNull(
                current.create(request), "Kafka recovery state factory returned null");
    }
}
