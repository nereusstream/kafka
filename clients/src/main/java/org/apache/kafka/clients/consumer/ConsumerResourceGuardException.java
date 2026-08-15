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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.KafkaException;

import java.util.Objects;

/** Typed fail-closed failure from a guarded Kafka Consumer operation. */
public final class ConsumerResourceGuardException extends KafkaException {
    private final ConsumerResourceGuardFailureReason reason;
    private final ConsumerResourceGuard guard;

    public ConsumerResourceGuardException(final String message,
                                          final ConsumerResourceGuardFailureReason reason,
                                          final ConsumerResourceGuard guard) {
        super(message);
        this.reason = Objects.requireNonNull(reason, "reason");
        this.guard = Objects.requireNonNull(guard, "guard");
    }

    public ConsumerResourceGuardException(final String message,
                                          final Throwable cause,
                                          final ConsumerResourceGuardFailureReason reason,
                                          final ConsumerResourceGuard guard) {
        super(message, cause);
        this.reason = Objects.requireNonNull(reason, "reason");
        this.guard = Objects.requireNonNull(guard, "guard");
    }

    public ConsumerResourceGuardFailureReason reason() {
        return reason;
    }

    public ConsumerResourceGuard guard() {
        return guard;
    }
}
