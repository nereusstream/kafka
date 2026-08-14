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
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.KafkaException;

import java.util.Objects;
import java.util.Optional;

/** Client-side guarded-resource failure; it is not a new Kafka wire error. */
public final class ResourceGuardException extends KafkaException {
    private static final long serialVersionUID = 1L;

    private final ResourceGuardFailureReason reason;
    private final ProducerResourceGuard guard;
    private final Optional<GuardedResponseEvidence> responseEvidence;
    private final boolean definitelyNotPersisted;

    ResourceGuardException(final String message, final Throwable cause, final ResourceGuardFailureReason reason,
                           final ProducerResourceGuard guard,
                           final Optional<GuardedResponseEvidence> responseEvidence,
                           final boolean definitelyNotPersisted) {
        super(message, cause);
        this.reason = Objects.requireNonNull(reason, "reason");
        this.guard = Objects.requireNonNull(guard, "guard");
        this.responseEvidence = Objects.requireNonNull(responseEvidence, "responseEvidence");
        this.definitelyNotPersisted = definitelyNotPersisted;
        if (definitelyNotPersisted && responseEvidence.isPresent()
                && responseEvidence.get().errorCode() == 0) {
            throw new IllegalArgumentException("a successful evidence cannot prove non-persistence");
        }
    }

    public ResourceGuardFailureReason reason() {
        return reason;
    }

    public ProducerResourceGuard guard() {
        return guard;
    }

    public Optional<GuardedResponseEvidence> responseEvidence() {
        return responseEvidence;
    }

    public boolean definitelyNotPersisted() {
        return definitelyNotPersisted;
    }
}
