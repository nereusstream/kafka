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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.ProducerResourceGuard;
import org.apache.kafka.clients.producer.ResourceGuardFailureReason;
import java.util.Arrays;
import java.util.Objects;
import java.util.OptionalInt;

/**
 * Internal bridge between a guarded record future and the producer I/O thread.
 *
 * The bridge deliberately carries opaque objects at its boundary. This keeps the
 * evidence construction authority in the public producer package while allowing
 * the accumulator and sender to remain internal implementation details.
 */
public interface GuardedCompletion {
    ProducerResourceGuard guard();

    void complete(org.apache.kafka.clients.producer.RecordMetadata metadata, Object evidence);

    void fail(Object failure);

    /** Request-scoped fields retained until an authenticated response is available. */
    final class RequestContext {
        public final ProducerResourceGuard guard;
        public final short requestVersion;
        public final int correlationId;
        public final int brokerNodeId;
        public final byte[] produceRequestBodySha256;
        public final byte[] selectedBatchRecordsSha256;

        public RequestContext(final ProducerResourceGuard guard, final short requestVersion,
                              final int correlationId, final int brokerNodeId,
                              final byte[] produceRequestBodySha256,
                              final byte[] selectedBatchRecordsSha256) {
            this.guard = Objects.requireNonNull(guard, "guard");
            this.requestVersion = requestVersion;
            this.correlationId = correlationId;
            this.brokerNodeId = brokerNodeId;
            this.produceRequestBodySha256 = copySha256(produceRequestBodySha256, "produceRequestBodySha256");
            this.selectedBatchRecordsSha256 = copySha256(selectedBatchRecordsSha256,
                    "selectedBatchRecordsSha256");
        }
    }

    /** Response fields used to construct one record's public evidence. */
    final class Evidence {
        public final RequestContext requestContext;
        public final short errorCode;
        public final long baseOffset;
        public final long logAppendTimeMs;
        public final OptionalInt responseLeaderEpoch;
        public final byte[] produceResponseBodySha256;
        public final int selectedBatchRecordIndex;
        public final int selectedBatchRecordCount;
        public final byte[] selectedRecordValueSha256;

        public Evidence(final RequestContext requestContext, final short errorCode,
                        final long baseOffset, final long logAppendTimeMs,
                        final OptionalInt responseLeaderEpoch,
                        final byte[] produceResponseBodySha256) {
            this(requestContext, errorCode, baseOffset, logAppendTimeMs, responseLeaderEpoch,
                    produceResponseBodySha256, -1, -1, null);
        }

        private Evidence(final RequestContext requestContext, final short errorCode,
                         final long baseOffset, final long logAppendTimeMs,
                         final OptionalInt responseLeaderEpoch,
                         final byte[] produceResponseBodySha256,
                         final int selectedBatchRecordIndex,
                         final int selectedBatchRecordCount,
                         final byte[] selectedRecordValueSha256) {
            this.requestContext = Objects.requireNonNull(requestContext, "requestContext");
            this.errorCode = errorCode;
            this.baseOffset = baseOffset;
            this.logAppendTimeMs = logAppendTimeMs;
            this.responseLeaderEpoch = Objects.requireNonNull(responseLeaderEpoch, "responseLeaderEpoch");
            this.produceResponseBodySha256 = copySha256(produceResponseBodySha256,
                    "produceResponseBodySha256");
            this.selectedBatchRecordIndex = selectedBatchRecordIndex;
            this.selectedBatchRecordCount = selectedBatchRecordCount;
            this.selectedRecordValueSha256 = selectedRecordValueSha256 == null
                    ? null : copySha256(selectedRecordValueSha256, "selectedRecordValueSha256");
        }

        public Evidence forRecord(final int batchIndex, final int batchCount, final byte[] valueSha256) {
            if (batchIndex < 0 || batchCount <= 0 || batchIndex >= batchCount) {
                throw new IllegalArgumentException("invalid guarded batch index/count");
            }
            return new Evidence(requestContext, errorCode, baseOffset, logAppendTimeMs,
                    responseLeaderEpoch, produceResponseBodySha256, batchIndex, batchCount, valueSha256);
        }
    }

    /** Typed fail-closed completion produced by the sender. */
    final class Failure {
        public final ResourceGuardFailureReason reason;
        public final String message;
        public final Throwable cause;
        public final Evidence evidence;
        public final boolean definitelyNotPersisted;

        public Failure(final ResourceGuardFailureReason reason, final String message,
                       final Throwable cause, final Evidence evidence,
                       final boolean definitelyNotPersisted) {
            this.reason = Objects.requireNonNull(reason, "reason");
            this.message = Objects.requireNonNull(message, "message");
            this.cause = cause;
            this.evidence = evidence;
            this.definitelyNotPersisted = definitelyNotPersisted;
        }

        public Failure forRecord(final int batchIndex, final int batchCount, final byte[] valueSha256) {
            return evidence == null ? this : new Failure(reason, message, cause,
                    evidence.forRecord(batchIndex, batchCount, valueSha256), definitelyNotPersisted);
        }
    }

    static byte[] copySha256(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length != 32) {
            throw new IllegalArgumentException(name + " must be a SHA-256 digest");
        }
        return Arrays.copyOf(value, value.length);
    }
}
