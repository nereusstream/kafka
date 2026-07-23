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

package kafka.log.nereus;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.protocol.Errors;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/** Converts protocol-neutral Nereus failures into Kafka-owned exceptions without weakening fencing. */
public final class NereusKafkaExceptionMapper {
    private NereusKafkaExceptionMapper() {
    }

    public static ApiException map(Throwable failure) {
        Throwable exact = unwrap(Objects.requireNonNull(failure, "failure"));
        if (exact instanceof ApiException kafka) {
            return kafka;
        }

        Errors error;
        if (exact instanceof CancellationException) {
            error = Errors.REQUEST_TIMED_OUT;
        } else if (exact instanceof IllegalArgumentException) {
            error = Errors.INVALID_REQUEST;
        } else if (exact instanceof NereusException nereus) {
            error = map(nereus.code());
        } else {
            error = Errors.KAFKA_STORAGE_ERROR;
        }
        ApiException mapped = error.exception(exact.getMessage());
        if (mapped.getCause() == null) {
            try {
                mapped.initCause(exact);
            } catch (IllegalStateException ignored) {
                // Some Kafka exception constructors close cause initialization themselves.
            }
        }
        return mapped;
    }

    private static Errors map(ErrorCode code) {
        return switch (code) {
            case INVALID_ARGUMENT -> Errors.INVALID_REQUEST;
            case CANCELLED, TIMEOUT -> Errors.REQUEST_TIMED_OUT;
            case STREAM_NOT_FOUND, STREAM_NOT_ACTIVE, STORAGE_CLOSED -> Errors.NOT_LEADER_OR_FOLLOWER;
            case APPEND_SESSION_EXPIRED, FENCED_APPEND -> Errors.FENCED_LEADER_EPOCH;
            case BACKPRESSURE_REJECTED -> Errors.THROTTLING_QUOTA_EXCEEDED;
            case OBJECT_CHECKSUM_MISMATCH, PRIMARY_WAL_CHECKSUM_MISMATCH -> Errors.CORRUPT_MESSAGE;
            case OFFSET_TRIMMED -> Errors.OFFSET_OUT_OF_RANGE;
            case OFFSET_NOT_AVAILABLE -> Errors.OFFSET_NOT_AVAILABLE;
            case UNSUPPORTED_READ_TARGET,
                    UNSUPPORTED_STORAGE_PROFILE,
                    UNSUPPORTED_DURABILITY_LEVEL,
                    UNSUPPORTED_FORMAT,
                    UNSUPPORTED_APPEND_PRECONDITION,
                    UNSUPPORTED_READ_SEMANTICS,
                    UNSUPPORTED_APPEND_AUTHORITY -> Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT;
            case OFFSET_CONFLICT,
                    OBJECT_UPLOAD_FAILED,
                    OBJECT_READ_FAILED,
                    OBJECT_NOT_FOUND,
                    PRIMARY_WAL_WRITE_FAILED,
                    PRIMARY_WAL_READ_FAILED,
                    PRIMARY_WAL_TARGET_NOT_FOUND,
                    READ_LIMIT_TOO_SMALL,
                    METADATA_UNAVAILABLE,
                    METADATA_CONDITION_FAILED,
                    METADATA_INVARIANT_VIOLATION,
                    READ_RESOLUTION_FAILED,
                    METADATA_LIMIT_EXCEEDED -> Errors.KAFKA_STORAGE_ERROR;
        };
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
