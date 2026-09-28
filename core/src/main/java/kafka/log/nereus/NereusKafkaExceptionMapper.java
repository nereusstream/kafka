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

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/** Converts protocol-neutral Nereus failures into Kafka-owned exceptions without weakening fencing. */
public final class NereusKafkaExceptionMapper {
    private NereusKafkaExceptionMapper() {}

    public static ApiException map(Throwable failure) {
        Throwable exact = unwrap(Objects.requireNonNull(failure, "failure"));
        if (exact instanceof ApiException kafka) {
            return kafka;
        }

        Errors error;
        if (exact instanceof CancellationException || exact instanceof java.util.concurrent.TimeoutException) {
            error = Errors.REQUEST_TIMED_OUT;
        } else if (exact instanceof IllegalArgumentException) {
            error = Errors.INVALID_REQUEST;
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

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
