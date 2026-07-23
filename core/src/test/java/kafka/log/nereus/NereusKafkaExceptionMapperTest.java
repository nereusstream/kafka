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
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.FencedLeaderEpochException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.errors.TimeoutException;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

class NereusKafkaExceptionMapperTest {
    @Test
    void preservesKafkaExceptions() {
        ApiException failure = new InvalidRequestException("already mapped");

        assertSame(failure, NereusKafkaExceptionMapper.map(failure));
    }

    @Test
    void mapsFencingAndOffsetFailuresWithoutWeakeningThem() {
        assertInstanceOf(FencedLeaderEpochException.class, map(ErrorCode.FENCED_APPEND));
        assertInstanceOf(OffsetOutOfRangeException.class, map(ErrorCode.OFFSET_TRIMMED));
    }

    @Test
    void mapsWrappedTimeoutAndCapacityFailures() {
        ApiException timeout = NereusKafkaExceptionMapper.map(
                new CompletionException(nereus(ErrorCode.TIMEOUT)));

        assertInstanceOf(TimeoutException.class, timeout);
        assertInstanceOf(ThrottlingQuotaExceededException.class, map(ErrorCode.BACKPRESSURE_REJECTED));
    }

    @Test
    void mapsIntegrityAndInternalStorageFailures() {
        assertInstanceOf(CorruptRecordException.class, map(ErrorCode.OBJECT_CHECKSUM_MISMATCH));
        assertInstanceOf(KafkaStorageException.class, map(ErrorCode.METADATA_LIMIT_EXCEEDED));
        assertInstanceOf(KafkaStorageException.class, NereusKafkaExceptionMapper.map(new IllegalStateException("boom")));
    }

    private static ApiException map(ErrorCode code) {
        return NereusKafkaExceptionMapper.map(nereus(code));
    }

    private static NereusException nereus(ErrorCode code) {
        return new NereusException(code, true, code.name());
    }
}
