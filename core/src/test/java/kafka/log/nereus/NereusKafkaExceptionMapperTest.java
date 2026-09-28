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

import org.apache.kafka.common.errors.FencedLeaderEpochException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.TimeoutException;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

class NereusKafkaExceptionMapperTest {
    @Test
    void preservesNativeErrorsAndUnwrapsActualStorageFailures() {
        var fenced = new FencedLeaderEpochException("stale");
        assertSame(fenced, NereusKafkaExceptionMapper.map(new CompletionException(fenced)));
        assertInstanceOf(
                TimeoutException.class, NereusKafkaExceptionMapper.map(new java.util.concurrent.TimeoutException()));
        assertInstanceOf(
                InvalidRequestException.class, NereusKafkaExceptionMapper.map(new IllegalArgumentException("invalid")));
        assertInstanceOf(
                KafkaStorageException.class,
                NereusKafkaExceptionMapper.map(new IllegalStateException("physical failure")));
    }
}
