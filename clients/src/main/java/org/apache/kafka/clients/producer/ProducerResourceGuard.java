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

import org.apache.kafka.common.Uuid;

import java.util.Objects;

/** Immutable resource identity required by a guarded single-record produce. */
public final class ProducerResourceGuard {
    private final String authenticatedClusterId;
    private final String canonicalTopic;
    private final Uuid expectedTopicId;
    private final int partition;

    public ProducerResourceGuard(final String authenticatedClusterId, final String canonicalTopic,
                                 final Uuid expectedTopicId, final int partition) {
        this.authenticatedClusterId = requireText(authenticatedClusterId, "authenticatedClusterId");
        this.canonicalTopic = requireText(canonicalTopic, "canonicalTopic");
        this.expectedTopicId = Objects.requireNonNull(expectedTopicId, "expectedTopicId");
        if (Uuid.ZERO_UUID.equals(expectedTopicId) || partition < 0) {
            throw new IllegalArgumentException("invalid Producer resource guard");
        }
        this.partition = partition;
    }

    public String authenticatedClusterId() {
        return authenticatedClusterId;
    }

    public String canonicalTopic() {
        return canonicalTopic;
    }

    public Uuid expectedTopicId() {
        return expectedTopicId;
    }

    public int partition() {
        return partition;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof ProducerResourceGuard)) {
            return false;
        }
        ProducerResourceGuard that = (ProducerResourceGuard) other;
        return partition == that.partition
                && authenticatedClusterId.equals(that.authenticatedClusterId)
                && canonicalTopic.equals(that.canonicalTopic)
                && expectedTopicId.equals(that.expectedTopicId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(authenticatedClusterId, canonicalTopic, expectedTopicId, partition);
    }

    @Override
    public String toString() {
        return "ProducerResourceGuard{authenticatedClusterId='" + authenticatedClusterId
                + "', canonicalTopic='" + canonicalTopic + "', expectedTopicId=" + expectedTopicId
                + ", partition=" + partition + "}";
    }

    private static String requireText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value;
    }
}
