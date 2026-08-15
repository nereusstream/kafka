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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Objects;

/** Authenticated v13+ Fetch response evidence for one guarded partition. */
public final class GuardedFetchEvidence {
    private final String authenticatedClusterId;
    private final String canonicalTopic;
    private final Uuid expectedTopicId;
    private final int partition;
    private final short requestVersion;
    private final int correlationId;
    private final int brokerNodeId;
    private final int sessionId;
    private final long fetchOffset;
    private final long firstRecordOffset;
    private final long lastRecordOffset;
    private final long highWatermark;
    private final long lastStableOffset;
    private final byte[] fetchResponseBodySha256;

    @SuppressWarnings("ParameterNumber")
    public GuardedFetchEvidence(final String authenticatedClusterId, final String canonicalTopic,
                                final Uuid expectedTopicId, final int partition, final short requestVersion,
                                final int correlationId, final int brokerNodeId, final int sessionId,
                                final long fetchOffset, final long firstRecordOffset, final long lastRecordOffset,
                                final long highWatermark, final long lastStableOffset,
                                final byte[] fetchResponseBodySha256) {
        this.authenticatedClusterId = requireText(authenticatedClusterId, "authenticatedClusterId");
        this.canonicalTopic = requireText(canonicalTopic, "canonicalTopic");
        this.expectedTopicId = Objects.requireNonNull(expectedTopicId, "expectedTopicId");
        if (invalidIdentity(expectedTopicId, partition, requestVersion, correlationId, brokerNodeId, sessionId)
                || fetchOffset < 0 || invalidRecordRange(firstRecordOffset, lastRecordOffset)
                || highWatermark < -1 || lastStableOffset < -1) {
            throw new IllegalArgumentException("invalid guarded Fetch evidence");
        }
        this.partition = partition;
        this.requestVersion = requestVersion;
        this.correlationId = correlationId;
        this.brokerNodeId = brokerNodeId;
        this.sessionId = sessionId;
        this.fetchOffset = fetchOffset;
        this.firstRecordOffset = firstRecordOffset;
        this.lastRecordOffset = lastRecordOffset;
        this.highWatermark = highWatermark;
        this.lastStableOffset = lastStableOffset;
        this.fetchResponseBodySha256 = requireSha256(fetchResponseBodySha256, "fetchResponseBodySha256");
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

    public TopicPartition topicPartition() {
        return new TopicPartition(canonicalTopic, partition);
    }

    public int partition() {
        return partition;
    }

    public short requestVersion() {
        return requestVersion;
    }

    public int correlationId() {
        return correlationId;
    }

    public int brokerNodeId() {
        return brokerNodeId;
    }

    public int sessionId() {
        return sessionId;
    }

    public long fetchOffset() {
        return fetchOffset;
    }

    public long firstRecordOffset() {
        return firstRecordOffset;
    }

    public long lastRecordOffset() {
        return lastRecordOffset;
    }

    public long highWatermark() {
        return highWatermark;
    }

    public long lastStableOffset() {
        return lastStableOffset;
    }

    public byte[] fetchResponseBodySha256() {
        return fetchResponseBodySha256.clone();
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof GuardedFetchEvidence)) {
            return false;
        }
        GuardedFetchEvidence that = (GuardedFetchEvidence) other;
        return partition == that.partition && requestVersion == that.requestVersion
                && correlationId == that.correlationId && brokerNodeId == that.brokerNodeId
                && sessionId == that.sessionId && fetchOffset == that.fetchOffset
                && firstRecordOffset == that.firstRecordOffset && lastRecordOffset == that.lastRecordOffset
                && highWatermark == that.highWatermark && lastStableOffset == that.lastStableOffset
                && authenticatedClusterId.equals(that.authenticatedClusterId)
                && canonicalTopic.equals(that.canonicalTopic) && expectedTopicId.equals(that.expectedTopicId)
                && Arrays.equals(fetchResponseBodySha256, that.fetchResponseBodySha256);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(authenticatedClusterId, canonicalTopic, expectedTopicId, partition,
                requestVersion, correlationId, brokerNodeId, sessionId, fetchOffset, firstRecordOffset,
                lastRecordOffset, highWatermark, lastStableOffset);
        return 31 * result + Arrays.hashCode(fetchResponseBodySha256);
    }

    @Override
    public String toString() {
        return "GuardedFetchEvidence{authenticatedClusterId='" + authenticatedClusterId
                + "', canonicalTopic='" + canonicalTopic + "', expectedTopicId=" + expectedTopicId
                + ", partition=" + partition + ", requestVersion=" + requestVersion
                + ", correlationId=" + correlationId + ", brokerNodeId=" + brokerNodeId
                + ", sessionId=" + sessionId + ", fetchOffset=" + fetchOffset
                + ", firstRecordOffset=" + firstRecordOffset + ", lastRecordOffset=" + lastRecordOffset + "}";
    }

    private static byte[] requireSha256(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length != 32) {
            throw new IllegalArgumentException(name + " must be a SHA-256 digest");
        }
        return value.clone();
    }

    private static String requireText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value;
    }

    private static boolean invalidIdentity(final Uuid topicId, final int partition, final short requestVersion,
                                           final int correlationId, final int brokerNodeId, final int sessionId) {
        return Uuid.ZERO_UUID.equals(topicId) || partition < 0 || requestVersion < 13
                || correlationId < 0 || brokerNodeId < 0 || sessionId < -1;
    }

    private static boolean invalidRecordRange(final long firstRecordOffset, final long lastRecordOffset) {
        return firstRecordOffset < -1 || lastRecordOffset < -1
                || (firstRecordOffset == -1L) != (lastRecordOffset == -1L)
                || (firstRecordOffset >= 0 && lastRecordOffset < firstRecordOffset);
    }
}
