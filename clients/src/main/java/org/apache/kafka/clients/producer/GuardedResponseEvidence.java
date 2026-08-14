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

import java.util.Arrays;
import java.util.Objects;
import java.util.OptionalInt;

/** Authenticated per-record evidence produced by a guarded v13+ request. */
public final class GuardedResponseEvidence {
    private final String authenticatedClusterId;
    private final String canonicalTopic;
    private final Uuid expectedTopicId;
    private final int partition;
    private final short requestVersion;
    private final int correlationId;
    private final int brokerNodeId;
    private final short errorCode;
    private final long baseOffset;
    private final long logAppendTimeMs;
    private final OptionalInt responseLeaderEpoch;
    private final byte[] produceRequestBodySha256;
    private final byte[] produceResponseBodySha256;
    private final byte[] selectedBatchRecordsSha256;
    private final byte[] selectedRecordValueSha256;
    private final int selectedBatchRecordIndex;
    private final int selectedBatchRecordCount;

    @SuppressWarnings("ParameterNumber")
    GuardedResponseEvidence(final String authenticatedClusterId, final String canonicalTopic,
                            final Uuid expectedTopicId, final int partition, final short requestVersion,
                            final int correlationId, final int brokerNodeId, final short errorCode,
                            final long baseOffset, final long logAppendTimeMs,
                            final OptionalInt responseLeaderEpoch, final byte[] produceRequestBodySha256,
                            final byte[] produceResponseBodySha256, final byte[] selectedBatchRecordsSha256,
                            final byte[] selectedRecordValueSha256, final int selectedBatchRecordIndex,
                            final int selectedBatchRecordCount) {
        this.authenticatedClusterId = requireText(authenticatedClusterId, "authenticatedClusterId");
        this.canonicalTopic = requireText(canonicalTopic, "canonicalTopic");
        this.expectedTopicId = Objects.requireNonNull(expectedTopicId, "expectedTopicId");
        if (Uuid.ZERO_UUID.equals(expectedTopicId) || partition < 0 || requestVersion < 13
                || baseOffset < (errorCode == 0 ? 0 : -1)
                || logAppendTimeMs < (errorCode == 0 ? 0 : -1)) {
            throw new IllegalArgumentException("invalid guarded response identity/evidence");
        }
        this.partition = partition;
        this.requestVersion = requestVersion;
        this.correlationId = correlationId;
        this.brokerNodeId = brokerNodeId;
        this.errorCode = errorCode;
        this.baseOffset = baseOffset;
        this.logAppendTimeMs = logAppendTimeMs;
        this.responseLeaderEpoch = Objects.requireNonNull(responseLeaderEpoch, "responseLeaderEpoch");
        if (responseLeaderEpoch.isPresent() && responseLeaderEpoch.getAsInt() < 0) {
            throw new IllegalArgumentException("responseLeaderEpoch must be non-negative");
        }
        this.produceRequestBodySha256 = requireSha256(produceRequestBodySha256, "produceRequestBodySha256");
        this.produceResponseBodySha256 = requireSha256(produceResponseBodySha256, "produceResponseBodySha256");
        this.selectedBatchRecordsSha256 = requireSha256(selectedBatchRecordsSha256, "selectedBatchRecordsSha256");
        this.selectedRecordValueSha256 = requireSha256(selectedRecordValueSha256, "selectedRecordValueSha256");
        if (selectedBatchRecordIndex < 0 || selectedBatchRecordCount <= 0
                || selectedBatchRecordIndex >= selectedBatchRecordCount) {
            throw new IllegalArgumentException("invalid selected batch index/count");
        }
        this.selectedBatchRecordIndex = selectedBatchRecordIndex;
        this.selectedBatchRecordCount = selectedBatchRecordCount;
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

    public short requestVersion() {
        return requestVersion;
    }

    public int correlationId() {
        return correlationId;
    }

    public int brokerNodeId() {
        return brokerNodeId;
    }

    public short errorCode() {
        return errorCode;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long logAppendTimeMs() {
        return logAppendTimeMs;
    }

    public OptionalInt responseLeaderEpoch() {
        return responseLeaderEpoch;
    }

    public byte[] produceRequestBodySha256() {
        return produceRequestBodySha256.clone();
    }

    public byte[] produceResponseBodySha256() {
        return produceResponseBodySha256.clone();
    }

    public byte[] selectedBatchRecordsSha256() {
        return selectedBatchRecordsSha256.clone();
    }

    public byte[] selectedRecordValueSha256() {
        return selectedRecordValueSha256.clone();
    }

    public int selectedBatchRecordIndex() {
        return selectedBatchRecordIndex;
    }

    public int selectedBatchRecordCount() {
        return selectedBatchRecordCount;
    }

    @Override
    public String toString() {
        return "GuardedResponseEvidence{authenticatedClusterId='" + authenticatedClusterId
                + "', canonicalTopic='" + canonicalTopic + "', expectedTopicId=" + expectedTopicId
                + ", partition=" + partition + ", requestVersion=" + requestVersion
                + ", correlationId=" + correlationId + ", brokerNodeId=" + brokerNodeId
                + ", errorCode=" + errorCode + ", baseOffset=" + baseOffset
                + ", logAppendTimeMs=" + logAppendTimeMs + ", selectedBatchRecordIndex="
                + selectedBatchRecordIndex + ", selectedBatchRecordCount=" + selectedBatchRecordCount + "}";
    }

    @Override
    @SuppressWarnings("CyclomaticComplexity")
    public boolean equals(final Object other) {
        if (!(other instanceof GuardedResponseEvidence)) {
            return false;
        }
        GuardedResponseEvidence that = (GuardedResponseEvidence) other;
        return partition == that.partition && requestVersion == that.requestVersion
                && correlationId == that.correlationId && brokerNodeId == that.brokerNodeId
                && errorCode == that.errorCode && baseOffset == that.baseOffset
                && logAppendTimeMs == that.logAppendTimeMs
                && selectedBatchRecordIndex == that.selectedBatchRecordIndex
                && selectedBatchRecordCount == that.selectedBatchRecordCount
                && authenticatedClusterId.equals(that.authenticatedClusterId)
                && canonicalTopic.equals(that.canonicalTopic) && expectedTopicId.equals(that.expectedTopicId)
                && responseLeaderEpoch.equals(that.responseLeaderEpoch)
                && Arrays.equals(produceRequestBodySha256, that.produceRequestBodySha256)
                && Arrays.equals(produceResponseBodySha256, that.produceResponseBodySha256)
                && Arrays.equals(selectedBatchRecordsSha256, that.selectedBatchRecordsSha256)
                && Arrays.equals(selectedRecordValueSha256, that.selectedRecordValueSha256);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(authenticatedClusterId, canonicalTopic, expectedTopicId, partition,
                requestVersion, correlationId, brokerNodeId, errorCode, baseOffset, logAppendTimeMs,
                responseLeaderEpoch, selectedBatchRecordIndex, selectedBatchRecordCount);
        result = 31 * result + Arrays.hashCode(produceRequestBodySha256);
        result = 31 * result + Arrays.hashCode(produceResponseBodySha256);
        result = 31 * result + Arrays.hashCode(selectedBatchRecordsSha256);
        return 31 * result + Arrays.hashCode(selectedRecordValueSha256);
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
}
