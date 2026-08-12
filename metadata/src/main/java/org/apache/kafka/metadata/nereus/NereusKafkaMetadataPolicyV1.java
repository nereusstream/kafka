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
package org.apache.kafka.metadata.nereus;

import org.apache.kafka.common.Uuid;

import com.nereusstream.domain.aggregate.PolicyCatalogDigest;
import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaCellId;
import com.nereusstream.domain.protocol.KafkaProtocolCellIdentity;

import java.util.Objects;

/** Immutable deployment policy input for the K1 metadata authority. */
public final class NereusKafkaMetadataPolicyV1 {
    public static final int INTERNAL_TOPIC_POLICY_VERSION = 1;

    private final KafkaProtocolCellIdentity cellIdentity;
    private final PolicyCatalogDigest policyCatalogDigest;
    private final StorageProfileV1 userTopicDefaultProfile;
    private final int internalTopicPolicyVersion;
    private final boolean remoteLogStorageSystemEnabled;

    public NereusKafkaMetadataPolicyV1(
        Uuid deploymentId,
        Uuid kafkaCellId,
        byte[] policyCatalogDigest,
        StorageProfileV1 userTopicDefaultProfile,
        int internalTopicPolicyVersion,
        boolean remoteLogStorageSystemEnabled
    ) {
        this.cellIdentity = new KafkaProtocolCellIdentity(
            new DeploymentId(requireNonZero(deploymentId, "deployment ID")),
            new KafkaCellId(requireNonZero(kafkaCellId, "Kafka Cell ID")));
        Sha256Digest digest = Sha256Digest.copyOf(
            Objects.requireNonNull(policyCatalogDigest, "policy catalog digest"));
        if (digest.isZero()) {
            throw new IllegalArgumentException("policy catalog digest must be non-zero");
        }
        this.policyCatalogDigest = new PolicyCatalogDigest(digest);
        this.userTopicDefaultProfile = Objects.requireNonNull(
            userTopicDefaultProfile, "user topic default profile");
        if (internalTopicPolicyVersion != INTERNAL_TOPIC_POLICY_VERSION) {
            throw new IllegalArgumentException(
                "unsupported internal topic policy version: " + internalTopicPolicyVersion);
        }
        this.internalTopicPolicyVersion = internalTopicPolicyVersion;
        this.remoteLogStorageSystemEnabled = remoteLogStorageSystemEnabled;
    }

    public KafkaProtocolCellIdentity cellIdentity() {
        return cellIdentity;
    }

    public PolicyCatalogDigest policyCatalogDigest() {
        return policyCatalogDigest;
    }

    public StorageProfileV1 userTopicDefaultProfile() {
        return userTopicDefaultProfile;
    }

    public int internalTopicPolicyVersion() {
        return internalTopicPolicyVersion;
    }

    public boolean remoteLogStorageSystemEnabled() {
        return remoteLogStorageSystemEnabled;
    }

    public void validateFeatureAdmission() {
        if (remoteLogStorageSystemEnabled) {
            throw new IllegalStateException(
                "nereus.storage.version=2 requires remote.log.storage.system.enable=false");
        }
    }

    private static Id128 requireNonZero(Uuid value, String field) {
        Objects.requireNonNull(value, field);
        Id128 result = new Id128(value.getMostSignificantBits(), value.getLeastSignificantBits());
        if (result.isZero()) {
            throw new IllegalArgumentException(field + " must be non-zero");
        }
        return result;
    }
}
