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
package org.apache.kafka.server.common;

import java.util.Map;

/**
 * Durable KRaft feature that activates Nereus authoritative Kafka storage semantics.
 *
 * <p>The feature is deliberately not part of Kafka's automatically bootstrapped production
 * feature set. Nodes advertise support only when their local Nereus storage mode is enabled,
 * and operators must explicitly establish level 2 during fresh storage formatting.
 */
public enum NereusStorageVersion implements FeatureVersion {

    // Version 2 is the clean-break V2 aggregate metadata format. Level 1 permanently denotes V1.
    NSV_2(2, MetadataVersion.MINIMUM_VERSION, Map.of());

    public static final String FEATURE_NAME = "nereus.storage.version";

    public static final short DISABLED_LEVEL = 0;
    public static final short LEGACY_V1_LEVEL = 1;

    public static final NereusStorageVersion LATEST_PRODUCTION = NSV_2;

    private final short featureLevel;
    private final MetadataVersion bootstrapMetadataVersion;
    private final Map<String, Short> dependencies;

    NereusStorageVersion(
        int featureLevel,
        MetadataVersion bootstrapMetadataVersion,
        Map<String, Short> dependencies
    ) {
        this.featureLevel = (short) featureLevel;
        this.bootstrapMetadataVersion = bootstrapMetadataVersion;
        this.dependencies = dependencies;
    }

    @Override
    public short featureLevel() {
        return featureLevel;
    }

    @Override
    public String featureName() {
        return FEATURE_NAME;
    }

    @Override
    public MetadataVersion bootstrapMetadataVersion() {
        return bootstrapMetadataVersion;
    }

    @Override
    public Map<String, Short> dependencies() {
        return dependencies;
    }

    public static NereusStorageVersion fromFeatureLevel(short version) {
        return switch (version) {
            case 2 -> NSV_2;
            default -> throw new IllegalArgumentException(
                "Unknown Nereus storage feature level: " + (int) version);
        };
    }
}
