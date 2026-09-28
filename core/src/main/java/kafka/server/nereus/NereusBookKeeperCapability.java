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

package kafka.server.nereus;

import kafka.server.KafkaConfig;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.TopicBindingAggregateRecord;
import org.apache.kafka.server.config.NereusKafkaConfigs;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperDigestTypeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperProtocolModeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperTimeoutClassV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.bookkeeper.BookKeeperV3Crc32cAddPayloadLimitV1;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Properties;

/** Reads the deployment capability input and checks it against the actual loaded client and native policy. */
final class NereusBookKeeperCapability {
    private NereusBookKeeperCapability() {}

    static BookKeeperCapabilitySnapshotV1 loadAndValidate(KafkaConfig config) throws Exception {
        var cap = capability(Path.of(config.getString(NereusKafkaConfigs.BOOKKEEPER_CAPABILITY_FILE_CONFIG)));
        var bk = config.nereusKafkaStorageConfig().bookKeeper().orElseThrow();
        if (!cap.providerScopeId().digest().toHex().equals(bk.providerScopeSha256())
                || cap.ensembleSize() != bk.ensembleSize()
                || cap.writeQuorumSize() != bk.writeQuorumSize()
                || cap.ackQuorumSize() != bk.ackQuorumSize())
            throw new IllegalArgumentException("native BK scope or quorum differs from the admitted capability");
        if (!cap.digestType().name().equals(bk.digestType())
                || !cap.credentialIdentityVersion().equals(bk.passwordVersion())
                || Files.size(bk.passwordFile()) != 0)
            throw new IllegalArgumentException("native BK credential differs from the admitted no-auth capability");
        if (cap.maximumAddPayloadBytes()
                != BookKeeperV3Crc32cAddPayloadLimitV1.maximumAddPayloadBytes(
                        cap.clientFrameLimitBytes(), cap.serverFrameLimitBytes()))
            throw new IllegalArgumentException("native BK add payload differs from the v3 CRC32C frame allowance");
        var jar = Path.of(org.apache.bookkeeper.client.BookKeeper.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .toURI());
        if (!Sha256Digest.hash(CanonicalBytes.copyOf(Files.readAllBytes(jar))).equals(cap.clientArtifactSha256()))
            throw new IllegalArgumentException("loaded native BK client artifact differs from capability");
        metadataPolicy(config).validateFeatureAdmission();
        return cap;
    }

    static void validateAggregate(KafkaConfig config, TopicBindingAggregateRecord aggregate) throws Exception {
        var policy = metadataPolicy(config);
        if (!Uuid.fromString(config.nereusKafkaStorageConfig()
                                .bookKeeper()
                                .orElseThrow()
                                .deploymentId())
                        .equals(aggregate.deploymentId())
                || !Uuid.fromString(config.nereusKafkaStorageConfig()
                                .core()
                                .cluster()
                                .orElseThrow())
                        .equals(aggregate.kafkaCellId())
                || !Arrays.equals(
                        policy.policyCatalogDigest().digest().bytes().toByteArray(), aggregate.policyCatalogDigest()))
            throw new IllegalArgumentException("native BK activation aggregate differs from local deployment policy");
    }

    static org.apache.kafka.metadata.nereus.NereusKafkaMetadataPolicyV1 metadataPolicy(kafka.server.KafkaConfig config)
            throws Exception {
        var props = descriptor(Path.of(config.getString(NereusKafkaConfigs.BOOKKEEPER_CAPABILITY_FILE_CONFIG)));
        return new org.apache.kafka.metadata.nereus.NereusKafkaMetadataPolicyV1(
                Uuid.fromString(config.nereusKafkaStorageConfig()
                        .bookKeeper()
                        .orElseThrow()
                        .deploymentId()),
                Uuid.fromString(
                        config.nereusKafkaStorageConfig().core().cluster().orElseThrow()),
                sha(props, "policyCatalogDigest").bytes().toByteArray(),
                com.nereusstream.domain.aggregate.StorageProfileV1.BOOKKEEPER_WAL_ONLY,
                1,
                false);
    }

    private static Properties descriptor(Path file) throws Exception {
        if (Files.size(file) > 65_536) throw new IllegalArgumentException("BK capability descriptor exceeds its bound");
        var props = new Properties();
        try (var input = Files.newInputStream(file)) {
            props.load(input);
        }
        return props;
    }

    private static BookKeeperCapabilitySnapshotV1 capability(Path file) throws Exception {
        var props = descriptor(file);
        return new BookKeeperCapabilitySnapshotV1(
                new CellProviderScopeId(sha(props, "providerScopeSha256")),
                required(props, "clientSourceCommit"),
                sha(props, "clientArtifactSha256"),
                required(props, "serverSourceCommit"),
                sha(props, "serverImageManifestSha256"),
                BookKeeperProtocolModeV1.valueOf(required(props, "protocolMode")),
                number(props, "clientFrameLimitBytes"),
                number(props, "serverFrameLimitBytes"),
                number(props, "maximumAddPayloadBytes"),
                Boolean.parseBoolean(required(props, "explicitEntryIdsSupported")),
                number(props, "ensembleSize"),
                number(props, "writeQuorumSize"),
                number(props, "ackQuorumSize"),
                BookKeeperDigestTypeV1.valueOf(required(props, "digestType")),
                Boolean.parseBoolean(required(props, "fencingSupported")),
                Boolean.parseBoolean(required(props, "recoverySupported")),
                new BookKeeperTimeoutClassV1(
                        number(props, "connectMillis"),
                        number(props, "addMillis"),
                        number(props, "readMillis"),
                        number(props, "recoveryMillis")),
                required(props, "credentialIdentityVersion"),
                sha(props, "configurationDigest"));
    }

    private static String required(Properties props, String key) {
        var value = props.getProperty(key);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("BK capability lacks " + key);
        return value;
    }

    private static int number(Properties props, String key) {
        return Integer.parseInt(required(props, key));
    }

    private static Sha256Digest sha(Properties props, String key) {
        return Sha256Digest.copyOf(HexFormat.of().parseHex(required(props, key)));
    }
}
