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

import org.apache.kafka.server.config.NereusKafkaConfigs;
import org.apache.kafka.server.util.Json;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperTimeoutClassV1;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import io.oxia.client.api.OxiaClientBuilder;

/** Checks an explicit current-source M4 receipt before the isolated Broker may provision read authority. */
final class NereusKafkaM4AdmissionV2 {
    private static final String PREFIX = "m4IsolatedAdmission.";
    private static final String NEREUS_SCHEMA = "NEREUS_V2_M5_KAFKA_RUN_SOURCE_RUN_V2";
    private static final String BOOKKEEPER_IMAGE =
            "sha256:d0e78aaf987ac2feb526507ffb7d4c5137d58c0530f2a8cab4a9595abc89d605";
    private static final String BOOKKEEPER_MANIFEST =
            "c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d";
    private static final String BOOKKEEPER_CLIENT_ARTIFACT =
            "8e64f2b7436bb814705f611eb0ac48d64d90de7a50d295905c459d89bc3f9d8f";
    private static final String BOOKKEEPER_CONFIGURATION =
            "eaf41c4b42b767b8ea6e86023a784425b8073f174dbade92b4249c8f3d301dbd";
    private static final String OXIA_IMAGE =
            "sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da";
    private static final Map<String, String> READ_CLASSES = Map.of(
            "nativePartition", "com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1",
            "nativeReadOwner", "com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperM4BrokerReadOwnerV2",
            "readAsyncExecutor", "com.nereusstream.storage.object.read.BindingReadAsyncExecutorV1",
            "selectorRuntime", "com.nereusstream.storage.object.read.control.BindingReadSelectorRuntimeV1",
            "readControl", "com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1",
            "proofCleanupPlanner", "com.nereusstream.storage.object.read.control.M4ProofCleanupPlannerV1",
            "oxiaReadRoute", "com.nereusstream.metadata.oxia.v2.retention.OxiaKafkaBookKeeperM4ReadRouteV2");
    private static final Map<String, String> RUNTIME_ARTIFACTS = Map.of(
            "nereus-storage-api", "com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1",
            "nereus-metadata-spi", "com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1",
            "nereus-storage-object", "com.nereusstream.storage.object.read.BindingReadAsyncExecutorV1",
            "nereus-storage-bookkeeper", "com.nereusstream.storage.bookkeeper.BookKeeperV3Crc32cAddPayloadLimitV1",
            "nereus-kafka-bookkeeper", "com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1",
            "nereus-metadata-oxia", "com.nereusstream.metadata.oxia.v2.retention.OxiaKafkaBookKeeperM4ReadRouteV2");

    private final Sha256Digest receiptSha256;
    private final Sha256Digest inputsSha256;
    private final Sha256Digest adapterSha256;
    private final Sha256Digest readContractSha256;
    private final Sha256Digest verifierSha256;
    private final Sha256Digest backendConfigurationSha256;
    private final long maximumSourceAccessLifetimeMillis;
    private final Map<String, String> verifiedSources;
    private final Map<String, String> verifiedRuntimeArtifacts;

    private NereusKafkaM4AdmissionV2(
            Sha256Digest receiptSha256, Sha256Digest inputsSha256, Sha256Digest adapterSha256,
            Sha256Digest readContractSha256, Sha256Digest verifierSha256,
            Sha256Digest backendConfigurationSha256, long maximumSourceAccessLifetimeMillis,
            Map<String, String> verifiedSources, Map<String, String> verifiedRuntimeArtifacts) {
        this.receiptSha256 = receiptSha256;
        this.inputsSha256 = inputsSha256;
        this.adapterSha256 = adapterSha256;
        this.readContractSha256 = readContractSha256;
        this.verifierSha256 = verifierSha256;
        this.backendConfigurationSha256 = backendConfigurationSha256;
        this.maximumSourceAccessLifetimeMillis = maximumSourceAccessLifetimeMillis;
        this.verifiedSources = Map.copyOf(verifiedSources);
        this.verifiedRuntimeArtifacts = Map.copyOf(verifiedRuntimeArtifacts);
    }

    static Optional<NereusKafkaM4AdmissionV2> load(
            KafkaConfig config, BookKeeperCapabilitySnapshotV1 bookKeeper) throws Exception {
        Properties descriptor = NereusBookKeeperCapability.descriptor(
                Path.of(config.getString(NereusKafkaConfigs.BOOKKEEPER_CAPABILITY_FILE_CONFIG)));
        String summaryName = descriptor.getProperty(PREFIX + "runSummaryFile");
        if (summaryName == null) {
            return Optional.empty();
        }
        Path root = Path.of(required(descriptor, "sourceRoot")).toRealPath();
        Path summaryPath = Path.of(summaryName).toRealPath();
        Path output = Objects.requireNonNull(summaryPath.getParent(), "M4 summary directory");
        if (!summaryPath.startsWith(root.resolve("build/m5-kafka-run-source"))) {
            throw new IllegalArgumentException("M4 receipt is outside the isolated source-bound run output");
        }
        byte[] summaryBytes = bounded(summaryPath, 1_048_576);
        Sha256Digest receipt = sha(summaryBytes);
        if (!receipt.toHex().equals(required(descriptor, "runSummarySha256"))) {
            throw new IllegalArgumentException("M4 receipt differs from the explicit admission digest");
        }
        Map<?, ?> summary = object(summaryBytes);
        requireText(summary, "schema", NEREUS_SCHEMA);
        requireText(summary, "sourceAndProtocolAuthority", "SYNTHETIC_FACTS_WITH_REAL_OXIA_AND_BK");
        requireText(summary, "bookKeeperImageId", BOOKKEEPER_IMAGE);
        requireText(summary, "oxiaImageId", OXIA_IMAGE);
        requireBoolean(summary, "capturedInputsUnchangedDuringRun", true);
        requireBoolean(summary, "nativeM4HeldSelectorCasReadAndFenceReal", true);
        for (String key : List.of("m5Receipt", "m5FinalAuthority", "physicalDeleteAuthority", "productionAuthority")) {
            requireBoolean(summary, key, false);
        }
        Sha256Digest inputs = verifyInputs(root, output, summary);
        verifySuites(output, summary);
        Map<?, ?> classes = nested(summary, "currentLoadedM4ReadClassSha256");
        verifyClasses(classes);
        verifyRuntimeArtifacts(nested(summary, "currentM4RuntimeArtifactSha256"));
        Map<?, ?> oxia = nested(summary, "lockedOxiaClientArtifactSha256");
        verifyArtifact(oxia, "oxia-client", Class.forName("io.oxia.client.OxiaClientBuilderImpl"));
        verifyArtifact(oxia, "oxia-client-api", OxiaClientBuilder.class);
        verifyBackend(config, bookKeeper);
        Sha256Digest adapter = digest("M4-BK-OXIA-ADAPTER-V2", text(classes, "nativePartition"),
                text(classes, "nativeReadOwner"), text(classes, "oxiaReadRoute"), text(oxia, "oxia-client"),
                text(oxia, "oxia-client-api"), bookKeeper.clientArtifactSha256().toHex());
        Sha256Digest contract = digest("M4-NATIVE-READ-CONTRACT-V2", text(classes, "readAsyncExecutor"),
                text(classes, "selectorRuntime"));
        Sha256Digest verifier = digest("M4-NATIVE-READ-VERIFIER-V2", text(classes, "readControl"),
                text(classes, "proofCleanupPlanner"));
        Sha256Digest backend = digest("M4-NATIVE-BACKEND-CONFIG-V2", bookKeeper.configurationDigest().toHex(),
                config.nereusKafkaStorageConfig().core().oxiaNamespace());
        long readMillis = Math.max(config.nereusKafkaStorageConfig().fetch().timeout().toMillis(),
                bookKeeper.timeoutClass().readMillis());
        byte[] sourceBytes = bounded(output.resolve("tested-inputs.json"), 1_048_576);
        if (!sha(sourceBytes).equals(inputs)) throw new IllegalArgumentException("verified M4 source manifest changed");
        return Optional.of(new NereusKafkaM4AdmissionV2(receipt, inputs, adapter, contract, verifier, backend,
                readMillis, stringMap(object(sourceBytes)), stringMap(nested(summary, "currentM4RuntimeArtifactSha256"))));
    }

    /** Actual implementation source from the verified manifest, bound to its loaded tested runtime JAR. */
    Sha256Digest implementationSourceSha256(Class<?> implementation) throws Exception {
        Path artifact = Path.of(implementation.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!verifiedRuntimeArtifacts.containsValue(sha(bounded(artifact, 67_108_864)).toHex()))
            throw new IllegalArgumentException("writer implementation is outside the verified Nereus runtime artifacts");
        String owner = implementation.getName().split("\\$", 2)[0];
        String suffix = "/src/main/java/" + owner.replace('.', '/') + ".java";
        var entries = verifiedSources.entrySet().stream().filter(value -> value.getKey().endsWith(suffix)).toList();
        if (entries.size() != 1) throw new IllegalArgumentException("writer implementation source is missing or ambiguous");
        return Sha256Digest.copyOf(java.util.HexFormat.of().parseHex(entries.get(0).getValue()));
    }

    private static Map<String, String> stringMap(Map<?, ?> input) {
        var result = new java.util.HashMap<String, String>();
        input.forEach((key, value) -> {
            if (!(key instanceof String name) || !(value instanceof String text))
                throw new IllegalArgumentException("verified source/artifact manifest has a non-string entry");
            result.put(name, text);
        });
        return Map.copyOf(result);
    }

    Sha256Digest receiptSha256() {
        return receiptSha256;
    }

    Sha256Digest inputsSha256() {
        return inputsSha256;
    }

    Sha256Digest adapterSha256() {
        return adapterSha256;
    }

    Sha256Digest readContractSha256() {
        return readContractSha256;
    }

    Sha256Digest verifierSha256() {
        return verifierSha256;
    }

    Sha256Digest backendConfigurationSha256() {
        return backendConfigurationSha256;
    }

    long maximumSourceAccessLifetimeMillis() {
        return maximumSourceAccessLifetimeMillis;
    }

    private static Sha256Digest verifyInputs(Path root, Path output, Map<?, ?> summary) throws Exception {
        byte[] bytes = bounded(output.resolve("tested-inputs.json"), 1_048_576);
        Sha256Digest inputsSha = sha(bytes);
        requireText(summary, "testedInputManifestSha256", inputsSha.toHex());
        Map<?, ?> manifest = object(bytes);
        if (manifest.isEmpty()) {
            throw new IllegalArgumentException("M4 source manifest is empty");
        }
        for (Map.Entry<?, ?> entry : manifest.entrySet()) {
            if (!(entry.getKey() instanceof String path) || !(entry.getValue() instanceof String expected)) {
                throw new IllegalArgumentException("M4 source manifest has an invalid entry");
            }
            Path source = root.resolve(path).normalize();
            if (!source.startsWith(root) || !sha(bounded(source, 16_777_216)).toHex().equals(expected)) {
                throw new IllegalArgumentException("M4 source changed: " + path);
            }
        }
        return inputsSha;
    }

    private static void verifySuites(Path output, Map<?, ?> summary) throws Exception {
        if (!(summary.get("suites") instanceof List<?> suites) || suites.isEmpty()) {
            throw new IllegalArgumentException("M4 receipt has no source-bound suites");
        }
        boolean realCallback = false;
        boolean ownerBudget = false;
        for (Object value : suites) {
            if (!(value instanceof Map<?, ?> suite)) {
                throw new IllegalArgumentException("M4 receipt has a malformed suite");
            }
            String task = text(suite, "task");
            requireText(suite, "xmlSha256", sha(bounded(output.resolve(task + ".xml"), 4_194_304)).toHex());
            Object count = suite.get("tests");
            if (!(count instanceof Number number) || number.intValue() <= 0) {
                throw new IllegalArgumentException("M4 receipt has no executed tests for " + task);
            }
            realCallback |= "v2M5KafkaRunRootsRealTest".equals(task) && number.intValue() == 16;
            ownerBudget |= "v2M5KafkaM4BrokerReadOwnerTest".equals(task) && number.intValue() == 5;
        }
        if (!realCallback || !ownerBudget) {
            throw new IllegalArgumentException("M4 receipt lacks real callback or bounded owner result");
        }
    }

    private static void verifyClasses(Map<?, ?> classes) throws Exception {
        for (Map.Entry<String, String> entry : READ_CLASSES.entrySet()) {
            Class<?> type = Class.forName(entry.getValue());
            try (var input = type.getResourceAsStream(type.getSimpleName() + ".class")) {
                if (input == null) {
                    throw new IllegalArgumentException("M4 class bytes are absent: " + type.getName());
                }
                requireText(classes, entry.getKey(), sha(input.readAllBytes()).toHex());
            }
        }
    }

    private static void verifyRuntimeArtifacts(Map<?, ?> expected) throws Exception {
        for (Map.Entry<String, String> entry : RUNTIME_ARTIFACTS.entrySet()) {
            Class<?> type = Class.forName(entry.getValue(), false, NereusKafkaM4AdmissionV2.class.getClassLoader());
            verifyArtifact(expected, entry.getKey(), type);
        }
    }

    private static void verifyBackend(KafkaConfig config, BookKeeperCapabilitySnapshotV1 bookKeeper) {
        verifyBackendSource(bookKeeper);
        verifyBackendProtocol(bookKeeper);
        verifyBackendQuorums(bookKeeper);
        if (!"default".equals(config.nereusKafkaStorageConfig().core().oxiaNamespace())) {
            throw new IllegalArgumentException("loaded Oxia namespace differs from the tested backend");
        }
    }

    private static void verifyBackendSource(BookKeeperCapabilitySnapshotV1 bookKeeper) {
        if (!"cd06340851d6d657b7c7546df01df365c18980de".equals(bookKeeper.clientSourceCommit())
                || !"cd06340851d6d657b7c7546df01df365c18980de".equals(bookKeeper.serverSourceCommit())
                || !BOOKKEEPER_MANIFEST.equals(bookKeeper.serverImageManifestSha256().toHex())
                || !BOOKKEEPER_CLIENT_ARTIFACT.equals(bookKeeper.clientArtifactSha256().toHex())
                || !BOOKKEEPER_CONFIGURATION.equals(bookKeeper.configurationDigest().toHex())) {
            throw new IllegalArgumentException("loaded BK source or artifacts differ from the tested backend");
        }
    }

    private static void verifyBackendProtocol(BookKeeperCapabilitySnapshotV1 bookKeeper) {
        if (!"V3".equals(bookKeeper.protocolMode().name())
                || bookKeeper.clientFrameLimitBytes() != 5_242_880
                || bookKeeper.serverFrameLimitBytes() != 5_242_880
                || bookKeeper.maximumAddPayloadBytes() != 5_242_771
                || !"CRC32C".equals(bookKeeper.digestType().name())) {
            throw new IllegalArgumentException("loaded BK protocol differs from the tested backend");
        }
    }

    private static void verifyBackendQuorums(BookKeeperCapabilitySnapshotV1 bookKeeper) {
        if (bookKeeper.ensembleSize() != 3
                || bookKeeper.writeQuorumSize() != 3
                || bookKeeper.ackQuorumSize() != 2
                || !bookKeeper.explicitEntryIdsSupported()
                || !bookKeeper.fencingSupported()) {
            throw new IllegalArgumentException("loaded BK quorum or fencing differs from the tested backend");
        }
        if (!bookKeeper.recoverySupported()
                || !new BookKeeperTimeoutClassV1(10_000, 5_000, 5_000, 30_000)
                        .equals(bookKeeper.timeoutClass())
                || !"bk-k0-no-auth:v1".equals(bookKeeper.credentialIdentityVersion())) {
            throw new IllegalArgumentException("loaded BK recovery or authentication differs from the tested backend");
        }
    }

    private static void verifyArtifact(Map<?, ?> expected, String key, Class<?> type) throws Exception {
        Path artifact = Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
        verifyArtifactDigest(expected, key, artifact);
    }

    static void verifyArtifactDigest(Map<?, ?> expected, String key, Path artifact) throws Exception {
        String actual = sha(bounded(artifact, 64_000_000)).toHex();
        if (!text(expected, key).equals(actual)) {
            throw new IllegalArgumentException("M4 receipt differs at " + key + ": loaded " + artifact
                    + " with digest " + actual);
        }
    }

    private static Sha256Digest digest(String... fields) {
        return sha(String.join("|", fields).getBytes(StandardCharsets.UTF_8));
    }

    private static Sha256Digest sha(byte[] bytes) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(bytes));
    }

    private static byte[] bounded(Path file, int maximum) throws Exception {
        if (!Files.isRegularFile(file) || Files.size(file) > maximum) {
            throw new IllegalArgumentException("M4 input is missing or exceeds its bound: " + file);
        }
        return Files.readAllBytes(file);
    }

    private static Map<?, ?> object(byte[] bytes) throws Exception {
        Object parsed = Json.parseBytesAs(bytes, Map.class);
        if (!(parsed instanceof Map<?, ?> value)) {
            throw new IllegalArgumentException("M4 receipt JSON is not an object");
        }
        return value;
    }

    private static Map<?, ?> nested(Map<?, ?> parent, String key) {
        if (!(parent.get(key) instanceof Map<?, ?> value)) {
            throw new IllegalArgumentException("M4 receipt lacks " + key);
        }
        return value;
    }

    private static String required(Properties value, String key) {
        String result = value.getProperty(PREFIX + key);
        if (result == null || result.isBlank()) {
            throw new IllegalArgumentException("M4 isolated admission lacks " + key);
        }
        return result;
    }

    private static String text(Map<?, ?> value, String key) {
        if (!(value.get(key) instanceof String result) || result.isBlank()) {
            throw new IllegalArgumentException("M4 receipt lacks " + key);
        }
        return result;
    }

    private static void requireText(Map<?, ?> value, String key, String expected) {
        if (!expected.equals(text(value, key))) {
            throw new IllegalArgumentException("M4 receipt differs at " + key);
        }
    }

    private static void requireBoolean(Map<?, ?> value, String key, boolean expected) {
        if (!(value.get(key) instanceof Boolean actual) || actual != expected) {
            throw new IllegalArgumentException("M4 receipt differs at " + key);
        }
    }
}
