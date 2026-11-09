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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.retention.OxiaKafkaBookKeeperM4ReadRouteV2;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static kafka.server.nereus.NereusKafkaM4AdmissionV2.bounded;
import static kafka.server.nereus.NereusKafkaM4AdmissionV2.digest;
import static kafka.server.nereus.NereusKafkaM4AdmissionV2.nested;
import static kafka.server.nereus.NereusKafkaM4AdmissionV2.object;
import static kafka.server.nereus.NereusKafkaM4AdmissionV2.requireBoolean;
import static kafka.server.nereus.NereusKafkaM4AdmissionV2.requireText;
import static kafka.server.nereus.NereusKafkaM4AdmissionV2.sha;
import static kafka.server.nereus.NereusKafkaM4AdmissionV2.text;

/** Explicit current-source qualification of the native Object WAL read adapter. */
record NereusKafkaObjectReadAdmissionV1(Sha256Digest receipt, Sha256Digest inputs, Sha256Digest adapter,
        Sha256Digest backend, Sha256Digest contract, Sha256Digest verifier, long readLifetimeMillis) {
    private static final String PREFIX = "objectWalReadAdmission.";
    private static final String SCHEMA = "NEREUS_V2_M6_KAFKA_OBJECT_WAL_READ_QUALIFICATION_V1";
    private static final String SUITE = "kafka.server.nereus.KafkaObjectWalReadDrainRealTest";
    private static final String CASE = "waitsForOriginalObjectFetchBeforeClosingNativeOwner(Path)";

    static Optional<NereusKafkaObjectReadAdmissionV1> load(KafkaConfig config, NereusKafkaObjectProvider provider)
            throws Exception {
        var descriptor = NereusBookKeeperCapability.descriptor(
                Path.of(config.getString(NereusKafkaConfigs.BOOKKEEPER_CAPABILITY_FILE_CONFIG)));
        if (descriptor.stringPropertyNames().stream().noneMatch(key -> key.startsWith(PREFIX))) return Optional.empty();
        Path file = Path.of(required(descriptor, "receiptFile")).toRealPath();
        if (!file.endsWith("object-wal-read-qualification.json")) {
            throw new IllegalArgumentException("Native Object WAL read qualification file has an unexpected name");
        }
        byte[] receiptBytes = bounded(file, 1_048_576);
        var receipt = sha(receiptBytes);
        if (!receipt.toHex().equals(required(descriptor, "receiptSha256"))) {
            throw new IllegalArgumentException("Native Object WAL read qualification differs from the configured digest");
        }
        var summary = object(receiptBytes);
        requireText(summary, "schema", SCHEMA);
        requireBoolean(summary, "promotionEligible", false);
        verifySources(summary);
        var output = file.getParent();
        var suite = qualifiedFile(output, summary, "TEST-" + SUITE + ".xml");
        NereusKafkaM6ReadQualification.verifySingleCase(suite, SUITE, CASE);
        var runtime = qualifiedFile(output, summary, "nereusKafkaObjectWalReadDrainRealTest-runtime.json");
        var classes = nested(summary, "loadedReadClasses");
        var adapter = digest("M6-OBJECT-WAL-READ-ADAPTER-V1",
                readClass(classes, "kafka.server.nereus.NereusKafkaObjectWalPartition"),
                readClass(classes, "kafka.server.nereus.NereusKafkaObjectWalPartition$ReadView"),
                readClass(classes, "kafka.server.nereus.NereusKafkaObjectWalPartition$Source"),
                readClass(classes, "kafka.server.nereus.NereusKafkaObjectProvider"),
                readClass(classes, "com.nereusstream.storage.object.control.WalRunObjectSession"),
                readClass(classes, "com.nereusstream.storage.object.control.WalRunHistoricalObjectReaderV1"));
        var contract = digest("M6-OBJECT-WAL-READ-CONTRACT-V1",
                readClass(classes, "com.nereusstream.kafka.bookkeeper.broker.KafkaObjectNativeFetchV1"),
                readClass(classes, "com.nereusstream.kafka.bookkeeper.broker.KafkaObjectNativeFetchV1$Collect"),
                readClass(classes, "com.nereusstream.kafka.bookkeeper.read.KafkaObjectMaterializedPrefixV1"));
        var verifier = digest("M6-OBJECT-WAL-READ-VERIFIER-V1",
                readClass(classes, "com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1"),
                readClass(classes, "com.nereusstream.metadata.oxia.v2.retention.OxiaKafkaBookKeeperM4ReadRouteV2"));
        var profile = provider.profile();
        var backend = digest("M6-OBJECT-WAL-READ-BACKEND-V1", profile.providerConfigurationSha256().toHex(),
                config.nereusKafkaStorageConfig().core().oxiaNamespace());
        var readMillis = Math.max(config.nereusKafkaStorageConfig().fetch().timeout().toMillis(),
                config.nereusKafkaStorageConfig().lifecycle().recoveryTimeout().toMillis());
        return Optional.of(new NereusKafkaObjectReadAdmissionV1(receipt, sha(runtime), adapter, backend,
                contract, verifier, readMillis));
    }

    BindingReadSelector provision(OxiaKafkaBookKeeperM4ReadRouteV2 route, Optional<BindingReadSelector> previous,
            Sha256Digest view, long ownerEpoch) {
        return route.provisionDurableDrainOnly(adapter, backend, contract, verifier, inputs, receipt,
                readLifetimeMillis, view, ownerEpoch, previous);
    }

    private static void verifySources(Map<?, ?> receipt) {
        for (var key : List.of("sourceCheckout", "sourceTrees")) {
            var values = nested(receipt, key);
            if (values.size() != 2 || List.of("nereus", "kafka").stream()
                    .anyMatch(name -> !text(values, name).matches("[0-9a-f]{40}"))) {
                throw new IllegalArgumentException("Native Object WAL read qualification lacks exact source " + key);
            }
        }
    }

    private static byte[] qualifiedFile(Path output, Map<?, ?> receipt, String name) throws Exception {
        var input = output.resolve(name);
        if (!input.equals(input.toRealPath())) {
            throw new IllegalArgumentException("Redirected native Object WAL qualification input: " + name);
        }
        var bytes = bounded(input, 16_777_216);
        requireText(nested(receipt, "evidence"), name, sha(bytes).toHex());
        return bytes;
    }

    private static String readClass(Map<?, ?> classes, String name) throws Exception {
        var type = Class.forName(name, false, NereusKafkaObjectReadAdmissionV1.class.getClassLoader());
        try (var input = type.getResourceAsStream(name.substring(name.lastIndexOf('.') + 1) + ".class")) {
            if (input == null) throw new IllegalArgumentException("Missing native Object WAL read class: " + name);
            var actual = sha(input.readAllBytes()).toHex();
            requireText(classes, name, actual);
            return actual;
        }
    }

    private static String required(Properties descriptor, String name) {
        var value = descriptor.getProperty(PREFIX + name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Native Object WAL read qualification lacks " + name);
        }
        return value;
    }
}
