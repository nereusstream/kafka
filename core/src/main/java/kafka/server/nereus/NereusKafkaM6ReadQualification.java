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

import org.w3c.dom.Element;

import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;

import static kafka.server.nereus.NereusKafkaM4AdmissionV2.bounded;
import static kafka.server.nereus.NereusKafkaM4AdmissionV2.nested;
import static kafka.server.nereus.NereusKafkaM4AdmissionV2.requireBoolean;
import static kafka.server.nereus.NereusKafkaM4AdmissionV2.requireText;
import static kafka.server.nereus.NereusKafkaM4AdmissionV2.sha;
import static kafka.server.nereus.NereusKafkaM4AdmissionV2.text;

/** Verifies the M6 read prerequisite without accepting it as an M5 or M6 promotion receipt. */
final class NereusKafkaM6ReadQualification {
    private static final String HELD_CAS = "blockedOxiaSelectorCasDoesNotHoldPartitionFenceOrOldBkReadCompletion()";
    private static final Map<String, Suite> SUITES = Map.of(
            "v2M5KafkaRunRootsRealTest", new Suite(
                    "com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperRunRootsV2RealTest", 16, HELD_CAS),
            "v2M5KafkaM4BrokerReadOwnerTest", new Suite(
                    "com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperM4BrokerReadOwnerV2Test", 5, null));

    private NereusKafkaM6ReadQualification() { }

    static void verify(Path output, Map<?, ?> summary) throws Exception {
        requireText(summary, "schema", "NEREUS_V2_M6_KAFKA_M4_READ_QUALIFICATION_V1");
        requireBoolean(summary, "promotionEligible", false);
        for (String key : List.of("sourceCommit", "sourceTree")) {
            require(text(summary, key).matches("[0-9a-f]{40}"), "invalid source identity: " + key);
        }
        verifyBackendFiles(output, nested(summary, "backendFiles"));
        verifySuites(output, summary.get("suites"));
    }

    private static void verifyBackendFiles(Path output, Map<?, ?> manifest) throws Exception {
        for (String name : List.of("backend.json", "bookkeeper.log", "oxia.log")) {
            byte[] bytes = bounded(output.resolve(name), 64_000_000);
            Map<?, ?> identity = nested(manifest, name);
            requireText(identity, "sha256", sha(bytes).toHex());
            require(identity.get("bytes") instanceof Number size && size.longValue() == bytes.length,
                    "backend evidence size differs: " + name);
        }
    }

    private static void verifySuites(Path output, Object value) throws Exception {
        require(value instanceof List<?> suites && suites.size() == SUITES.size(), "missing exact suites");
        var seen = new HashSet<String>();
        for (Object entry : (List<?>) value) {
            if (!(entry instanceof Map<?, ?> receipt)) {
                throw new IllegalArgumentException("malformed M6 read qualification suite");
            }
            String name = text(receipt, "task");
            Suite suite = SUITES.get(name);
            if (suite == null || !seen.add(name)) {
                throw new IllegalArgumentException("Unexpected or duplicate M6 read suite: " + name);
            }
            require(receipt.get("tests") instanceof Number count && count.longValue() == suite.count(),
                    "wrong test count: " + name);
            byte[] bytes = bounded(output.resolve(name + ".xml"), 16_777_216);
            requireText(receipt, "xmlSha256", sha(bytes).toHex());
            verifyXml(bytes, suite);
        }
    }

    private static void verifyXml(byte[] bytes, Suite suite) throws Exception {
        var factory = DocumentBuilderFactory.newDefaultInstance();
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        Element root = factory.newDocumentBuilder().parse(new ByteArrayInputStream(bytes)).getDocumentElement();
        require("testsuite".equals(root.getTagName()), "expected a single testsuite");
        require(suite.className().equals(root.getAttribute("name")), "unexpected testsuite class");
        require(Integer.toString(suite.count()).equals(root.getAttribute("tests")), "wrong XML test count");
        for (String counter : List.of("failures", "errors", "skipped")) {
            require("0".equals(root.getAttribute(counter)), "unsuccessful XML suite: " + counter);
        }
        for (String terminal : List.of("failure", "error", "skipped")) {
            require(root.getElementsByTagName(terminal).getLength() == 0, "unsuccessful XML case: " + terminal);
        }
        verifyCases(root, suite);
    }

    static void verifySingleCase(byte[] bytes, String className, String caseName) throws Exception {
        verifyXml(bytes, new Suite(className, 1, caseName));
    }

    private static void verifyCases(Element root, Suite suite) {
        var cases = root.getElementsByTagName("testcase");
        require(cases.getLength() == suite.count(), "wrong executed test count");
        var names = new HashSet<String>();
        for (int i = 0; i < cases.getLength(); i++) {
            Element test = (Element) cases.item(i);
            require(root == test.getParentNode() && suite.className().equals(test.getAttribute("classname")),
                    "unexpected testcase class or nesting");
            String name = test.getAttribute("name");
            require(!name.isBlank() && names.add(name), "missing or duplicate testcase identity");
        }
        require(suite.requiredCase() == null || names.contains(suite.requiredCase()), "missing held-selector case");
    }

    private static void require(boolean valid, String reason) {
        if (!valid) throw new IllegalArgumentException("Invalid M6 read qualification: " + reason);
    }

    private record Suite(String className, int count, String requiredCase) { }
}
