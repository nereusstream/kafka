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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.config.NereusKafkaConfigs;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaKafkaBookKeeperM4ReadRouteV2;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import io.oxia.client.api.AsyncOxiaClient;

import static kafka.server.nereus.KafkaObjectWalProcessRealTest.awaitInitialOwner;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.readUncommitted;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.topic;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Uses a small, complete Object WAL key domain sealed by the real Controller handoff. */
final class KafkaObjectNativeCompactionRealScenario {
    static final TopicPartition COMPACT = new TopicPartition("native-object-compacted", 0);
    private static final List<String> SELECTED = List.of("kept", "latest");

    private KafkaObjectNativeCompactionRealScenario() { }

    static Map<String, Object> configureReadQualification(Path work, KafkaObjectWalAdapterRealTest.Provisioned fixture)
            throws Exception {
        var receipt = Path.of(System.getProperty("nereus.nsip.objectWalReadAdmissionFile")).toRealPath();
        var digest = NereusKafkaM4AdmissionV2.sha(Files.readAllBytes(receipt));
        Files.writeString(work.resolve("capability.properties"), "\nobjectWalReadAdmission.receiptFile=" + receipt
                + "\nobjectWalReadAdmission.receiptSha256=" + digest.toHex() + "\n", StandardOpenOption.APPEND);
        var settings = new HashMap<>(fixture.settings());
        settings.put(NereusKafkaConfigs.RETENTION_CHECK_INTERVAL_MS_CONFIG, "1000");
        return settings;
    }

    static void installCapacity(KafkaObjectWalAdapterRealTest.Provisioned fixture, AsyncOxiaClient control, Uuid topicId) {
        var scope = KafkaObjectWalProcessRealTest.scope(fixture, topicId);
        var binding = new BindingIdentity(scope.bindingId(),
                Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(scope.topicIncarnation())),
                scope.storageEpochId().digest());
        var route = OxiaKafkaBookKeeperM4ReadRouteV2.forObjectWal(control, fixture.namespace().identity(),
                scope.partitionId(), binding);
        KafkaBookKeeperObjectMaterializationRealTest.ScheduledLimits.install(route, fixture.profile().protocolCellSha256());
    }

    static void seed(Admin admin, Properties client) throws Exception {
        var topic = topic(COMPACT, "OBJECT_WAL").configs(Map.of(
                "nereus.storage.profile", "OBJECT_WAL", "cleanup.policy", "compact"));
        admin.createTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
        awaitInitialOwner(admin, COMPACT);
        try (var producer = new KafkaProducer<String, String>(client)) {
            append(producer, 0, "duplicate", "old");
            append(producer, 1, "unrelated", "kept");
            append(producer, 2, "duplicate", "latest");
        }
        assertEquals(List.of("old", "kept", "latest"), readUncommitted(client, COMPACT, 3));
    }

    private static void append(KafkaProducer<String, String> producer, long offset, String key, String value)
            throws Exception {
        assertEquals(offset, producer.send(new ProducerRecord<>(COMPACT.topic(), 0, key, value))
                .get(30, TimeUnit.SECONDS).offset());
    }

    static void awaitSelected(Admin admin, Properties client) throws Exception {
        waitForCondition(() -> SELECTED.equals(readUncommitted(client, COMPACT, 2)),
                30_000, "Native Object WAL Fetch never selected the compacted sealed key domain");
        requireSelected(admin, client);
    }

    static void requireSelected(Admin admin, Properties client) throws Exception {
        assertEquals(3, admin.listOffsets(Map.of(COMPACT, OffsetSpec.latest())).partitionResult(COMPACT)
                .get(5, TimeUnit.SECONDS).offset());
        assertEquals(SELECTED, readUncommitted(client, COMPACT, 2));
    }
}
