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

import kafka.server.nsip.KafkaBookKeeperProcessRealTest;
import kafka.server.nsip.KafkaBookKeeperProcessRealTest.NativeNode;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.config.NereusKafkaConfigs;
import org.apache.kafka.test.TestUtils;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static kafka.server.nereus.KafkaObjectWalProcessRealTest.OBJECT;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.authorization;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.readUncommitted;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.scope;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.topic;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Independent native Object read admission proof using the original held Provider response. */
public class KafkaObjectWalReadDrainRealTest {
    @Test
    @Timeout(120)
    public void waitsForOriginalObjectFetchBeforeClosingNativeOwner(@TempDir Path work) throws Exception {
        var endpoint = URI.create(System.getProperty("nereus.nsip.s3Endpoint"));
        var oxia = System.getProperty("nereus.nsip.oxia");
        try (var minio = KafkaObjectProviderAdmissionRealTest.minio(endpoint);
                 var control = OxiaClientBuilder.create(oxia).asyncClient().get(30, TimeUnit.SECONDS)) {
            var fixture = KafkaObjectWalAdapterRealTest.provision(work, minio, control, KafkaObjectFaultRelay.endpoint(),
                    System.getProperty("nereus.nsip.bkUri"), oxia);
            verify(work, fixture, control, minio);
        } finally {
            KafkaBookKeeperProcessRealTest.copyProcessLogs(work);
        }
    }

    private static void verify(Path work, KafkaObjectWalAdapterRealTest.Provisioned fixture, AsyncOxiaClient control,
            S3Client minio) throws Exception {
        var settings = fixture.settings();
        var bucket = (String) settings.get(NereusKafkaConfigs.OBJECT_BUCKET_CONFIG);
        int[] ports = KafkaBookKeeperProcessRealTest.allocatePorts();
        var voters = "2@127.0.0.1:" + ports[2];
        var cluster = Uuid.randomUuid().toString();
        try (var controller = new NativeNode(work, settings, 2, ports[2], voters, true);
                 var broker = new NativeNode(work, settings, 0, ports[0], voters, false)) {
            controller.format(cluster);
            broker.format(cluster);
            controller.start();
            broker.start();
            var client = KafkaBookKeeperProcessRealTest.client(ports[0]);
            try (var admin = Admin.create(client); var producer = new KafkaProducer<String, String>(client)) {
                KafkaBookKeeperProcessRealTest.waitForBroker(admin, 0);
                var topicId = admin.createTopics(List.of(topic(OBJECT, "OBJECT_WAL")))
                        .topicId(OBJECT.topic()).get(30, TimeUnit.SECONDS);
                assertEquals(0, producer.send(new ProducerRecord<>(OBJECT.topic(), 0, "key", "pinned"))
                        .get(30, TimeUnit.SECONDS).offset());
                var terminal = KafkaObjectWalAdapterRealTest.NativeTerminal.capture(fixture, control, topicId, OBJECT.topic(), 0);
                var head = authorization(terminal.metadata(), terminal.ref().shardId()).read(scope(fixture, topicId)).orElseThrow();
                var key = head.grants().getFirst().object().key();
                try (var relay = new KafkaObjectFaultRelay(bucket, key, "*")) {
                    relay.arm("held-unversioned");
                    verifyShutdownWaits(work, broker, ports[0], topicId, terminal, relay);
                }
                terminal.requireSealedByOriginalWriter(minio, bucket, 1);
                broker.start();
                KafkaBookKeeperProcessRealTest.waitForBroker(admin, 0);
                assertEquals(List.of("pinned"), readUncommitted(client, OBJECT, 1));
            }
        }
    }

    private static void verifyShutdownWaits(Path work, NativeNode broker, int port, Uuid topicId,
            KafkaObjectWalAdapterRealTest.NativeTerminal terminal, KafkaObjectFaultRelay relay) throws Exception {
        var original = terminal.metadata().getVersioned(terminal.checkpointHeadKey()).orElseThrow();
        short version = 16;
        var fetch = FetchRequest.Builder.forConsumer(version, 0, 1,
                        Map.of(OBJECT, new FetchRequest.PartitionData(topicId, 0,
                                FetchRequest.INVALID_LOG_START_OFFSET, 4096, Optional.empty())))
                .setMaxBytes(4096).isolationLevel(IsolationLevel.READ_COMMITTED).build(version);
        try (var request = new NativeKafkaRequest(port, fetch)) {
            relay.awaitAppliedRange();
            assertThrows(SocketTimeoutException.class, () -> request.receive(200));
        }
        var shutdown = CompletableFuture.runAsync(() -> {
            try {
                broker.stop();
            } catch (Exception failure) {
                throw new CompletionException(failure);
            }
        });
        try {
            TestUtils.waitForCondition(() -> Files.readString(work.resolve("node-0/server.log"))
                    .contains("[BrokerServer id=0] shutting down"), 5_000, "Broker did not enter native shutdown");
            assertThrows(TimeoutException.class, () -> shutdown.get(200, TimeUnit.MILLISECONDS));
            assertEquals(Optional.of(original), terminal.metadata().getVersioned(terminal.checkpointHeadKey()));
        } finally {
            relay.release();
        }
        shutdown.get(30, TimeUnit.SECONDS);
    }
}
