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
package kafka.server.nsip;

import kafka.server.nereus.NereusBrokerStorageRuntime;
import kafka.server.nereus.NereusControllerStorageRuntimeFactory;
import kafka.server.nereus.NereusKafkaOwnedProviderRuntime;
import kafka.server.storage.BrokerStorageRuntimeFactory;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.server.common.MetadataVersion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Drops only the callback after a real Oxia OPEN write; the Broker must reread exact persisted bytes. */
final class KafkaBookKeeperOpenResponseLossRealScenario {
    private KafkaBookKeeperOpenResponseLossRealScenario() { }

    static void run(Optional<Path> m4Summary) throws Exception {
        String uri = System.getProperty("nereus.nsip.bkUri");
        String oxia = System.getProperty("nereus.nsip.oxia");
        if (uri == null || oxia == null) throw new IllegalStateException("real BK URI and Oxia address are required");
        Path inputs = Files.createTempDirectory("nereus-nsip-open-response-");
        try {
            KafkaBookKeeperOwnerRelocationRealTest.writeCapability(inputs, m4Summary);
            var bootstrap = BootstrapMetadata.fromVersion(MetadataVersion.latestTesting(), "nsip-open-response")
                    .copyWithFeatureRecord("nereus.storage.version", (short) 2);
            var nodes = new TestKitNodes.Builder(bootstrap).setNumBrokerNodes(1).setNumControllerNodes(1).build();
            var dropped = new AtomicBoolean();
            var responses = new NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver() {
                @Override
                public CompletionStage<Void> afterOpen(CompletionStage<Void> response) {
                    return response.thenCompose(ignored -> {
                        if (dropped.compareAndSet(false, true)) {
                            return CompletableFuture.failedFuture(
                                    new IllegalStateException("injected loss of persisted OPEN response"));
                        }
                        return CompletableFuture.completedFuture(null);
                    });
                }
            };
            BrokerStorageRuntimeFactory factory = context -> new NereusBrokerStorageRuntime(
                    context, new NereusKafkaOwnedProviderRuntime(context, responses));
            var builder = new KafkaClusterTestKit.Builder(nodes)
                    .setBrokerStorageRuntimeFactory(factory)
                    .setControllerStorageRuntimeFactory(new NereusControllerStorageRuntimeFactory());
            KafkaBookKeeperOwnerRelocationRealTest.nativeConfig(inputs, uri, oxia).forEach(builder::setConfigProp);
            try (KafkaClusterTestKit cluster = builder.build()) {
                cluster.format();
                cluster.startup();
                cluster.waitForReadyBrokers();
                var client = new Properties();
                client.put("bootstrap.servers", cluster.bootstrapServers());
                try (Admin admin = Admin.create(client)) {
                    admin.createTopics(List.of(new NewTopic("native-cold", Map.of(0, List.of(0)))))
                            .all()
                            .get(30, TimeUnit.SECONDS);
                    var producerConfig = new Properties();
                    producerConfig.putAll(client);
                    producerConfig.put("key.serializer", StringSerializer.class.getName());
                    producerConfig.put("value.serializer", StringSerializer.class.getName());
                    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig)) {
                        assertEquals(0, producer.send(new ProducerRecord<>("native-cold", 0, "key", "after-open-loss"))
                                .get(30, TimeUnit.SECONDS).offset());
                    }
                    assertTrue(dropped.get(), "the actual Oxia OPEN response was not dropped");
                    assertEquals(List.of("after-open-loss"), KafkaBookKeeperOwnerRelocationRealTest.consume(client, 1));
                }
                cluster.fatalFaultHandler().maybeRethrowFirstException();
                cluster.nonFatalFaultHandler().maybeRethrowFirstException();
            }
        } finally {
            try (var files = Files.walk(inputs)) {
                for (var file : files.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(file);
            }
        }
    }
}
