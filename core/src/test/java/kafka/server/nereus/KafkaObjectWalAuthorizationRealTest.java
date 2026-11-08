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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.config.NereusKafkaConfigs;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.PutResult;

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.OBJECT;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.assertOffsets;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.authorization;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.readUncommitted;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.scope;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.topic;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Measures the real Oxia authorization operations on the native Object WAL Produce path. */
public class KafkaObjectWalAuthorizationRealTest {
    @Test
    @Timeout(120)
    public void nativeObjectWalWaitsForExactAuthorizationAfterLostCasResponse(@TempDir Path work) throws Exception {
        var endpoint = URI.create(System.getProperty("nereus.nsip.s3Endpoint"));
        var oxia = System.getProperty("nereus.nsip.oxia");
        try (var minio = KafkaObjectProviderAdmissionRealTest.minio(endpoint);
                 var control = OxiaClientBuilder.create(oxia).asyncClient().get(30, TimeUnit.SECONDS)) {
            var fixture = KafkaObjectWalAdapterRealTest.provision(work, minio, control, endpoint,
                    System.getProperty("nereus.nsip.bkUri"), oxia);
            var cut = new AuthorizationCut();
            var bootstrap = BootstrapMetadata.fromVersion(MetadataVersion.latestTesting(), "nsip-object-auth")
                    .copyWithFeatureRecord("nereus.storage.version", (short) 2);
            var nodes = new TestKitNodes.Builder(bootstrap).setNumBrokerNodes(1).setNumControllerNodes(1).build();
            var builder = new KafkaClusterTestKit.Builder(nodes)
                    .setControllerStorageRuntimeFactory(new NereusControllerStorageRuntimeFactory())
                    .setBrokerStorageRuntimeFactory(context -> new NereusBrokerStorageRuntime(
                            context, new NereusKafkaOwnedProviderRuntime(context, cut)));
            fixture.settings().forEach((key, value) -> {
                if (!Set.of("node.id", "process.roles", "controller.quorum.voters", "log.dirs").contains(key)) {
                    builder.setConfigProp(key, value);
                }
            });
            builder.setConfigProp(NereusKafkaConfigs.CHECKPOINT_INTERVAL_MS_CONFIG, 10_000L);
            try (var cluster = builder.build()) {
                cluster.format();
                cluster.startup();
                cluster.waitForReadyBrokers();
                var client = KafkaBookKeeperProcessRealTest.client(0);
                client.setProperty("bootstrap.servers", cluster.bootstrapServers());
                verifyAuthorizationCut(client, control, fixture, cut);
                cluster.fatalFaultHandler().maybeRethrowFirstException();
                cluster.nonFatalFaultHandler().maybeRethrowFirstException();
            }
        }
    }

    private static void verifyAuthorizationCut(Properties client, AsyncOxiaClient control,
            KafkaObjectWalAdapterRealTest.Provisioned fixture, AuthorizationCut cut) throws Exception {
        try (var admin = Admin.create(client);
                 var producer = new KafkaProducer<String, String>(client)) {
            var created = admin.createTopics(List.of(topic(OBJECT, "OBJECT_WAL")));
            Uuid topicId = created.topicId(OBJECT.topic()).get(30, TimeUnit.SECONDS);
            assertOffsets(admin, 0, 0);
            var run = KafkaObjectWalAdapterRealTest.NativeTerminal.capture(fixture, control, topicId, OBJECT.topic(), 0);
            var fence = scope(fixture, topicId);
            var authority = authorization(run.metadata(), run.ref().shardId());
            String prefix = "/nereus-kafka-object-wal/" + fixture.profile().protocolCellSha256().toHex() + "/";
            String checkpointPagePrefix = WalRunControlKeys.checkpointPagePrefix(
                    run.ref().shardId(), run.ref().shardRunEpoch());
            String checkpointHeadKey = WalRunControlKeys.checkpointHeadKey(run.ref().shardId(), run.ref().shardRunEpoch());
            assertTrue(authority.read(fence).orElseThrow().grants().isEmpty());
            cut.arm(prefix + authority.headKey(fence), prefix + checkpointPagePrefix + "/");
            try {
                var request = producer.send(new ProducerRecord<>(OBJECT.topic(), 0, "key", "authorized"));
                var applied = cut.applied.get(30, TimeUnit.SECONDS);
                assertFalse(request.isDone(), "Produce completed before the original authorization CAS settled");
                assertEquals(1, cut.casAttempts.get());
                assertTrue(cut.readsBeforeCas.get() > 0);
                assertEquals(0, cut.reconciliationReads.get());
                var selected = authority.read(fence).orElseThrow();
                assertEquals(1, selected.endOffset());
                assertEquals(1, selected.grants().size());
                assertEquals(0, selected.grants().get(0).locator().startOffset());
                assertEquals(1, selected.grants().get(0).locator().endOffsetExclusive());

                cut.loseCas.complete(null);
                var reread = cut.reconciled.get(30, TimeUnit.SECONDS);
                assertEquals(applied, reread);
                assertFalse(request.isDone(), "Produce completed before the exact Oxia reread reached the Broker");
                cut.deliverReread.complete(null);
                String heldPage = cut.checkpointApplied.get(30, TimeUnit.SECONDS);
                assertEquals(0, request.get(30, TimeUnit.SECONDS).offset());
                assertEquals(1, cut.casAttempts.get());
                assertEquals(1, cut.reconciliationReads.get());
                assertEquals(1, cut.checkpointAttempts.get());
                assertTrue(heldPage.startsWith(prefix + checkpointPagePrefix + "/"));
                assertEquals(-1, WalRunControlCodec.decodeCheckpointHead(
                        run.metadata().get(checkpointHeadKey).orElseThrow()).pageOrdinal());
                assertOffsets(admin, 1, 1);
                assertEquals(List.of("authorized"), readUncommitted(client, OBJECT, 1));
                cut.deliverCheckpoint.complete(null);
                waitForCondition(() -> WalRunControlCodec.decodeCheckpointHead(
                        run.metadata().get(checkpointHeadKey).orElseThrow()).pageOrdinal() == 0,
                        10_000, "The deferred physical checkpoint did not publish after its page reply arrived");
                System.out.printf("Original Object ACK authorizationCas=%d preCasRead=%d exactReconciliationRead=%d"
                                + " deferredCheckpointPage=%d%n",
                        cut.casAttempts.get(), cut.readsBeforeCas.get(), cut.reconciliationReads.get(),
                        cut.checkpointAttempts.get());
            } finally {
                cut.loseCas.complete(null);
                cut.deliverReread.complete(null);
                cut.deliverCheckpoint.complete(null);
            }
        }
    }

    private record AuthorizationVersion(long version, CanonicalBytes body) { }

    private record Keys(String authorization, String checkpointPagePrefix) { }

    private static final class AuthorizationCut implements NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver {
        private final AtomicReference<Keys> keys = new AtomicReference<>();
        private final AtomicInteger casAttempts = new AtomicInteger();
        private final AtomicInteger readsBeforeCas = new AtomicInteger();
        private final AtomicInteger reconciliationReads = new AtomicInteger();
        private final AtomicInteger checkpointAttempts = new AtomicInteger();
        private final CompletableFuture<AuthorizationVersion> applied = new CompletableFuture<>();
        private final CompletableFuture<AuthorizationVersion> reconciled = new CompletableFuture<>();
        private final CompletableFuture<Void> loseCas = new CompletableFuture<>();
        private final CompletableFuture<Void> deliverReread = new CompletableFuture<>();
        private final CompletableFuture<String> checkpointApplied = new CompletableFuture<>();
        private final CompletableFuture<Void> deliverCheckpoint = new CompletableFuture<>();

        void arm(String exactKey, String checkpointPagePrefix) {
            assertTrue(keys.compareAndSet(null, new Keys(exactKey, checkpointPagePrefix)));
        }

        @Override
        public AsyncOxiaClient controlClient(AsyncOxiaClient client) {
            return (AsyncOxiaClient) Proxy.newProxyInstance(AsyncOxiaClient.class.getClassLoader(),
                    new Class<?>[]{AsyncOxiaClient.class}, (proxy, method, args) -> {
                        Object response;
                        try {
                            response = method.invoke(client, args);
                        } catch (InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                        var selected = keys.get();
                        if (selected == null || args == null || args.length == 0 || !(args[0] instanceof String key)) {
                            return response;
                        }
                        return switch (method.getName()) {
                            case "put" -> afterPut(selected, key, (byte[]) args[1], (CompletionStage<?>) response);
                            case "get" -> afterGet(selected, key, (CompletionStage<?>) response);
                            default -> response;
                        };
                    });
        }

        private Object afterPut(Keys selected, String key, byte[] bytes, CompletionStage<?> response) {
            if (key.startsWith(selected.checkpointPagePrefix()) && checkpointAttempts.incrementAndGet() == 1) {
                return response.thenCompose(value -> {
                    checkpointApplied.complete(key);
                    return deliverCheckpoint.thenApply(ignored -> value);
                }).toCompletableFuture();
            }
            if (!selected.authorization().equals(key) || casAttempts.incrementAndGet() != 1) {
                return response;
            }
            var body = CanonicalBytes.copyOf(bytes);
            return response.thenCompose(value -> {
                var result = (PutResult) value;
                applied.complete(new AuthorizationVersion(result.version().versionId(), body));
                return loseCas.thenCompose(ignored -> CompletableFuture.failedFuture(
                        new TimeoutException("applied exact authorization CAS response lost")));
            }).toCompletableFuture();
        }

        private Object afterGet(Keys selected, String key, CompletionStage<?> response) {
            if (!selected.authorization().equals(key)) return response;
            if (casAttempts.get() == 0) {
                readsBeforeCas.incrementAndGet();
                return response;
            }
            if (reconciliationReads.incrementAndGet() != 1) return response;
            return response.thenCompose(value -> {
                var result = (GetResult) value;
                reconciled.complete(new AuthorizationVersion(result.version().versionId(), CanonicalBytes.copyOf(result.value())));
                return deliverReread.thenApply(ignored -> result);
            }).toCompletableFuture();
        }
    }
}
