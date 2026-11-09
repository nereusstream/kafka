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

import kafka.server.nsip.KafkaBookKeeperOwnerRelocationRealTest;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.test.KafkaClusterTestKit;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaProtocolCheckpointHeadCodecV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaProtocolCheckpointHeadStateV1;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;

import static kafka.server.nereus.KafkaObjectWalProcessRealTest.OBJECT;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.assertOffsets;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.readUncommitted;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Loses the real terminal protocol Head CAS reply during native Kafka shutdown. */
final class KafkaObjectCheckpointHeadRealScenario implements NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver {
    private final AtomicInteger terminalWrites = new AtomicInteger();
    private volatile Cut cut;

    private record Observation(long version, CanonicalBytes body) { }

    private record Cut(KafkaObjectWalAdapterRealTest.NativeTerminal run, String key,
                       CompletableFuture<Observation> applied, CompletableFuture<Observation> reread,
                       CompletableFuture<Void> loseReply, CompletableFuture<Void> deliverRead) { }

    KafkaObjectWalAdapterRealTest.NativeTerminal verify(KafkaClusterTestKit cluster, Properties client,
            AsyncOxiaClient control,
            KafkaObjectWalAdapterRealTest.Provisioned fixture, Uuid topicId) throws Exception {
        try (var producer = new KafkaProducer<String, String>(client)) {
            assertEquals(2, producer.send(new ProducerRecord<>(OBJECT.topic(), 0, "key", "after-recovery"))
                    .get(30, TimeUnit.SECONDS).offset());
        }
        var run = KafkaObjectWalAdapterRealTest.NativeTerminal.capture(fixture, control, topicId, OBJECT.topic(), 1);
        String relative = run.checkpointHeadKey();
        String key = "/nereus-kafka-object-wal/" + fixture.profile().protocolCellSha256().toHex() + "/" + relative;
        assertTrue(run.metadata().get(relative).isEmpty());
        var selected = new Cut(run, key, new CompletableFuture<>(), new CompletableFuture<>(),
                new CompletableFuture<>(), new CompletableFuture<>());
        cut = selected;
        var shutdown = CompletableFuture.runAsync(cluster.brokers().get(1)::shutdown);
        try (var admin = Admin.create(client)) {
            var terminal = selected.applied().get(30, TimeUnit.SECONDS);
            var exact = run.metadata().getVersioned(relative).orElseThrow();
            assertEquals(terminal.body(), exact.canonicalStoredBytes());
            assertFalse(shutdown.isDone(), "The Broker shut down while its terminal Head response was held");
            selected.loseReply().complete(null);
            assertEquals(terminal, selected.reread().get(30, TimeUnit.SECONDS));
            assertFalse(shutdown.isDone(), "The Broker shut down before the terminal Head reread returned");
            selected.deliverRead().complete(null);
            shutdown.get(30, TimeUnit.SECONDS);
            assertEquals(1, terminalWrites.get());
            assertEquals(exact, run.metadata().getVersioned(relative).orElseThrow());
            assertEquals(0, KafkaBookKeeperOwnerRelocationRealTest.awaitLeader(admin, 1));
            assertEquals(List.of("authorized", "authorized-before-revocation", "after-recovery"),
                    readUncommitted(client, OBJECT, 3));
            assertOffsets(admin, 3, 3);
            assertEquals(exact, run.metadata().getVersioned(relative).orElseThrow());
            return run;
        } finally {
            selected.loseReply().complete(null);
            selected.deliverRead().complete(null);
        }
    }

    @Override
    public AsyncOxiaClient controlClient(AsyncOxiaClient client) {
        return (AsyncOxiaClient) Proxy.newProxyInstance(AsyncOxiaClient.class.getClassLoader(),
                new Class<?>[] {AsyncOxiaClient.class}, (proxy, method, args) -> {
                    Object response;
                    try {
                        response = method.invoke(client, args);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                    var selected = cut;
                    if (selected == null || args == null || args.length == 0 || !selected.key().equals(args[0])) {
                        return response;
                    }
                    return switch (method.getName()) {
                        case "put" -> terminalReply(selected, CanonicalBytes.copyOf((byte[]) args[1]),
                                (CompletionStage<?>) response);
                        case "get" -> reconciliationRead(selected, (CompletionStage<?>) response);
                        default -> response;
                    };
                });
    }

    private Object terminalReply(Cut selected, CanonicalBytes candidate, CompletionStage<?> response) {
        var head = KafkaProtocolCheckpointHeadCodecV1.decode(
                selected.run().checkpointPrefix(), selected.run().ref().rootSha256(), candidate);
        if (head.state() != KafkaProtocolCheckpointHeadStateV1.TERMINAL) {
            return response;
        }
        terminalWrites.incrementAndGet();
        return response.thenCompose(value -> {
            var put = (PutResult) value;
            if (!selected.applied().complete(new Observation(put.version().versionId(), candidate))) {
                throw new AssertionError("Terminal Head CAS was dispatched twice");
            }
            return selected.loseReply().thenCompose(ignored ->
                    CompletableFuture.failedFuture(new TimeoutException("applied terminal NWKCP1 Head reply lost")));
        }).toCompletableFuture();
    }

    private static Object reconciliationRead(Cut selected, CompletionStage<?> response) {
        if (!selected.loseReply().isDone()) {
            return response;
        }
        return response.thenCompose(value -> {
            var get = (GetResult) value;
            if (selected.reread().complete(
                    new Observation(get.version().versionId(), CanonicalBytes.copyOf(get.value())))) {
                return selected.deliverRead().thenApply(ignored -> get);
            }
            return CompletableFuture.completedFuture(get);
        }).toCompletableFuture();
    }
}
