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

import kafka.log.nereus.NereusUnifiedLog;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.KafkaClusterTestKit;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;

import static kafka.server.nereus.KafkaObjectWalProcessRealTest.awaitInitialOwner;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.readUncommitted;
import static kafka.server.nereus.KafkaObjectWalProcessRealTest.topic;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Holds one real physical checkpoint page while the Controller supersedes the recovering Owner. */
final class KafkaObjectRecoverySupersessionRealScenario implements NereusKafkaOwnedProviderRuntime.NativeRunResponseObserver {
    private static final TopicPartition PARTITION = new TopicPartition("native-object-superseded", 0);

    private final KafkaObjectCheckpointHeadRealScenario terminal;
    private volatile HeldPage page;

    private record HeldPage(String key, CompletableFuture<GetResult> observed, CompletableFuture<Void> release) { }

    KafkaObjectRecoverySupersessionRealScenario(KafkaObjectCheckpointHeadRealScenario terminal) {
        this.terminal = terminal;
    }

    void verify(KafkaClusterTestKit cluster, Properties client, AsyncOxiaClient control,
            KafkaObjectWalAdapterRealTest.Provisioned fixture) throws Exception {
        try (var admin = Admin.create(client); var producer = new KafkaProducer<String, String>(client)) {
            var topicId = admin.createTopics(List.of(topic(PARTITION, "OBJECT_WAL")))
                    .topicId(PARTITION.topic()).get(30, TimeUnit.SECONDS);
            awaitInitialOwner(admin, PARTITION);
            assertEquals(0, producer.send(new ProducerRecord<>(PARTITION.topic(), 0, "key", "before"))
                    .get(30, TimeUnit.SECONDS).offset());

            var root = KafkaObjectWalAdapterRealTest.NativeTerminal.capture(fixture, control, topicId, PARTITION.topic(), 0);
            waitForCondition(() -> root.physicalPageKey().isPresent(), "The original physical checkpoint did not publish");
            String prefix = "/nereus-kafka-object-wal/" + fixture.profile().protocolCellSha256().toHex() + "/";
            String key = prefix + root.physicalPageKey().orElseThrow();
            var exact = control.get(key).get(30, TimeUnit.SECONDS);
            var held = new HeldPage(key, new CompletableFuture<>(), new CompletableFuture<>());
            page = held;
            try {
                assign(admin, 1);
                assertEquals(exact.version().versionId(), held.observed().get(30, TimeUnit.SECONDS).version().versionId());
                var pending = cluster.brokers().get(1).replicaManager().onlinePartition(PARTITION);
                assertTrue(pending.isDefined());
                int oldEpoch = pending.get().getLeaderEpoch();
                assertFalse(writable(cluster, 1, oldEpoch));

                assign(admin, 0);
                waitForCondition(() -> {
                    cluster.nonFatalFaultHandler().maybeRethrowFirstException();
                    var replacement = cluster.brokers().get(0).replicaManager().onlinePartition(PARTITION);
                    return replacement.isDefined() && replacement.get().getLeaderEpoch() > oldEpoch
                            && writable(cluster, 0, replacement.get().getLeaderEpoch());
                }, 15_000, "The latest Controller assignment never became writable");
                assertFalse(writable(cluster, 1, oldEpoch));

                assertEquals(1, producer.send(new ProducerRecord<>(PARTITION.topic(), 0, "key", "after"))
                        .get(30, TimeUnit.SECONDS).offset());
                var successor = KafkaObjectWalAdapterRealTest.NativeTerminal.capture(
                        fixture, control, topicId, PARTITION.topic(), 0);
                assertEquals(root.ref(), successor.root().predecessor().orElseThrow().root());
                held.release().complete(null);
                waitForCondition(() -> cluster.brokers().get(1).metadataCache().getImage().topics()
                        .getTopic(topicId).partitions().get(0).leader == 0,
                        "The superseded Broker did not process the newer Controller assignment");
                assertFalse(writable(cluster, 1, oldEpoch));
                assertEquals(List.of("before", "after"), readUncommitted(client, PARTITION, 2));
                cluster.nonFatalFaultHandler().maybeRethrowFirstException();
                admin.deleteTopics(List.of(PARTITION.topic())).all().get(30, TimeUnit.SECONDS);
            } finally {
                held.release().complete(null);
                page = null;
            }
        }
    }

    private static void assign(Admin admin, int broker) throws Exception {
        admin.alterPartitionReassignments(Map.of(PARTITION, Optional.of(new NewPartitionReassignment(List.of(broker)))))
                .all().get(30, TimeUnit.SECONDS);
    }

    private static boolean writable(KafkaClusterTestKit cluster, int broker, int epoch) {
        var local = cluster.brokers().get(broker).replicaManager().localLog(PARTITION);
        return local.isDefined() && local.get() instanceof NereusUnifiedLog log && log.nereusWritable(epoch);
    }

    @Override
    public AsyncOxiaClient controlClient(AsyncOxiaClient client) {
        var delegate = terminal.controlClient(client);
        return (AsyncOxiaClient) Proxy.newProxyInstance(AsyncOxiaClient.class.getClassLoader(),
                new Class<?>[] {AsyncOxiaClient.class}, (proxy, method, args) -> {
                    Object response;
                    try {
                        response = method.invoke(delegate, args);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                    var held = page;
                    if (held == null || !method.getName().equals("get") || args == null
                            || args.length == 0 || !held.key().equals(args[0])) {
                        return response;
                    }
                    return ((CompletionStage<?>) response).thenCompose(value -> {
                        var result = (GetResult) value;
                        if (!held.observed().complete(result)) return CompletableFuture.completedFuture(result);
                        return held.release().thenApply(ignored -> result);
                    }).toCompletableFuture();
                });
    }
}
