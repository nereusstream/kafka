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

import org.apache.kafka.common.errors.FencedLeaderEpochException;

import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaRawAssignedRecordBatchFactsV1;
import com.nereusstream.kafka.bookkeeper.broker.KafkaObjectNativeAppendV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentCommitCoordinatorV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectAuthorizationV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionPublicationKindV1;
import com.nereusstream.metadata.oxia.v2.objectwal.OxiaCanonicalControlMetadataStore;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.oxia.client.api.AsyncOxiaClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Forces native revocation at the local publication seam after actual Object and Oxia success. */
final class KafkaObjectRevokedAppendRealScenario {
    private KafkaObjectRevokedAppendRealScenario() { }

    static void verify(KafkaPartitionFenceV1 fence, KafkaObjectWalAdapterRealTest.Provisioned fixture,
            AsyncOxiaClient control, Oxia09ExactMetadataTransactionStoreV1 exact, NereusKafkaObjectProvider provider,
            NereusKafkaObjectWalKms kms) throws Exception {
        var timeout = Duration.ofSeconds(10);
        var profile = fixture.profile();
        var shard = NereusKafkaObjectWalShard.reserve(exact, profile.protocolCellSha256(), fence, timeout);
        var metadata = new OxiaCanonicalControlMetadataStore(control, shard.cellRoot(), shard.id());
        var roots = new NereusKafkaObjectWalRoot(fixture.config(), profile);
        var runs = new NereusKafkaObjectWalRuns(shard, metadata, exact, provider, kms, roots, timeout);
        var authority = new KafkaObjectAuthorizationV1(metadata, shard.id(),
                new KafkaObjectAuthorizationV1.Bounds(64, 16, 16L << 20, 32));
        verify(runs, metadata, authority, fence);
    }

    private static void verify(NereusKafkaObjectWalRuns runs,
            OxiaCanonicalControlMetadataStore metadata, KafkaObjectAuthorizationV1 authority,
            KafkaPartitionFenceV1 fence) throws Exception {
        var nativeOwner = KafkaObjectWalAdapterRealTest.election(fence);
        runs.admitNativeOwner(nativeOwner);
        var selected = runs.selectOrCreateGenesis(System.currentTimeMillis(), false, nativeOwner);
        try (var session = selected.freshSession().orElseThrow()) {
            var events = new ArrayList<KafkaPartitionPublicationKindV1>();
            var coordinator = KafkaCoherentCommitCoordinatorV1.bootstrapObject(fence, 0,
                    event -> events.add(event.kind()));
            authority.open(fence, 0, Optional.empty());
            var current = new AtomicBoolean(true);
            var writer = KafkaObjectNativeAppendV1.open(session, metadata, authority, coordinator,
                    Optional.empty(), current::get);
            var assignment = new NereusKafkaObjectWalPartition.NativeAssignment() {
                @Override
                public boolean current() {
                    return current.get();
                }

                @Override
                public <T> T whileCurrent(Callable<T> action) {
                    assertEquals(1, authority.read(fence).orElseThrow().endOffset());
                    assertTrue(current.compareAndSet(true, false));
                    throw new FencedLeaderEpochException("assignment revoked before local publication");
                }
            };
            var partition = new NereusKafkaObjectWalPartition(coordinator, writer, Map.of(),
                    Runnable::run, Runnable::run, assignment, System::currentTimeMillis, ignored -> {
                        throw new AssertionError("single append did not roll its run");
                    });
            try {
                var canonical = KafkaObjectWalAdapterRealTest.nativeBytes();
                var assigned = KafkaNativeAssignedRecordBatchV1.validate(
                        KafkaRawAssignedRecordBatchFactsV1.parse(canonical));
                try (var reservation = partition.admit(List.of(canonical.length()))) {
                    assertThrows(FencedLeaderEpochException.class, () -> partition.append(
                            reservation, List.of(assigned), snapshot -> {
                                throw new AssertionError("revoked assignment acknowledged a durable grant");
                            }));
                }
                var frontiers = partition.capture().root().frontiers();
                assertEquals(0, frontiers.durableEndOffset());
                assertEquals(0, frontiers.readableEndOffset());
                assertFalse(events.contains(KafkaPartitionPublicationKindV1.COMMIT));
                assertEquals(1, authority.read(fence).orElseThrow().grants().size());
            } finally {
                partition.closeAsync().get(10, TimeUnit.SECONDS);
            }
        }
    }
}
