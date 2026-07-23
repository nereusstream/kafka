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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.image.AclsImage;
import org.apache.kafka.image.ClientQuotasImage;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.ConfigurationsImage;
import org.apache.kafka.image.DelegationTokenImage;
import org.apache.kafka.image.FeaturesImage;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.ProducerIdsImage;
import org.apache.kafka.image.ScramImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.KRaftMetadataCache;
import org.apache.kafka.server.common.KRaftVersion;

import com.nereusstream.api.NereusException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NereusKafkaContextAdaptersTest {
    @TempDir
    Path logDirectory;

    @Test
    void adaptsKafkaTimeWithoutOwningIt() {
        MockTime time = new MockTime(0, 1234, 17);
        NereusKafkaClock clock = new NereusKafkaClock(time);

        assertEquals(1234, clock.millis());
        assertEquals(1234, clock.instant().toEpochMilli());
        assertSame(clock, clock.withZone(clock.getZone()));
        assertEquals(
                ZoneId.of("Asia/Shanghai"),
                clock.withZone(ZoneId.of("Asia/Shanghai")).getZone());

        time.sleep(9);
        assertEquals(1243, clock.millis());
    }

    @Test
    void rejectsSnapshotUntilInitialKRaftImageExists() {
        KRaftMetadataCache cache = cache();
        NereusKafkaStorageClusterSnapshotProvider provider =
                new NereusKafkaStorageClusterSnapshotProvider(
                        "kafka-a", cache, List.of(logDirectory));

        CompletionException failure = org.junit.jupiter.api.Assertions.assertThrows(
                CompletionException.class,
                () -> provider.currentSnapshot().toCompletableFuture().join());

        assertTrue(failure.getCause() instanceof NereusException);
        assertTrue(((NereusException) failure.getCause()).retriable());
    }

    @Test
    void capturesSortedBrokerEpochsAndConservativeLocalLogFact() throws Exception {
        KRaftMetadataCache cache = cache();
        cache.setImage(image(11));
        Files.createDirectory(logDirectory.resolve("__cluster_metadata-0"));
        NereusKafkaStorageClusterSnapshotProvider provider =
                new NereusKafkaStorageClusterSnapshotProvider(
                        "kafka-a", cache, List.of(logDirectory));

        var empty = provider.currentSnapshot().toCompletableFuture().join();
        assertEquals(11, empty.metadataOffset());
        assertEquals(List.of(2, 7), empty.brokers().stream()
                .map(identity -> identity.brokerId())
                .toList());
        assertFalse(empty.topicsPresent());
        assertFalse(empty.authoritativeLocalLogsPresent());
        assertFalse(empty.bindingsPresent());

        Files.createDirectory(logDirectory.resolve("orders-0"));
        var withLog = provider.currentSnapshot().toCompletableFuture().join();
        assertTrue(withLog.authoritativeLocalLogsPresent());
    }

    private static KRaftMetadataCache cache() {
        return new KRaftMetadataCache(
                2, () -> KRaftVersion.KRAFT_VERSION_1);
    }

    private static MetadataImage image(long offset) {
        Map<Integer, BrokerRegistration> brokers = Map.of(
                7, registration(7, 19),
                2, registration(2, 23));
        return new MetadataImage(
                new MetadataProvenance(offset, 1, 1, true),
                FeaturesImage.EMPTY,
                new ClusterImage(brokers, Map.of()),
                TopicsImage.EMPTY,
                ConfigurationsImage.EMPTY,
                ClientQuotasImage.EMPTY,
                ProducerIdsImage.EMPTY,
                AclsImage.EMPTY,
                ScramImage.EMPTY,
                DelegationTokenImage.EMPTY);
    }

    private static BrokerRegistration registration(int id, long epoch) {
        return new BrokerRegistration.Builder()
                .setId(id)
                .setEpoch(epoch)
                .setIncarnationId(Uuid.randomUuid())
                .setListeners(Map.of())
                .setSupportedFeatures(Map.of())
                .setDirectories(List.of())
                .build();
    }
}
