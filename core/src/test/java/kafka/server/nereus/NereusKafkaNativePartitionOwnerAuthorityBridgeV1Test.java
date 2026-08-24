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

import kafka.cluster.Partition;
import kafka.server.ReplicaManager;

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.FencedLeaderEpochException;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.KRaftMetadataCache;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.storage.internals.log.PartitionLeaderAuthority;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.partition.KafkaPartitionLeaderOpenRequest;
import com.nereusstream.storage.object.control.WalRunObjectSession;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import scala.Option;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NereusKafkaNativePartitionOwnerAuthorityBridgeV1Test {
    @Test
    void executesExactlyOnceUnderTheExactCurrentKafkaAuthority() throws IOException {
        Facts facts = new Facts(5, 9, 10);
        AtomicInteger callbacks = new AtomicInteger();
        WalRunObjectSession expected = mock(WalRunObjectSession.class);

        WalRunObjectSession actual = facts.bridge.executeWhileCurrentOwner(
                facts.fence(9, 5),
                () -> {
                    callbacks.incrementAndGet();
                    assertTrue(facts.partitionLock.getReadHoldCount() > 0);
                    assertFalse(facts.partitionLock.writeLock().tryLock());
                    return expected;
                });

        assertSame(expected, actual);
        assertEquals(1, callbacks.get());
    }

    @Test
    void propagatesProviderIoFailureAndReleasesTheOwnerGuard() throws IOException {
        Facts facts = new Facts(5, 9, 10);
        IOException expected = new IOException("provider unavailable");

        IOException actual = assertThrows(IOException.class, () ->
                facts.bridge.executeWhileCurrentOwner(
                        facts.fence(9, 5),
                        () -> {
                            throw expected;
                        }));
        assertSame(expected, actual);

        WalRunObjectSession recovered = mock(WalRunObjectSession.class);
        assertSame(recovered, facts.bridge.executeWhileCurrentOwner(
                facts.fence(9, 5),
                () -> recovered));
    }

    @Test
    void rejectsRegressedOrSubstitutedFenceBeforeTheCallback() {
        Facts facts = new Facts(6, 10, 11);
        AtomicInteger callbacks = new AtomicInteger();

        assertThrows(FencedLeaderEpochException.class, () ->
                facts.bridge.executeWhileCurrentOwner(
                        facts.fence(9, 6),
                        () -> countedSession(callbacks)));
        assertThrows(FencedLeaderEpochException.class, () ->
                facts.bridge.executeWhileCurrentOwner(
                        facts.fence(10, 5),
                        () -> countedSession(callbacks)));
        KafkaPartitionFenceV1 differentTopic = new KafkaPartitionFenceV1(
                facts.bindingId,
                new KafkaTopicIncarnationIdentity(
                        new KafkaTopicId(new Id128(4, 5)),
                        new KafkaTopicName("events")),
                0,
                1,
                facts.storageEpochId,
                10,
                6);
        assertThrows(FencedLeaderEpochException.class, () ->
                facts.bridge.executeWhileCurrentOwner(
                        differentTopic,
                        () -> countedSession(callbacks)));

        assertEquals(0, callbacks.get());
    }

    @Test
    void rejectsLateOwnerWhenBrokerOrMetadataAuthorityAdvanced() {
        Facts brokerAdvanced = new Facts(5, 9, 10);
        brokerAdvanced.processBrokerEpoch.set(10);
        AtomicInteger brokerCallbacks = new AtomicInteger();

        assertThrows(FencedLeaderEpochException.class, () ->
                brokerAdvanced.bridge.executeWhileCurrentOwner(
                        brokerAdvanced.fence(9, 5),
                        () -> countedSession(brokerCallbacks)));
        assertEquals(0, brokerCallbacks.get());

        Facts metadataRegressed = new Facts(5, 9, 10);
        metadataRegressed.metadataOffset.set(9);
        AtomicInteger metadataCallbacks = new AtomicInteger();
        assertThrows(FencedLeaderEpochException.class, () ->
                metadataRegressed.bridge.executeWhileCurrentOwner(
                        metadataRegressed.fence(9, 5),
                        () -> countedSession(metadataCallbacks)));
        assertEquals(0, metadataCallbacks.get());
    }

    @Test
    void blocksLeaderSupersedeForTheCompleteProviderCallback() throws Exception {
        Facts facts = new Facts(5, 9, 10);
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        CountDownLatch takeoverAttempted = new CountDownLatch(1);
        CountDownLatch takeoverBlocked = new CountDownLatch(1);
        CountDownLatch takeoverFinished = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<WalRunObjectSession> ownerOpen = executor.submit(() ->
                    facts.bridge.executeWhileCurrentOwner(
                            facts.fence(9, 5),
                            () -> {
                                callbackEntered.countDown();
                                await(releaseCallback);
                                return mock(WalRunObjectSession.class);
                            }));
            assertTrue(callbackEntered.await(5, TimeUnit.SECONDS));

            Future<?> takeover = executor.submit(() -> {
                takeoverAttempted.countDown();
                boolean acquired = facts.partitionLock.writeLock().tryLock();
                if (!acquired) {
                    takeoverBlocked.countDown();
                    facts.partitionLock.writeLock().lock();
                }
                try {
                    facts.liveLeaderEpoch.set(6);
                } finally {
                    facts.partitionLock.writeLock().unlock();
                    takeoverFinished.countDown();
                }
            });
            assertTrue(takeoverAttempted.await(5, TimeUnit.SECONDS));
            assertTrue(takeoverBlocked.await(5, TimeUnit.SECONDS));
            assertEquals(1, takeoverFinished.getCount());

            releaseCallback.countDown();
            ownerOpen.get(5, TimeUnit.SECONDS);
            takeover.get(5, TimeUnit.SECONDS);
            assertTrue(takeoverFinished.await(5, TimeUnit.SECONDS));

            AtomicInteger lateCallbacks = new AtomicInteger();
            assertThrows(FencedLeaderEpochException.class, () ->
                    facts.bridge.executeWhileCurrentOwner(
                            facts.fence(9, 5),
                            () -> countedSession(lateCallbacks)));
            assertEquals(0, lateCallbacks.get());
        } finally {
            executor.shutdownNow();
        }
    }

    private static WalRunObjectSession countedSession(AtomicInteger callbacks) {
        callbacks.incrementAndGet();
        return mock(WalRunObjectSession.class);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for deterministic test latch");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for deterministic test latch", failure);
        }
    }

    private static final class Facts {
        private static final Uuid TOPIC_ID = new Uuid(2, 3);
        private static final TopicPartition TOPIC_PARTITION = new TopicPartition("events", 0);

        private final TopicBindingId bindingId = new TopicBindingId(digest(1));
        private final StorageEpochId storageEpochId = new StorageEpochId(digest(2));
        private final AtomicInteger liveLeaderEpoch;
        private final AtomicLong processBrokerEpoch;
        private final AtomicLong metadataOffset;
        private final ReentrantReadWriteLock partitionLock = new ReentrantReadWriteLock();
        private final Partition partition = mock(Partition.class);
        private final ReplicaManager replicaManager = mock(ReplicaManager.class);
        private final KRaftMetadataCache metadataCache = mock(KRaftMetadataCache.class);
        private final BrokerRegistration broker = mock(BrokerRegistration.class);
        private final TopicsImage topics = mock(TopicsImage.class);
        private final MetadataImage image = mock(MetadataImage.class);
        private final KafkaPartitionLeaderOpenRequest open;
        private final NereusKafkaNativePartitionOwnerAuthorityBridgeV1 bridge;

        private Facts(int leaderEpoch, long brokerEpoch, long metadataOffset) {
            this.liveLeaderEpoch = new AtomicInteger(leaderEpoch);
            this.processBrokerEpoch = new AtomicLong(brokerEpoch);
            this.metadataOffset = new AtomicLong(metadataOffset);
            KafkaPartitionIdentity identity = new KafkaPartitionIdentity(
                    "kraft-cluster", TOPIC_ID.toString(), 0, "events");
            this.open = new KafkaPartitionLeaderOpenRequest(
                    identity,
                    1,
                    leaderEpoch,
                    brokerEpoch,
                    StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT,
                    metadataOffset,
                    Duration.ofSeconds(5));

            when(partition.isLeader()).thenAnswer(ignored -> liveLeaderEpoch.get() == leaderEpoch);
            when(partition.getLeaderEpoch()).thenAnswer(ignored -> liveLeaderEpoch.get());
            when(partition.topicId()).thenReturn(Option.apply(TOPIC_ID));
            when(replicaManager.onlinePartition(TOPIC_PARTITION)).thenReturn(Option.apply(partition));
            when(metadataCache.currentImage()).thenReturn(image);
            when(image.provenance()).thenAnswer(ignored -> new org.apache.kafka.image.MetadataProvenance(
                    this.metadataOffset.get(), 1, 1_000, true));
            when(image.cluster()).thenReturn(new ClusterImage(Map.of(1, broker), Map.of()));
            when(broker.epoch()).thenReturn(brokerEpoch);
            when(broker.fenced()).thenReturn(false);
            when(image.topics()).thenReturn(topics);
            when(topics.getTopic(TOPIC_ID)).thenAnswer(ignored -> new TopicImage(
                    "events",
                    TOPIC_ID,
                    Map.of(0, registration(leaderEpoch))));

            PartitionLeaderAuthority authority = new PartitionLeaderAuthority() {
                @Override
                public <T> T capture(int expectedLeaderEpoch, Supplier<T> action) {
                    partitionLock.readLock().lock();
                    try {
                        if (liveLeaderEpoch.get() != expectedLeaderEpoch) {
                            throw new FencedLeaderEpochException("test leader advanced");
                        }
                        return action.get();
                    } finally {
                        partitionLock.readLock().unlock();
                    }
                }

                @Override
                public void publish(int expectedLeaderEpoch, Runnable action) {
                    capture(expectedLeaderEpoch, () -> {
                        action.run();
                        return null;
                    });
                }
            };
            this.bridge = new NereusKafkaNativePartitionOwnerAuthorityBridgeV1(
                    open,
                    partition,
                    replicaManager,
                    metadataCache,
                    this.processBrokerEpoch::get,
                    authority);
        }

        private KafkaPartitionFenceV1 fence(long ownerEpoch, int leaderEpoch) {
            return new KafkaPartitionFenceV1(
                    bindingId,
                    new KafkaTopicIncarnationIdentity(
                            new KafkaTopicId(new Id128(
                                    TOPIC_ID.getMostSignificantBits(),
                                    TOPIC_ID.getLeastSignificantBits())),
                            new KafkaTopicName("events")),
                    0,
                    1,
                    storageEpochId,
                    ownerEpoch,
                    leaderEpoch);
        }

        private static PartitionRegistration registration(int leaderEpoch) {
            return new PartitionRegistration.Builder()
                    .setReplicas(new int[] {1})
                    .setDirectories(new Uuid[] {DirectoryId.UNASSIGNED})
                    .setIsr(new int[] {1})
                    .setLeader(1)
                    .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
                    .setLeaderEpoch(leaderEpoch)
                    .setPartitionEpoch(1)
                    .build();
        }

        private static Sha256Digest digest(int marker) {
            byte[] bytes = new byte[Sha256Digest.LENGTH];
            Arrays.fill(bytes, (byte) marker);
            return Sha256Digest.copyOf(bytes);
        }
    }
}
