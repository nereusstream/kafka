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
import kafka.log.nereus.NereusUnifiedLog;
import kafka.server.ReplicaManager;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.FencedLeaderEpochException;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.KRaftMetadataCache;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.storage.internals.log.PartitionLeaderAuthority;

import com.nereusstream.domain.identity.Id128;
import com.nereusstream.kafka.bookkeeper.object.recovery.KafkaNativePartitionOwnerAuthorityV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.partition.KafkaLeaderAuthority;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.partition.KafkaPartitionLeaderOpenRequest;
import com.nereusstream.storage.object.control.WalRunObjectSession;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/**
 * Executes one Object-WAL owner-open callback under the exact live Kafka partition-leader lock.
 *
 * <p>This bridge carries no reusable authority proof. Every execution reacquires the stock
 * {@link PartitionLeaderAuthority}, then revalidates the process-current KRaft image, broker registration epoch,
 * {@link ReplicaManager} partition object, and exact Nereus fence before the callback can perform Provider I/O.
 * A leader transition needs the corresponding partition write lock and therefore cannot supersede the owner during
 * the callback.
 */
public final class NereusKafkaNativePartitionOwnerAuthorityBridgeV1
        implements KafkaNativePartitionOwnerAuthorityV1 {
    private final KafkaPartitionLeaderOpenRequest expectedOpen;
    private final Partition expectedPartition;
    private final ReplicaManager replicaManager;
    private final KRaftMetadataCache metadataCache;
    private final LongSupplier brokerEpochSupplier;
    private final PartitionLeaderAuthority partitionAuthority;
    private final AtomicBoolean executing = new AtomicBoolean();

    /** Creates the production bridge from the exact stock leader log and partition authority. */
    public static NereusKafkaNativePartitionOwnerAuthorityBridgeV1 forCurrentLeader(
            KafkaPartitionLeaderOpenRequest expectedOpen,
            Partition expectedPartition,
            ReplicaManager replicaManager,
            KRaftMetadataCache metadataCache,
            LongSupplier brokerEpochSupplier
    ) {
        Objects.requireNonNull(expectedPartition, "expectedPartition");
        if (!(expectedPartition.localLogOrException() instanceof NereusUnifiedLog expectedLog)) {
            throw fenced("Kafka native Object-WAL owner does not own a Nereus UnifiedLog");
        }
        return new NereusKafkaNativePartitionOwnerAuthorityBridgeV1(
                expectedOpen,
                expectedPartition,
                replicaManager,
                metadataCache,
                brokerEpochSupplier,
                expectedPartition.nereusMaintenanceAuthority(expectedLog));
    }

    NereusKafkaNativePartitionOwnerAuthorityBridgeV1(
            KafkaPartitionLeaderOpenRequest expectedOpen,
            Partition expectedPartition,
            ReplicaManager replicaManager,
            KRaftMetadataCache metadataCache,
            LongSupplier brokerEpochSupplier,
            PartitionLeaderAuthority partitionAuthority
    ) {
        this.expectedOpen = Objects.requireNonNull(expectedOpen, "expectedOpen");
        this.expectedPartition = Objects.requireNonNull(expectedPartition, "expectedPartition");
        this.replicaManager = Objects.requireNonNull(replicaManager, "replicaManager");
        this.metadataCache = Objects.requireNonNull(metadataCache, "metadataCache");
        this.brokerEpochSupplier = Objects.requireNonNull(brokerEpochSupplier, "brokerEpochSupplier");
        this.partitionAuthority = Objects.requireNonNull(partitionAuthority, "partitionAuthority");
    }

    @Override
    public WalRunObjectSession executeWhileCurrentOwner(
            KafkaPartitionFenceV1 exactCurrentFence,
            SynchronousOwnerCallback callback
    ) throws IOException {
        Objects.requireNonNull(exactCurrentFence, "exactCurrentFence");
        Objects.requireNonNull(callback, "callback");
        if (!executing.compareAndSet(false, true)) {
            throw new IllegalStateException("Kafka native Object-WAL owner callback is already executing");
        }
        Thread callingThread = Thread.currentThread();
        try {
            try {
                return partitionAuthority.capture(expectedOpen.leaderEpoch(), () -> {
                    if (Thread.currentThread() != callingThread) {
                        throw new IllegalStateException(
                                "Kafka partition authority changed thread before native owner callback");
                    }
                    requireCurrent(exactCurrentFence);
                    WalRunObjectSession result;
                    try {
                        result = Objects.requireNonNull(
                                callback.execute(), "Kafka native owner callback result");
                    } catch (IOException failure) {
                        throw new OwnerCallbackIOException(failure);
                    }
                    if (Thread.currentThread() != callingThread) {
                        throw new IllegalStateException(
                                "Kafka native owner callback changed thread");
                    }
                    requireCurrent(exactCurrentFence);
                    return result;
                });
            } catch (OwnerCallbackIOException failure) {
                throw failure.ioCause();
            }
        } finally {
            executing.set(false);
        }
    }

    private static final class OwnerCallbackIOException extends RuntimeException {
        private OwnerCallbackIOException(IOException cause) {
            super(cause);
        }

        private IOException ioCause() {
            return (IOException) getCause();
        }
    }

    private void requireCurrent(KafkaPartitionFenceV1 fence) {
        KafkaLeaderAuthority expectedAuthority = expectedOpen.authority();
        KafkaPartitionIdentity identity = expectedAuthority.identity();
        TopicPartition topicPartition = new TopicPartition(
                identity.observedTopicName(), identity.partition());
        Uuid topicId = requireTopicId(identity);

        requireExactFence(fence, expectedAuthority, topicId);
        requireProcessAuthority(expectedAuthority);
        requireMetadataAuthority(expectedAuthority, identity, topicId);
        requirePartitionAuthority(expectedAuthority, topicPartition, topicId);
    }

    private static Uuid requireTopicId(KafkaPartitionIdentity identity) {
        Uuid topicId;
        try {
            topicId = Uuid.fromString(identity.topicId());
        } catch (RuntimeException failure) {
            throw fenced("Kafka native Object-WAL owner has an invalid topic ID");
        }
        return topicId;
    }

    private void requireProcessAuthority(KafkaLeaderAuthority expectedAuthority) {
        long processBrokerEpoch = brokerEpochSupplier.getAsLong();
        if (processBrokerEpoch != expectedAuthority.brokerEpoch()) {
            throw fenced("Kafka broker registration epoch superseded the Object-WAL owner");
        }
    }

    private void requireMetadataAuthority(
            KafkaLeaderAuthority expectedAuthority,
            KafkaPartitionIdentity identity,
            Uuid topicId
    ) {
        MetadataImage image = metadataCache.currentImage();
        if (image.provenance().lastContainedOffset() < expectedOpen.metadataOffset()) {
            throw fenced("KRaft metadata image regressed behind the Object-WAL owner-open offset");
        }
        BrokerRegistration broker = image.cluster().broker(expectedAuthority.leaderId());
        if (broker == null
                || broker.fenced()
                || broker.epoch() != expectedAuthority.brokerEpoch()) {
            throw fenced("KRaft broker registration no longer matches the Object-WAL owner");
        }
        TopicImage topic = image.topics().getTopic(topicId);
        PartitionRegistration registration = topic == null
                ? null : topic.partitions().get(identity.partition());
        if (topic == null
                || !topic.name().equals(identity.observedTopicName())
                || registration == null
                || registration.leader != expectedAuthority.leaderId()
                || registration.leaderEpoch != expectedAuthority.leaderEpoch()) {
            throw fenced("KRaft partition registration no longer matches the Object-WAL owner");
        }
    }

    private void requirePartitionAuthority(
            KafkaLeaderAuthority expectedAuthority,
            TopicPartition topicPartition,
            Uuid topicId
    ) {
        scala.Option<Partition> current = replicaManager.onlinePartition(topicPartition);
        if (current.isEmpty()
                || current.get() != expectedPartition
                || !expectedPartition.isLeader()
                || expectedPartition.getLeaderEpoch() != expectedAuthority.leaderEpoch()
                || !expectedPartition.topicId().contains(topicId)) {
            throw fenced("ReplicaManager no longer owns the exact Object-WAL leader partition");
        }
    }

    private static void requireExactFence(
            KafkaPartitionFenceV1 fence,
            KafkaLeaderAuthority authority,
            Uuid topicId
    ) {
        Id128 exactTopicId = fence.topicIncarnation().topicId().value();
        if (fence.partitionId() != authority.identity().partition()
                || !fence.topicIncarnation().topicName().value()
                        .equals(authority.identity().observedTopicName())
                || exactTopicId.highBits() != topicId.getMostSignificantBits()
                || exactTopicId.lowBits() != topicId.getLeastSignificantBits()
                || fence.ownerEpoch() != authority.brokerEpoch()
                || fence.kafkaLeaderEpoch() != authority.leaderEpoch()) {
            throw fenced("Kafka Object-WAL fence is not the exact process-current native owner");
        }
    }

    private static FencedLeaderEpochException fenced(String message) {
        return new FencedLeaderEpochException(message);
    }
}
