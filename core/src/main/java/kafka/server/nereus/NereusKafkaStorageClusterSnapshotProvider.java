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

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.activation.KafkaStorageClusterSnapshot;
import com.nereusstream.kafka.activation.KafkaStorageClusterSnapshotProvider;
import com.nereusstream.metadata.oxia.KafkaBrokerIdentity;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import kafka.server.KafkaRaftServer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.metadata.KRaftMetadataCache;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Captures one immutable KRaft image and a conservative local-log existence proof for activation.
 *
 * <p>Durable Nereus binding existence is added product-side from the shared Oxia binding store.
 */
public final class NereusKafkaStorageClusterSnapshotProvider
        implements KafkaStorageClusterSnapshotProvider {
    private final String kafkaClusterId;
    private final KRaftMetadataCache metadataCache;
    private final List<Path> logDirectories;

    public NereusKafkaStorageClusterSnapshotProvider(
            String kafkaClusterId,
            KRaftMetadataCache metadataCache,
            List<Path> logDirectories
    ) {
        this.kafkaClusterId = nonblank(kafkaClusterId, "kafkaClusterId");
        this.metadataCache = Objects.requireNonNull(metadataCache, "metadataCache");
        this.logDirectories = List.copyOf(Objects.requireNonNull(logDirectories, "logDirectories"));
        if (this.logDirectories.isEmpty()) {
            throw new IllegalArgumentException("logDirectories must be non-empty");
        }
    }

    @Override
    public CompletionStage<KafkaStorageClusterSnapshot> currentSnapshot() {
        try {
            MetadataImage image = metadataCache.currentImage();
            long metadataOffset = image.provenance().lastContainedOffset();
            if (metadataOffset < 0) {
                return CompletableFuture.failedFuture(unavailable(
                        "initial KRaft metadata image is not published", null));
            }
            List<KafkaBrokerIdentity> brokers = image.cluster().brokers().entrySet().stream()
                    .map(entry -> new KafkaBrokerIdentity(
                            entry.getKey(), entry.getValue().epoch()))
                    .sorted(Comparator.naturalOrder())
                    .toList();
            if (brokers.isEmpty()) {
                return CompletableFuture.failedFuture(unavailable(
                        "KRaft broker registration set is empty", null));
            }
            return CompletableFuture.completedFuture(new KafkaStorageClusterSnapshot(
                    kafkaClusterId,
                    metadataOffset,
                    KafkaStorageProtocolActivationRecord.KAFKA_FEATURE_LEVEL,
                    brokers,
                    !image.topics().isEmpty(),
                    authoritativeLocalLogsPresent(),
                    false));
        } catch (Throwable failure) {
            if (failure instanceof NereusException nereus) {
                return CompletableFuture.failedFuture(nereus);
            }
            return CompletableFuture.failedFuture(unavailable(
                    "cannot capture KRaft/local-log activation snapshot", failure));
        }
    }

    private boolean authoritativeLocalLogsPresent() throws IOException {
        for (Path logDirectory : logDirectories) {
            if (!Files.isDirectory(logDirectory)) {
                continue;
            }
            try (DirectoryStream<Path> children = Files.newDirectoryStream(logDirectory)) {
                for (Path child : children) {
                    if (!Files.isDirectory(child)) {
                        continue;
                    }
                    try {
                        TopicPartition topicPartition =
                                UnifiedLog.parseTopicPartitionName(child.toFile());
                        if (!topicPartition.topic().equals(KafkaRaftServer.MetadataTopic())) {
                            return true;
                        }
                    } catch (IOException | RuntimeException parseFailure) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static NereusException unavailable(String message, Throwable cause) {
        return cause == null
                ? new NereusException(ErrorCode.METADATA_UNAVAILABLE, true, message)
                : new NereusException(ErrorCode.METADATA_UNAVAILABLE, true, message, cause);
    }

    private static String nonblank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must be nonblank");
        }
        return value;
    }
}
