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
package org.apache.kafka.controller;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/** Commits a boundary-epoch fixture through the actual Controller event queue and KRaft log. */
public final class NereusNativeLeaderEpochTestBridge {
    private NereusNativeLeaderEpochTestBridge() {}

    public static CompletableFuture<Void> advance(QuorumController controller, Uuid topicId, int epoch) {
        return controller.appendWriteEvent("nsipBoundaryLeaderEpoch", OptionalLong.empty(), () -> {
            var current = controller.replicationControl().getPartition(topicId, 0);
            if (current == null || epoch <= current.leaderEpoch)
                throw new IllegalArgumentException("boundary fixture must advance an actual partition");
            var encoded = current.toRecord(
                    topicId, 0, new ImageWriterOptions.Builder(MetadataVersion.latestTesting()).build());
            var record = (PartitionRecord) encoded.message();
            record.setLeaderEpoch(epoch).setPartitionEpoch(current.partitionEpoch + 1);
            return ControllerResult.atomicOf(List.of(new ApiMessageAndVersion(record, encoded.version())), null);
        });
    }
}
