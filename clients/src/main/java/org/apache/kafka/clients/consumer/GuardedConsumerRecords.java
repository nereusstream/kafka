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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Consumer records whose returned Fetch has a typed guarded response proof. */
public final class GuardedConsumerRecords<K, V> extends ConsumerRecords<K, V> {
    private final GuardedFetchEvidence fetchEvidence;

    public GuardedConsumerRecords(final ConsumerRecords<K, V> records,
                                  final GuardedFetchEvidence fetchEvidence) {
        super(copyRecords(Objects.requireNonNull(records, "records")), records.nextOffsets());
        if (!records.isEmpty() && fetchEvidence == null) {
            throw new IllegalArgumentException("non-empty guarded records require Fetch evidence");
        }
        this.fetchEvidence = fetchEvidence;
    }

    public GuardedFetchEvidence fetchEvidence() {
        return fetchEvidence;
    }

    private static <K, V> Map<TopicPartition, List<ConsumerRecord<K, V>>> copyRecords(
            final ConsumerRecords<K, V> records) {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> copy = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            copy.put(partition, records.records(partition));
        }
        return copy;
    }
}
