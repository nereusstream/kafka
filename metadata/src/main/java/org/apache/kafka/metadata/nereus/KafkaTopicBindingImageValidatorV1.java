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
package org.apache.kafka.metadata.nereus;

import org.apache.kafka.image.FeaturesImage;
import org.apache.kafka.image.TopicDelta;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsDelta;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.server.common.NereusStorageVersion;

/** Publication-boundary validator for the feature-2 topic/aggregate exact-one invariant. */
public final class KafkaTopicBindingImageValidatorV1 {
    private KafkaTopicBindingImageValidatorV1() {
    }

    public static void validatePublication(
        FeaturesImage features,
        TopicsImage topics,
        TopicsDelta delta,
        boolean fullScan
    ) {
        Short level = features.finalizedVersions().get(NereusStorageVersion.FEATURE_NAME);
        if (level != null && level != NereusStorageVersion.NSV_2.featureLevel()) {
            throw new IllegalStateException("Unsupported persisted " + NereusStorageVersion.FEATURE_NAME +
                " level " + level + "; this source tuple accepts only level 2");
        }
        if (fullScan) {
            for (TopicImage topic : topics.topicsById().values()) {
                validateTopic(level != null, topic);
            }
        } else if (delta != null) {
            for (TopicDelta changedTopic : delta.changedTopics().values()) {
                TopicImage topic = topics.getTopic(changedTopic.id());
                if (topic != null) {
                    validateTopic(level != null, topic);
                }
            }
        }
    }

    private static void validateTopic(boolean enabled, TopicImage topic) {
        if (enabled) {
            KafkaTopicBindingAggregateV1 aggregate = topic.nereusAggregate().orElseThrow(() ->
                new IllegalStateException("Feature-2 topic " + topic.name() + " with ID " + topic.id() +
                    " has no TopicBindingAggregateRecord"));
            KafkaTopicBindingAggregateMapperV1.validateBackReference(aggregate, topic.id(), topic.name());
        } else if (topic.nereusAggregate().isPresent()) {
            throw new IllegalStateException("Feature-disabled topic " + topic.name() + " with ID " + topic.id() +
                " contains a TopicBindingAggregateRecord");
        }
    }
}
