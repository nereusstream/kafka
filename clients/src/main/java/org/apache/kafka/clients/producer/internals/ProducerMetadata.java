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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.producer.ProducerResourceGuard;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;

public class ProducerMetadata extends Metadata {
    // If a topic hasn't been accessed for this many milliseconds, it is removed from the cache.
    private final long metadataIdleMs;

    /* Topics with expiry time */
    private final Map<String, Long> topics = new HashMap<>();
    private final Set<String> newTopics = new HashSet<>();
    private final Logger log;
    private final Time time;
    private Map<String, Errors> errors = null;
    // One current extension call, without an unbounded waiter list or historical proof cache.
    private GuardedUpdate guardedUpdate;
    private long guardedUpdateId;

    private static final class GuardedUpdate {
        final long id;
        final ProducerResourceGuard guard;
        boolean sent;
        int requestVersion;
        Cluster response;
        KafkaException failure;

        GuardedUpdate(long id, ProducerResourceGuard guard) {
            this.id = id;
            this.guard = guard;
        }
    }

    public ProducerMetadata(long refreshBackoffMs,
                            long refreshBackoffMaxMs,
                            long metadataExpireMs,
                            long metadataIdleMs,
                            LogContext logContext,
                            ClusterResourceListeners clusterResourceListeners,
                            Time time) {
        super(refreshBackoffMs, refreshBackoffMaxMs, metadataExpireMs, logContext, clusterResourceListeners);
        this.metadataIdleMs = metadataIdleMs;
        this.log = logContext.logger(ProducerMetadata.class);
        this.time = time;
    }

    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilder() {
        return new MetadataRequest.Builder(new ArrayList<>(topics.keySet()), true);
    }

    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilderForNewTopics() {
        return new MetadataRequest.Builder(new ArrayList<>(newTopics), true);
    }

    @Override
    public synchronized MetadataRequestAndVersion newMetadataRequestAndVersion(long nowMs) {
        MetadataRequestAndVersion ordinary = super.newMetadataRequestAndVersion(nowMs);
        if (guardedUpdate == null || guardedUpdate.response != null || guardedUpdate.failure != null)
            return ordinary;
        guardedUpdate.sent = true;
        guardedUpdate.requestVersion = ordinary.requestVersion;
        // Only this topic; no new tracked topic and no auto-creation side effect after cancellation.
        return metadataRequestAndVersion(new MetadataRequest.Builder(
                Collections.singletonList(guardedUpdate.guard.canonicalTopic()), false),
                ordinary.requestVersion, true);
    }

    public synchronized long requestGuardedUpdate(ProducerResourceGuard guard) {
        Objects.requireNonNull(guard, "resource guard");
        if (isClosed())
            throw new KafkaException("Requested guarded metadata after close");
        if (guardedUpdate != null)
            throw new KafkaException("Another guarded metadata observation is already pending");
        guardedUpdate = new GuardedUpdate(++guardedUpdateId, guard);
        // Bump the request version so a previously in-flight response cannot satisfy this observation.
        requestUpdateForNewTopics();
        return guardedUpdate.id;
    }

    public synchronized Cluster awaitGuardedUpdate(long id, long timeoutMs) throws InterruptedException {
        final long nowMs = time.milliseconds();
        final long deadlineMs = nowMs + timeoutMs < 0 ? Long.MAX_VALUE : nowMs + timeoutMs;
        time.waitObject(this, () -> {
            maybeThrowFatalException();
            return isClosed() || guardedUpdate == null || guardedUpdate.id != id
                    || guardedUpdate.response != null || guardedUpdate.failure != null;
        }, deadlineMs);
        if (isClosed() || guardedUpdate == null || guardedUpdate.id != id)
            throw new KafkaException("Guarded metadata observation closed or cancelled");
        if (guardedUpdate.failure != null)
            throw guardedUpdate.failure;
        return guardedUpdate.response;
    }

    public synchronized void cancelGuardedUpdate(long id) {
        if (guardedUpdate != null && guardedUpdate.id == id) {
            guardedUpdate = null;
            notifyAll();
        }
    }

    public synchronized void add(String topic, long nowMs) {
        Objects.requireNonNull(topic, "topic cannot be null");
        if (topics.put(topic, nowMs + metadataIdleMs) == null) {
            newTopics.add(topic);
            requestUpdateForNewTopics();
        }
    }

    /**
     * Atomically add a batch of topics to the working set, refreshing the
     * expiry for those already present. If any of the topics was newly added,
     * a partial metadata refresh is requested and the current updateVersion is
     * returned so the caller can pass it to {@link #awaitUpdate(int, long)} to
     * wait for the next response. Returns an empty {@code OptionalInt} when no
     * topic was newly added (no refresh requested, nothing to wait for).
     */
    public synchronized OptionalInt add(Collection<String> topics, long nowMs) {
        boolean anyNew = false;
        for (String topic : topics) {
            if (this.topics.put(topic, nowMs + metadataIdleMs) == null) {
                newTopics.add(topic);
                anyNew = true;
            }
        }
        return anyNew ? OptionalInt.of(requestUpdateForNewTopics()) : OptionalInt.empty();
    }

    public synchronized int requestUpdateForTopic(String topic) {
        if (newTopics.contains(topic)) {
            return requestUpdateForNewTopics();
        } else {
            return requestUpdate(false);
        }
    }

    // Visible for testing
    synchronized Set<String> topics() {
        return topics.keySet();
    }

    // Visible for testing
    synchronized Set<String> newTopics() {
        return newTopics;
    }

    public synchronized boolean containsTopic(String topic) {
        return topics.containsKey(topic);
    }

    @Override
    public synchronized boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        if (guardedUpdate != null && guardedUpdate.guard.canonicalTopic().equals(topic))
            return true;
        Long expireMs = topics.get(topic);
        if (expireMs == null) {
            return false;
        } else if (newTopics.contains(topic)) {
            return true;
        } else if (expireMs <= nowMs) {
            log.debug("Removing unused topic {} from the metadata list, expiryMs {} now {}", topic, expireMs, nowMs);
            topics.remove(topic);
            return false;
        } else {
            return true;
        }
    }

    /**
     * Wait for metadata update until the current version is larger than the last version we know of
     */
    public synchronized void awaitUpdate(final int lastVersion, final long timeoutMs) throws InterruptedException {
        long currentTimeMs = time.milliseconds();
        long deadlineMs = currentTimeMs + timeoutMs < 0 ? Long.MAX_VALUE : currentTimeMs + timeoutMs;
        time.waitObject(this, () -> {
            // Throw fatal exceptions, if there are any. Recoverable topic errors will be handled by the caller.
            maybeThrowFatalException();
            return updateVersion() > lastVersion || isClosed();
        }, deadlineMs);

        if (isClosed())
            throw new KafkaException("Requested metadata update after close");
    }

    @Override
    public synchronized void update(int requestVersion, MetadataResponse response, boolean isPartialUpdate, long nowMs) {
        if (guardedUpdate != null && guardedUpdate.sent && guardedUpdate.requestVersion == requestVersion
                && guardedUpdate.response == null && guardedUpdate.failure == null) {
            try {
                if (response.topLevelError() != Errors.NONE)
                    throw response.topLevelError().exception();
                if (response.topicMetadata().size() != 1)
                    throw new KafkaException("Guarded metadata response did not cover exactly its requested topic");
                MetadataResponse.TopicMetadata topic = response.topicMetadata().iterator().next();
                if (!guardedUpdate.guard.canonicalTopic().equals(topic.topic()))
                    throw new KafkaException("Guarded metadata response covered a different topic");
                if (topic.error() != Errors.NONE)
                    throw topic.error().exception();
                MetadataResponse.PartitionMetadata partition = topic.partitionMetadata().stream()
                        .filter(p -> p.partition() == guardedUpdate.guard.partition()).findFirst()
                        .orElseThrow(() -> new KafkaException("Guarded metadata response omitted its partition"));
                if (partition.error != Errors.NONE)
                    throw partition.error.exception();
                if (partition.leaderId.isEmpty())
                    throw new KafkaException("Guarded metadata response omitted its partition leader");
                guardedUpdate.response = response.buildCluster();
            } catch (KafkaException failure) {
                guardedUpdate.failure = failure;
            }
        }
        super.update(requestVersion, response, isPartialUpdate, nowMs);
        errors = response.errors();

        // Remove all topics in the response that are in the new topic set. Note that if an error was encountered for a
        // new topic's metadata, then any work to resolve the error will include the topic in a full metadata update.
        if (!newTopics.isEmpty()) {
            for (MetadataResponse.TopicMetadata metadata : response.topicMetadata()) {
                newTopics.remove(metadata.topic());
            }
        }

        notifyAll();
    }

    public Errors getError(final String topic) {
        if (errors != null) {
            return errors.get(topic);
        }
        return null;
    }

    @Override
    public synchronized void fatalError(KafkaException fatalException) {
        super.fatalError(fatalException);
        notifyAll();
    }

    /**
     * Close this instance and notify any awaiting threads.
     */
    @Override
    public synchronized void close() {
        super.close();
        notifyAll();
    }

}
