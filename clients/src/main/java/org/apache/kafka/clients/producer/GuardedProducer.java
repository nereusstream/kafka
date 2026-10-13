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
package org.apache.kafka.clients.producer;

import java.util.concurrent.Future;

/** Producer API whose success result carries authenticated resource evidence. */
public interface GuardedProducer<K, V> extends Producer<K, V> {
    /** Local same-object cleanup; business closure alone does not prove native termination. */
    default void closeGuardedResources(java.time.Duration timeout) {
        throw new UnsupportedOperationException("guarded producer native cleanup is unavailable");
    }

    default boolean guardedResourcesTerminated() { return false; }

    /** Original guarded-close failure is retained after later successful cleanup. */
    default Throwable guardedFirstCloseFailure() { return null; }
    /**
     * Requires a fresh response from this producer's Metadata path for the exact resource.
     * The query prohibits automatic topic creation and never submits a record or changes a transaction.
     * Implementations without this extension fail explicitly instead of accepting cached identity.
     */
    default void requireResourceGuard(ProducerResourceGuard guard) {
        throw new UnsupportedOperationException("fresh guarded metadata observation is unavailable");
    }

    Future<GuardedRecordMetadata> sendGuarded(ProducerRecord<K, V> record, ProducerResourceGuard guard);

    Future<GuardedRecordMetadata> sendGuarded(ProducerRecord<K, V> record, ProducerResourceGuard guard,
                                              GuardedCallback callback);
}
