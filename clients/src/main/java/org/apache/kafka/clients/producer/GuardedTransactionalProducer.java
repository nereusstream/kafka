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

/**
 * Guarded producer extension for records that are part of the current Kafka transaction.
 *
 * <p>The caller must initialize and begin the transaction through the normal {@link Producer} API. This method
 * does not start, commit, or abort a transaction; it only binds the exact resource guard to a transactional produce
 * request and returns the same authenticated response evidence as {@link GuardedProducer#sendGuarded}.</p>
 */
public interface GuardedTransactionalProducer<K, V> extends GuardedProducer<K, V> {
    Future<GuardedRecordMetadata> sendGuardedInTransaction(ProducerRecord<K, V> record,
                                                           ProducerResourceGuard guard);

    Future<GuardedRecordMetadata> sendGuardedInTransaction(ProducerRecord<K, V> record,
                                                           ProducerResourceGuard guard,
                                                           GuardedCallback callback);
}
