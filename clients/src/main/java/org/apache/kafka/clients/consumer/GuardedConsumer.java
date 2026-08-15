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

import java.time.Duration;

/** Consumer API whose source records carry authenticated Fetch v13 evidence. */
public interface GuardedConsumer<K, V> extends Consumer<K, V> {
    /** Binds this consumer to one immutable topic identity and partition. */
    void bindResourceGuard(ConsumerResourceGuard guard);

    /** Returns the immutable guard bound to this consumer. */
    ConsumerResourceGuard resourceGuard();

    /** Polls and requires every non-empty returned batch to carry guarded Fetch evidence. */
    GuardedConsumerRecords<K, V> pollGuarded(Duration timeout);
}
