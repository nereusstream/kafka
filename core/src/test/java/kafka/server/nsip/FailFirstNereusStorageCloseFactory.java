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
package kafka.server.nsip;

import kafka.log.UnifiedLogFactory;
import kafka.server.ReplicaManager;
import kafka.server.metadata.AsyncTopicDeltaLifecycle;
import kafka.server.nereus.NereusBrokerStorageRuntimeFactory;
import kafka.server.storage.BrokerStorageAppendExecutor;
import kafka.server.storage.BrokerStorageDrainReason;
import kafka.server.storage.BrokerStorageFetchExecutor;
import kafka.server.storage.BrokerStorageRuntime;
import kafka.server.storage.BrokerStorageRuntimeContext;
import kafka.server.storage.BrokerStorageRuntimeFactory;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import scala.Option;

/** Injects one close failure before the real native runtime closes, then delegates the Broker retry. */
final class FailFirstNereusStorageCloseFactory implements BrokerStorageRuntimeFactory {
    private final NereusBrokerStorageRuntimeFactory nativeFactory = new NereusBrokerStorageRuntimeFactory();
    private final AtomicInteger attempts = new AtomicInteger();
    private final AtomicInteger completedNativeCloses = new AtomicInteger();
    private final int brokerId;

    FailFirstNereusStorageCloseFactory(int brokerId) {
        this.brokerId = brokerId;
    }

    int attempts() {
        return attempts.get();
    }

    int completedNativeCloses() {
        return completedNativeCloses.get();
    }

    @Override
    public BrokerStorageRuntime create(BrokerStorageRuntimeContext context) {
        BrokerStorageRuntime nativeRuntime = nativeFactory.create(context);
        if (context.config().brokerId() != brokerId) return nativeRuntime;
        return new BrokerStorageRuntime() {
            @Override
            public CompletionStage<Void> start() {
                return nativeRuntime.start();
            }

            @Override
            public UnifiedLogFactory unifiedLogFactory() {
                return nativeRuntime.unifiedLogFactory();
            }

            @Override
            public Option<BrokerStorageAppendExecutor> appendExecutor() {
                return nativeRuntime.appendExecutor();
            }

            @Override
            public Option<BrokerStorageFetchExecutor> fetchExecutor() {
                return nativeRuntime.fetchExecutor();
            }

            @Override
            public Option<AsyncTopicDeltaLifecycle> asyncTopicDeltaLifecycle(ReplicaManager replicaManager) {
                return nativeRuntime.asyncTopicDeltaLifecycle(replicaManager);
            }

            @Override
            public CompletionStage<Void> beginDrain(BrokerStorageDrainReason reason) {
                return nativeRuntime.beginDrain(reason);
            }

            @Override
            public CompletionStage<Void> awaitDrained(Duration timeout) {
                return nativeRuntime.awaitDrained(timeout);
            }

            @Override
            public void close() {
                if (attempts.incrementAndGet() == 1) {
                    throw new IllegalStateException("injected native storage close failure");
                }
                nativeRuntime.close();
                completedNativeCloses.incrementAndGet();
            }
        };
    }
}
