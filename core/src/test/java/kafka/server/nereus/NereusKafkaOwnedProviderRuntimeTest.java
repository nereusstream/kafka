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

import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.server.config.NereusKafkaConfigs;
import org.apache.kafka.server.config.NereusKafkaStorageConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.log.nereus.NereusUnifiedLog;
import kafka.server.KafkaConfig;
import kafka.server.nsip.KafkaBookKeeperOwnerRelocationRealTest;
import kafka.server.storage.BrokerStorageRuntimeContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NereusKafkaOwnedProviderRuntimeTest {
    @Test
    public void existingExactOpenCannotBypassFailedQuotaReservation() {
        var quotaFailure = new IllegalStateException("unaccounted authority cannot be imported into a zero quota grant");
        var openAndCreateAttempted = new AtomicBoolean();
        var result = NereusKafkaOwnedProviderRuntime.afterGrantedQuota(
                CompletableFuture.<Void>failedFuture(quotaFailure), () -> {
                    openAndCreateAttempted.set(true);
                    return CompletableFuture.completedFuture("exact OPEN then native create");
                });

        assertSame(quotaFailure, assertThrows(CompletionException.class,
                () -> result.toCompletableFuture().join()).getCause());
        assertFalse(openAndCreateAttempted.get());
    }

    @Test
    public void m5InstallationAllowsSealVerificationWhileAppendOwnsLogGuard(@TempDir Path directory) throws Exception {
        try (var fixture = new Fixture(directory)) {
            var actors = Executors.newFixedThreadPool(3);
            var logGuard = new Object();
            var appendEntered = new CompletableFuture<Void>();
            var installationEntered = new CompletableFuture<Void>();
            var sealVerified = new CompletableFuture<Void>();
            try {
                var append = CompletableFuture.runAsync(() -> {
                    synchronized (logGuard) {
                        appendEntered.complete(null);
                        sealVerified.join();
                    }
                }, actors);
                appendEntered.get(2, TimeUnit.SECONDS);
                var installation = CompletableFuture.runAsync(() -> fixture.opened.attachM5Log(fixture.log, () -> {
                    installationEntered.complete(null);
                    synchronized (logGuard) {
                        // The real log installation needs the guard still held by append during rollover.
                    }
                    return null;
                }), actors);
                installationEntered.get(2, TimeUnit.SECONDS);
                var verification = CompletableFuture.runAsync(() -> {
                    synchronized (fixture.opened) {
                        // Native reader admission needs this monitor before sealRoot can complete.
                        sealVerified.complete(null);
                    }
                }, actors);
                CompletableFuture.allOf(append, installation, verification).get(2, TimeUnit.SECONDS);
                assertThrows(IllegalStateException.class,
                        () -> fixture.opened.attachM5Log(fixture.log, () -> null));
            } finally {
                sealVerified.complete(null);
                actors.shutdown();
                assertTrue(actors.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void closingWaitsForActualInstallationTermination(@TempDir Path directory) throws Exception {
        try (var fixture = new Fixture(directory)) {
            var actors = Executors.newFixedThreadPool(2);
            var entered = new CompletableFuture<Void>();
            var finishInstallation = new CompletableFuture<Void>();
            var failure = new IllegalStateException("installation failed after enrollment");
            try {
                var installation = CompletableFuture.runAsync(() -> fixture.opened.attachM5Log(fixture.log, () -> {
                    entered.complete(null);
                    finishInstallation.join();
                    throw failure;
                }), actors);
                entered.get(2, TimeUnit.SECONDS);
                assertEquals("native M5 log is not ready",
                        assertThrows(IllegalStateException.class, fixture.opened::nativeM5InputCut).getMessage());
                var closing = CompletableFuture.supplyAsync(fixture.opened::closeAsync, actors)
                        .get(2, TimeUnit.SECONDS);
                assertFalse(closing.isDone());
                assertFalse(fixture.resourcesClosed.get());
                finishInstallation.complete(null);
                assertSame(failure, assertThrows(CompletionException.class, installation::join).getCause());
                closing.get(2, TimeUnit.SECONDS);
                assertTrue(fixture.resourcesClosed.get());
                var attemptedAfterClose = new AtomicBoolean();
                fixture.opened.attachM5Log(fixture.log, () -> {
                    attemptedAfterClose.set(true);
                    return null;
                });
                assertFalse(attemptedAfterClose.get());
            } finally {
                finishInstallation.complete(null);
                actors.shutdown();
                assertTrue(actors.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void failedInstallationCannotBeRestarted(@TempDir Path directory) throws Exception {
        try (var fixture = new Fixture(directory)) {
            var failure = new IllegalStateException("installation failed after enrollment");
            assertSame(failure, assertThrows(IllegalStateException.class,
                    () -> fixture.opened.attachM5Log(fixture.log, () -> {
                        throw failure;
                    })));
            assertEquals("native M5 log is not ready",
                    assertThrows(IllegalStateException.class, fixture.opened::nativeM5InputCut).getMessage());
            var restarted = new AtomicBoolean();
            assertThrows(IllegalStateException.class,
                    () -> fixture.opened.attachM5Log(fixture.log, () -> {
                        restarted.set(true);
                        return null;
                    }));
            assertFalse(restarted.get());
            fixture.opened.closeAsync().get(2, TimeUnit.SECONDS);
            assertTrue(fixture.resourcesClosed.get());
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final NereusKafkaOwnedProviderRuntime runtime;
        private final NereusKafkaOwnedProviderRuntime.Opened opened;
        private final NereusUnifiedLog log = mock(NereusUnifiedLog.class);
        private final AtomicBoolean resourcesClosed = new AtomicBoolean();

        private Fixture(Path directory) throws Exception {
            KafkaBookKeeperOwnerRelocationRealTest.writeCapability(directory, Optional.empty());
            var properties = new HashMap<>(KafkaBookKeeperOwnerRelocationRealTest.nativeConfig(
                    directory, "zk://127.0.0.1:2181/ledgers", "127.0.0.1:6648"));
            var snapshot = NereusKafkaStorageConfig.from(new AbstractConfig(NereusKafkaConfigs.CONFIG_DEF, properties));
            var config = mock(KafkaConfig.class);
            when(config.nereusKafkaStorageConfig()).thenReturn(snapshot);
            var context = mock(BrokerStorageRuntimeContext.class);
            when(context.config()).thenReturn(config);
            runtime = new NereusKafkaOwnedProviderRuntime(context);
            var source = mock(M5BookKeeperNativeCreateClientV2.class);
            when(source.fenceCreates()).thenReturn(CompletableFuture.completedFuture(null));
            var session = mock(BookKeeperCellSession.class);
            when(session.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
            doAnswer(ignored -> {
                resourcesClosed.set(true);
                return null;
            }).when(source).close();
            opened = runtime.new Opened(source, session,
                    Sha256Digest.hash(CanonicalUtf8.fromString("binding").bytes()), new Semaphore(1));
        }

        @Override
        public void close() throws Exception {
            try {
                opened.close();
            } finally {
                runtime.close();
            }
        }
    }
}
