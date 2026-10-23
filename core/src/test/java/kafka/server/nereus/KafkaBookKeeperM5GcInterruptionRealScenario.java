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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureOutcome;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2;

import io.oxia.client.api.AsyncOxiaClient;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Leaves actual native absence under the original INTENT, then rebuilds the owned GC scope. */
final class KafkaBookKeeperM5GcInterruptionRealScenario {
    private volatile String blockedAuthority;
    private final AtomicInteger withheldDone = new AtomicInteger();

    AsyncOxiaClient client(String mode, AsyncOxiaClient actual) {
        if (!mode.endsWith("_GC_INTERRUPTED")) return actual;
        return (AsyncOxiaClient) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {AsyncOxiaClient.class},
                (proxy, method, args) -> {
                    boolean put = method.getName().equals("put") && args != null && args.length > 1;
                    if (put && args[0] instanceof String key && blockedAuthority != null && key.endsWith(blockedAuthority)
                            && args[1] instanceof byte[] bytes) {
                        var candidate = M5TargetDeleteAuthorityCodecV1.decodeAuthority(CanonicalBytes.copyOf(bytes));
                        if (candidate.state() == TargetDeleteAuthorityStateV1.DELETE_DONE_V1) {
                            withheldDone.incrementAndGet();
                            return CompletableFuture.failedFuture(new TimeoutException("native delete completed; DONE write unavailable"));
                        }
                    }
                    try {
                        return method.invoke(actual, args);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    void reclaim(String mode, NereusKafkaOwnedProviderRuntime.Opened opened) throws Exception {
        if (mode.endsWith("_GC_INTERRUPTED")) verify(opened);
        else if (!mode.contains("_COLD")) KafkaBookKeeperM5ReplacementGcRealScenario.reclaimIfAdmitted(opened, mode.endsWith("_GC"));
    }

    void verify(NereusKafkaOwnedProviderRuntime.Opened opened) throws Exception {
        var selectedBefore = KafkaBookKeeperM5ReplacementGcRealScenario.selectedBytes(opened);
        NereusKafkaM5GcInputsV1 original;
        com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2.Snapshot before;
        com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteIntentV1 intent;
        com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2 target;
        try (var scan = opened.openNativeM5SourceScan().toCompletableFuture().get(30, TimeUnit.SECONDS)) {
            original = NereusKafkaM5GcInputsV1.captureReplacement(scan).toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertFalse(original.resources().isEmpty());
            target = original.resources().stream().sorted().findFirst().orElseThrow();
            before = scan.physical().quota().snapshot().toCompletableFuture().get(30, TimeUnit.SECONDS);
            blockedAuthority = target.authorityKey();
            assertThrows(ExecutionException.class, () -> scan.reclaimReplacedSources().toCompletableFuture()
                    .get(Math.multiplyExact(30L, original.resources().size()), TimeUnit.SECONDS));
            assertTrue(withheldDone.get() > 0, "actual delete did not reach the unavailable DONE write");
            var stored = scan.physical().read(target.authorityKey()).toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow();
            var authority = M5TargetDeleteStoredValueV2.decode(stored).fullAuthority().orElseThrow();
            assertEquals(TargetDeleteAuthorityStateV1.DELETE_INTENT_V1, authority.state());
            intent = authority.deleteIntent().orElseThrow();
            assertEquals(CaptureOutcome.DEFINITIVELY_ABSENT, scan.nativeClient().captureExactTarget(original.handle(target))
                    .toCompletableFuture().get(30, TimeUnit.SECONDS).outcome());
            assertFalse(scan.physical().quota().requireExistingReservation(target).toCompletableFuture()
                    .get(30, TimeUnit.SECONDS).settled(), "uncertain DONE refunded capacity");
        } finally {
            blockedAuthority = null;
        }
        // Reconstruct all facts and SDK clients; no reuse of the old GC target or its cached native metadata.
        try (var scan = opened.openNativeM5SourceScan().toCompletableFuture().get(30, TimeUnit.SECONDS)) {
            scan.reclaimReplacedSources().toCompletableFuture().get(Math.multiplyExact(30L, original.resources().size()), TimeUnit.SECONDS);
            var done = M5TargetDeleteStoredValueV2.decode(scan.physical().read(target.authorityKey()).toCompletableFuture()
                    .get(30, TimeUnit.SECONDS).orElseThrow()).compactDone().orElseThrow();
            assertEquals(intent.deleteAttemptIdSha256(), done.done().deleteAttemptIdSha256());
            for (var resource : original.resources()) {
                assertTrue(scan.physical().quota().requireExistingReservation(resource).toCompletableFuture()
                        .get(30, TimeUnit.SECONDS).settled());
                assertEquals(CaptureOutcome.DEFINITIVELY_ABSENT, scan.nativeClient().captureExactTarget(original.handle(resource))
                        .toCompletableFuture().get(30, TimeUnit.SECONDS).outcome());
            }
            var after = scan.physical().quota().snapshot().toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertEquals(500_000_000, after.head().capacityBytes());
            System.out.println("M5_CANONICAL_CAPACITY_BYTES " + after.head().capacityBytes());
            assertEquals(before.head().reservedResources() - original.resources().size(), after.head().reservedResources());
            assertEquals(before.head().settledResources() + original.resources().size(), after.head().settledResources());
        }
        assertEquals(selectedBefore, KafkaBookKeeperM5ReplacementGcRealScenario.selectedBytes(opened),
                "reconstructed GC changed the actual selected bytes");
    }
}
