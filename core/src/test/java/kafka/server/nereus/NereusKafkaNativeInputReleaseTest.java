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

import org.junit.jupiter.api.Test;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionAdmissionV1;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NereusKafkaNativeInputReleaseTest {
    @Test
    void ownerDrainRebasesOnlyItsCancellationAfterAnotherOwnerWinsTheCellCas() {
        var fixture = new Fixture();
        fixture.store.beforeNextCas = () -> assertEquals(KafkaCompactionAdmissionV1.Outcome.APPLIED_EXACT,
                fixture.admission.reserve(fixture.arriving));
        fixture.capture.releaseAfterOwnerDrain();
        fixture.requireOnlySiblings(5);
    }

    @Test
    void ownerDrainConfirmsANewFenceWhenItsAppliedReplyWasOvertakenByASibling() {
        var fixture = new Fixture();
        fixture.store.loseAppliedReply = true;
        fixture.store.afterNextCas = () -> assertEquals(KafkaCompactionAdmissionV1.Outcome.APPLIED_EXACT,
                fixture.admission.reserve(fixture.arriving));
        fixture.capture.releaseAfterOwnerDrain();
        fixture.requireOnlySiblings(6);
    }

    @Test
    void unknownCancellationRetainsTheOriginalReservationAndDoesNotRetry() {
        var fixture = new Fixture();
        fixture.store.holdNextCas = true;
        var failure = assertThrows(IllegalStateException.class, fixture.capture::releaseAfterOwnerDrain);
        assertTrue(failure.getMessage().contains("OUTCOME_UNKNOWN"));
        assertEquals(List.of(fixture.own, fixture.sibling).stream()
                .sorted(java.util.Comparator.comparing(value -> value.compactionTaskIdSha256().toHex())).toList(),
                fixture.admission.read().orElseThrow().reservations());
        assertEquals(1, fixture.store.heldCancellations);
        assertEquals(3, fixture.admission.read().orElseThrow().generation());
    }

    private static final class Fixture {
        final Store store = new Store();
        final KafkaCompactionAdmissionV1 admission = new KafkaCompactionAdmissionV1(store, digest("release-cell"));
        final KafkaCompactionAdmissionV1.Reservation own = reservation("owned");
        final KafkaCompactionAdmissionV1.Reservation sibling = reservation("sibling");
        final KafkaCompactionAdmissionV1.Reservation arriving = reservation("arriving");
        final NereusKafkaNativeInputV2.CellReservation capture;

        Fixture() {
            assertEquals(KafkaCompactionAdmissionV1.Outcome.APPLIED_EXACT, admission.install(
                    new KafkaCompactionAdmissionV1.CellCaps(4, 1000, 1000, 1000, 1000, 1000, 1000, 1000,
                            1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000)));
            assertEquals(KafkaCompactionAdmissionV1.Outcome.APPLIED_EXACT, admission.reserve(own));
            assertEquals(KafkaCompactionAdmissionV1.Outcome.APPLIED_EXACT, admission.reserve(sibling));
            capture = NereusKafkaNativeInputV2.CellReservation.selectedCache(admission, own, digest("selected"), Runnable::run);
        }

        void requireOnlySiblings(long generation) {
            var state = admission.read().orElseThrow();
            assertEquals(generation, state.generation());
            assertEquals(2, state.reservations().size());
            assertTrue(state.reservations().containsAll(List.of(sibling, arriving)));
            assertFalse(state.reservations().contains(own));
        }
    }

    private static KafkaCompactionAdmissionV1.Reservation reservation(String name) {
        return new KafkaCompactionAdmissionV1.Reservation(digest(name), 10, 10, 10, 10, 10, 10, 10,
                10, 10, 10, 10, 10, 10, 10, 10);
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalUtf8.fromString(value).bytes());
    }

    private static final class Store implements CanonicalControlMetadataStore {
        final Map<String, CanonicalBytes> values = new LinkedHashMap<>();
        Runnable beforeNextCas;
        Runnable afterNextCas;
        boolean holdNextCas;
        boolean loseAppliedReply;
        int heldCancellations;

        public Optional<CanonicalBytes> get(String key) {
            return Optional.ofNullable(values.get(key));
        }

        public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes value) {
            return values.putIfAbsent(key, value) == null ? ControlMutationOutcome.APPLIED : ControlMutationOutcome.DEFINITIVE_CONFLICT;
        }

        public ControlMutationOutcome compareAndSet(String key, Optional<CanonicalBytes> expected, CanonicalBytes candidate) {
            var before = beforeNextCas;
            beforeNextCas = null;
            if (before != null) before.run();
            if (holdNextCas) {
                heldCancellations++;
                return ControlMutationOutcome.RESPONSE_UNKNOWN;
            }
            if (!Objects.equals(values.get(key), expected.orElse(null))) return ControlMutationOutcome.DEFINITIVE_CONFLICT;
            values.put(key, candidate);
            boolean lostReply = loseAppliedReply;
            loseAppliedReply = false;
            var after = afterNextCas;
            afterNextCas = null;
            if (after != null) after.run();
            return lostReply ? ControlMutationOutcome.RESPONSE_UNKNOWN : ControlMutationOutcome.APPLIED;
        }
    }
}
