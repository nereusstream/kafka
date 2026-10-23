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

import kafka.server.nereus.KafkaBookKeeperCreateResponseLossRealScenario;
import kafka.server.nereus.KafkaBookKeeperNeverCreatedTakeoverRealScenario;
import kafka.server.nereus.KafkaBookKeeperRootVerifierRealScenario;
import kafka.server.nereus.KafkaBookKeeperSourcePublicationRealScenario;
import kafka.server.nereus.KafkaBookKeeperNativeInputRealScenario;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Optional;

/** Requires a freshly run source-bound BK/Oxia receipt and real native Broker backend. */
public class KafkaBookKeeperM4AdmissionRealTest {
    @org.junit.jupiter.api.Test
    @Timeout(value = 240)
    public void nativeM5FirstPolicyPersistsAndTracksActualKafkaConfigWriters() throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null)
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        KafkaBookKeeperNativeInputRealScenario.run(Path.of(receipt), "POLICY", true);
    }

    @org.junit.jupiter.api.Test
    @Timeout(value = 240)
    public void nativeM5CancelledTaskCapturesActualReferenceClasses() throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null)
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        KafkaBookKeeperNativeInputRealScenario.run(Path.of(receipt), "TASK_OUTPUT", true);
    }

    @org.junit.jupiter.api.Test
    @Timeout(value = 240)
    public void nativeM5CancelledTaskReclaimsSettlesAndReadmits() throws Exception {
        KafkaBookKeeperNativeInputRealScenario.run(Path.of(System.getProperty("nereus.nsip.m4RunSummary")), "TASK_OUTPUT_GC", true);
    }

    @ParameterizedTest
    @ValueSource(strings = {"TASK_OUTPUT_GC_DISABLED", "TASK_OUTPUT_GC_DRY_RUN"})
    @Timeout(value = 240)
    public void nativeM5GcTotalSwitchVeto(String mode) throws Exception {
        KafkaBookKeeperNativeInputRealScenario.run(Path.of(System.getProperty("nereus.nsip.m4RunSummary")), mode, true);
    }

    @ParameterizedTest
    @ValueSource(strings = {"MEMBERSHIP", "LIFETIME", "CLEANUP", "POLICY", "CHECKPOINT", "CAPACITY", "PLAN"})
    @Timeout(value = 240)
    public void nativeM5InputCutUsesCurrentBrokerMembershipAndOwnsItsScan(String mode) throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null)
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        KafkaBookKeeperNativeInputRealScenario.run(Path.of(receipt), mode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"TASK", "TASK_ROLLOVER", "TASK_REGISTER_UNKNOWN", "TASK_CONVERT_UNKNOWN", "TASK_CONVERT_APPLIED_UNKNOWN", "TASK_CANCEL_UNKNOWN"})
    @Timeout(value = 240)
    public void nativeM5TaskRegistrationRetainsItsExactCapacityAcrossUnknowns(String mode) throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null)
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        KafkaBookKeeperNativeInputRealScenario.run(Path.of(receipt), mode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"TASK_MIN_LAG", "TASK_RATIO", "TASK_RATIO_NEAR_ONE", "TASK_MAX_LAG", "TASK_MAX_LAG_ZERO", "TASK_EMPTY_KEY"})
    @Timeout(value = 240)
    public void nativeM5TaskUsesActualCleanerPolicyAndEmptyKeySemantics(String mode) throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null)
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        KafkaBookKeeperNativeInputRealScenario.run(Path.of(receipt), mode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"TASK_OUTPUT", "TASK_OUTPUT_READ", "TASK_OUTPUT_ROLLOVER", "TASK_OUTPUT_APPEND_UNKNOWN",
        "TASK_OUTPUT_CLOSE_UNKNOWN", "TASK_OUTPUT_CLOSE_HELD", "TASK_OUTPUT_TICKET_UNKNOWN",
        "TASK_OUTPUT_WORKER_UNKNOWN", "TASK_OUTPUT_WORKER_STALE", "TASK_OUTPUT_EMPTY_KEY"})
    @Timeout(value = 240)
    public void nativeM5MaterializesAndDrainsItsActualTaskScope(String mode) throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null)
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        KafkaBookKeeperNativeInputRealScenario.run(Path.of(receipt), mode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"TASK_OUTPUT_PUBLISH", "TASK_OUTPUT_PUBLISH_UNKNOWN", "TASK_OUTPUT_PUBLISH_CLOSE_HELD",
        "TASK_OUTPUT_PUBLISH_ROLLOVER", "TASK_OUTPUT_PUBLISH_ROLLOVER_UNKNOWN", "TASK_OUTPUT_PUBLISH_CACHE_BUDGET_UNKNOWN",
        "TASK_OUTPUT_PUBLISH_COLD", "TASK_OUTPUT_PUBLISH_RELEASE", "TASK_OUTPUT_PUBLISH_RELEASE_UNKNOWN",
        "TASK_OUTPUT_PUBLISH_RELEASE_DRAIN_HELD", "TASK_OUTPUT_PUBLISH_RELEASE_COLD", "TASK_OUTPUT_PUBLISH_RELEASE_ROLLOVER",
        "TASK_OUTPUT_PUBLISH_RELEASE_OWNER_UNKNOWN", "TASK_OUTPUT_PUBLISH_RELEASE_OWNER_CLOSED", "TASK_OUTPUT_PUBLISH_RELEASE_OWNER_REFRESHED",
        "TASK_OUTPUT_PUBLISH_RELEASE_SCOPE_UNKNOWN", "TASK_OUTPUT_PUBLISH_COLD_SCOPE_HELD",
        "TASK_OUTPUT_PUBLISH_RELEASE_COLD_SCOPE_HELD"})
    @Timeout(value = 240)
    public void nativeM5PublishesItsActualTaskAndFetchesSelectedBytes(String mode) throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null)
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        KafkaBookKeeperNativeInputRealScenario.run(Path.of(receipt), mode);
    }

    @Test
    @Timeout(value = 600)
    public void nativeM5BookKeeperOnlyInternalTopicsCompactReclaimAndRecover() throws Exception {
        kafka.server.nereus.KafkaBookKeeperInternalCompactionRealScenario.run(Path.of(System.getProperty("nereus.nsip.m4RunSummary")));
    }

    @Test
    @Timeout(value = 240)
    public void nativeM5ReplacedSourceResumesAfterDeleteBeforeDone() throws Exception {
        KafkaBookKeeperNativeInputRealScenario.run(Path.of(System.getProperty("nereus.nsip.m4RunSummary")),
                "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_GC_INTERRUPTED", true);
    }

    @Test
    @Timeout(value = 240)
    public void nativeM5ReplacedSourceReclaimsAndSettles() throws Exception {
        KafkaBookKeeperNativeInputRealScenario.run(Path.of(System.getProperty("nereus.nsip.m4RunSummary")),
                "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_GC", true);
    }

    @ParameterizedTest
    @ValueSource(strings = {"TASK_OUTPUT_PUBLISH_RELEASE_CAPTURE"})
    @Timeout(value = 240)
    public void nativeM5RecompactionCapturesActualSelectedGenerationAndSealedTail(String mode) throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null)
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        KafkaBookKeeperNativeInputRealScenario.run(Path.of(receipt), mode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT", "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_COLD",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_COLD_SCOPE_HELD", "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_PUBLISH_UNKNOWN",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_PUBLISH_UNKNOWN_COLD", "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_SCOPE_UNKNOWN",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_SCOPE_UNKNOWN_COLD_SCOPE_HELD",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_FACTS_REFRESH",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_PROOF_REFRESH",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_PROOF_UNKNOWN",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_PHYSICAL_REFRESH",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_PHYSICAL_UNKNOWN",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_QUOTA_REFRESH",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_TASK_DIRECTORY_REFRESH",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_TASK_DIRECTORY_UNKNOWN",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_TASK_INVENTORY_UNKNOWN",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_TASK_DECISION_REFRESH",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_TASK_DECISION_UNKNOWN",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_TASK_TERMINAL_UNKNOWN",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_READER_SCOPE_REFRESH",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_READER_SCOPE_UNKNOWN",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_READER_SCOPE_MISSING",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_COLD_REPLACEMENT_READER_SCOPE_REFRESH",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_COLD_REPLACEMENT_READER_SCOPE_UNKNOWN",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_COLD_REPLACEMENT_READER_OWNER_REFRESH",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_COLD_REPLACEMENT_READER_OWNER_UNKNOWN",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_COLD_REPLACEMENT_RECOVERY_REFRESH",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_TRANSACTION_COLD",
        "TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_COLD_REPLACEMENT_SPECULATIVE_PENDING"})
    @Timeout(value = 240)
    public void nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache(String mode) throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null)
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        KafkaBookKeeperNativeInputRealScenario.run(Path.of(receipt), mode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"TASK_OUTPUT_PUBLISH_RELEASE_COLD_NO_TAIL"})
    @Timeout(value = 240)
    public void nativeM5ColdEntryOwnsAnActuallyRejectedEmptyTail(String mode) throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null)
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        KafkaBookKeeperNativeInputRealScenario.run(Path.of(receipt), mode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"FENCED", "UNKNOWN", "CLEANUP", "MEMBER_MISSING", "MEMBER_UNKNOWN", "MEMBER_REFRESH"})
    @Timeout(value = 240)
    public void nativeSourcePublicationRetainsExactFailureProtection(String mode) throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null)
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        KafkaBookKeeperSourcePublicationRealScenario.run(Path.of(receipt), mode);
    }
    @ParameterizedTest
    @ValueSource(strings = {"HEADER_HELD", "RECOVERED_HELD", "RECOVERED_UNKNOWN", "RECOVERED_THROWN", "RECOVERED_CAPACITY"})
    @Timeout(value = 240)
    public void nativeRootVerifierOwnsHeaderAndRecoveredReaders(String mode) throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null)
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        KafkaBookKeeperRootVerifierRealScenario.run(Optional.of(Path.of(receipt)), mode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"HISTORY_FENCE_HELD", "HISTORY_FENCE_UNKNOWN", "HISTORY_FENCE_CAPACITY",
        "HISTORY_CHECKPOINT_UNKNOWN", "HISTORY_REPLAY_HELD", "HISTORY_FETCH_HELD", "HISTORY_FETCH_UNKNOWN", "HISTORY_FETCH_CANCEL"})
    @Timeout(value = 240)
    public void selectedHistoryReaderOwnsItsExactInvocationAndCapacity(String mode) throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null)
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        KafkaBookKeeperRootVerifierRealScenario.run(Optional.of(Path.of(receipt)), mode);
    }

    @Test
    @Timeout(value = 240)
    public void historicalFetchMetadataReplyCannotBlockOwnerClose() throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null)
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        KafkaBookKeeperRootVerifierRealScenario.run(Optional.empty(), "HISTORY_FETCH_CLOSING");
    }

    @Test
    @Timeout(value = 240)
    public void historicalFetchAcrossRunsAndCachedIndexUsesOnlyLocalControlState() throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null)
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        KafkaBookKeeperRootVerifierRealScenario.run(Optional.of(Path.of(receipt)), "HISTORY_FETCH_LOCAL");
    }

    @Test
    @Timeout(value = 240)
    public void concurrentM4OwnerCloseWaitsForItsActualHistoricalReader() throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null)
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        KafkaBookKeeperRootVerifierRealScenario.run(Optional.of(Path.of(receipt)), "HISTORY_FETCH_CLOSE");
    }

    @ParameterizedTest
    @ValueSource(strings = {"HELD", "UNKNOWN", "CAPACITY", "UNKNOWN_CAPACITY", "CLEANUP_READ", "CLEANUP_LOST_REPLY"})
    @Timeout(value = 240)
    public void recoveryReaderCloseProtectsItsExactInvocationAndCapacity(String mode) throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null) {
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        }
        KafkaBookKeeperCreateResponseLossRealScenario.runReaderCloseResponseLoss(Optional.of(Path.of(receipt)), mode);
    }

    @Test
    @Timeout(value = 240)
    public void isolatedReceiptAdmitsBrokerFetchAndTwoColdRelocations() throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null) {
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        }
        KafkaBookKeeperOwnerRelocationRealTest.runColdOwnerRelocations(Optional.of(Path.of(receipt)));
    }

    @Test
    @Timeout(value = 180)
    public void brokerReconcilesLostPersistedOpenResponseBeforeNativeCreate() throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null) {
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        }
        KafkaBookKeeperOpenResponseLossRealScenario.run(Optional.of(Path.of(receipt)));
    }

    @Test
    @Timeout(value = 240)
    public void twoBrokerTakeoversRetainOneCreatedUnselectedLedger() throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null) {
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        }
        KafkaBookKeeperCreateResponseLossRealScenario.run(Optional.of(Path.of(receipt)));
    }

    @Test
    @Timeout(value = 240)
    public void confirmedCreateWithUnresolvedTicketStaysUnselectedUntilSuccessorReconciles() throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null) {
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        }
        KafkaBookKeeperCreateResponseLossRealScenario.runTicketReleaseResponseLoss(Optional.of(Path.of(receipt)));
    }

    @Test
    @Timeout(value = 240)
    public void lostRecoveredNativeFactsRetainOldWriterTicketUntilNextBroker() throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null) {
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        }
        KafkaBookKeeperCreateResponseLossRealScenario.runRecoveredFactsResponseLoss(Optional.of(Path.of(receipt)));
    }

    @Test
    @Timeout(value = 240)
    public void successorActivatesAfterFencingReservedButNeverCreatedLedger() throws Exception {
        String receipt = System.getProperty("nereus.nsip.m4RunSummary");
        if (receipt == null || System.getProperty("nereus.nsip.m4SourceRoot") == null) {
            throw new IllegalStateException("current-source M4 run summary and source root are required");
        }
        KafkaBookKeeperNeverCreatedTakeoverRealScenario.run(Optional.of(Path.of(receipt)));
    }
}
