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

package org.apache.kafka.server.config;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

/** Immutable stock-owned snapshot of the complete BookKeeper primary-WAL binding. */
public record NereusKafkaBookKeeperConfig(
        String deploymentId,
        String clusterAlias,
        String providerScopeSha256,
        int ledgerIdPrefixBits,
        long ledgerIdPrefixValue,
        String ledgerIdNamespaceReservationId,
        int ensembleSize,
        int writeQuorumSize,
        int ackQuorumSize,
        String digestType,
        Path passwordFile,
        String passwordVersion,
        long maxEntriesPerLedger,
        long maxBytesPerLedger,
        int maxAppendRangesPerLedger,
        int protectionSlotsPerRange,
        int maxReaderLeasesPerLedger,
        int maxUncertainAllocations,
        Duration maxLedgerAge,
        int maxWritesInFlight,
        int maxReadsInFlight,
        long maxReadBytesInFlight,
        Duration operationTimeout,
        Duration allocationTimeout,
        Duration sealTimeout,
        Duration deleteTimeout,
        Duration readerLeaseTtl,
        Duration readerLeaseRenewInterval,
        Duration retentionScanInterval,
        int retentionPageSize,
        long readinessEpoch,
        String readinessSha256,
        int persistentBrokerCount,
        LedgerGc ledgerGc
) {
    public NereusKafkaBookKeeperConfig {
        deploymentId = nonblank(deploymentId, "deploymentId");
        clusterAlias = nonblank(clusterAlias, "clusterAlias");
        providerScopeSha256 = sha256(providerScopeSha256, "providerScopeSha256");
        ledgerIdNamespaceReservationId = nonblank(
                ledgerIdNamespaceReservationId,
                "ledgerIdNamespaceReservationId");
        digestType = nonblank(digestType, "digestType");
        passwordFile = Objects.requireNonNull(passwordFile, "passwordFile")
                .toAbsolutePath()
                .normalize();
        passwordVersion = nonblank(passwordVersion, "passwordVersion");
        Objects.requireNonNull(maxLedgerAge, "maxLedgerAge");
        Objects.requireNonNull(operationTimeout, "operationTimeout");
        Objects.requireNonNull(allocationTimeout, "allocationTimeout");
        Objects.requireNonNull(sealTimeout, "sealTimeout");
        Objects.requireNonNull(deleteTimeout, "deleteTimeout");
        Objects.requireNonNull(readerLeaseTtl, "readerLeaseTtl");
        Objects.requireNonNull(readerLeaseRenewInterval, "readerLeaseRenewInterval");
        Objects.requireNonNull(retentionScanInterval, "retentionScanInterval");
        ledgerGc = Objects.requireNonNull(ledgerGc, "ledgerGc");
        readinessSha256 = sha256(readinessSha256, "readinessSha256");
        if (ledgerIdPrefixBits < 8 || ledgerIdPrefixBits > 24) {
            throw new IllegalArgumentException("ledgerIdPrefixBits must be in [8,24]");
        }
        long prefixLimit = 1L << ledgerIdPrefixBits;
        long prefixFloor = 1L << (ledgerIdPrefixBits - 1);
        if (ledgerIdPrefixValue < prefixFloor || ledgerIdPrefixValue >= prefixLimit) {
            throw new IllegalArgumentException(
                    "ledgerIdPrefixValue must be canonical with its highest prefix bit set");
        }
        if (ensembleSize < writeQuorumSize
                || writeQuorumSize < ackQuorumSize
                || ackQuorumSize <= 0) {
            throw new IllegalArgumentException(
                    "BookKeeper quorum sizes must satisfy ensemble >= write >= ack > 0");
        }
        if ((long) maxAppendRangesPerLedger * protectionSlotsPerRange > 65_536L) {
            throw new IllegalArgumentException(
                    "BookKeeper append range/protection slot product exceeds 65536");
        }
        if (readerLeaseRenewInterval.compareTo(readerLeaseTtl) >= 0) {
            throw new IllegalArgumentException(
                    "BookKeeper reader lease renewal must be shorter than its TTL");
        }
        if ((long) persistentBrokerCount + 1L > maxReaderLeasesPerLedger) {
            throw new IllegalArgumentException(
                    "BookKeeper reader leases cannot cover the broker set plus restart overlap");
        }
        ledgerGc.validateAgainst(readerLeaseTtl);
    }

    /** Local rollout controls; durable metadata and activation remain the deletion authority. */
    public record LedgerGc(
            int maxConcurrentDeletes,
            Duration maxClockSkew,
            Duration drainGrace,
            Duration lateCreateAuditGrace,
            boolean enabled,
            boolean dryRun
    ) {
        public LedgerGc {
            if (maxConcurrentDeletes <= 0) {
                throw new IllegalArgumentException(
                        "BookKeeper GC maxConcurrentDeletes must be positive");
            }
            maxClockSkew = nonNegative(maxClockSkew, "maxClockSkew");
            drainGrace = positive(drainGrace, "drainGrace");
            lateCreateAuditGrace = positive(
                    lateCreateAuditGrace,
                    "lateCreateAuditGrace");
            if (!enabled && !dryRun) {
                throw new IllegalArgumentException(
                        "disabled BookKeeper GC must remain dry-run");
            }
        }

        public static LedgerGc safeDefault() {
            return new LedgerGc(
                    1,
                    Duration.ofSeconds(30),
                    Duration.ofMinutes(5),
                    Duration.ofDays(7),
                    false,
                    true);
        }

        private void validateAgainst(Duration readerLeaseTtl) {
            if (drainGrace.compareTo(readerLeaseTtl.plus(maxClockSkew)) < 0) {
                throw new IllegalArgumentException(
                        "BookKeeper GC drain grace must cover reader lease TTL plus clock skew");
            }
        }

        private static Duration positive(Duration value, String name) {
            Duration exact = Objects.requireNonNull(value, name);
            if (exact.isZero() || exact.isNegative()) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return exact;
        }

        private static Duration nonNegative(Duration value, String name) {
            Duration exact = Objects.requireNonNull(value, name);
            if (exact.isNegative()) {
                throw new IllegalArgumentException(name + " must be non-negative");
            }
            return exact;
        }
    }

    private static String nonblank(String value, String name) {
        String exact = Objects.requireNonNull(value, name);
        if (exact.isBlank()) {
            throw new IllegalArgumentException(name + " must be nonblank");
        }
        return exact;
    }

    private static String sha256(String value, String name) {
        String exact = nonblank(value, name);
        if (!exact.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException(name + " must be lowercase SHA-256");
        }
        return exact;
    }
}
