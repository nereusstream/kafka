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

import org.apache.kafka.clients.producer.internals.GuardedCompletion;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Internal future used by {@link GuardedProducer}; never returned as a concrete type. */
final class GuardedRecordFuture implements Future<GuardedRecordMetadata>, GuardedCompletion {
    private final ProducerResourceGuard guard;
    private final GuardedCallback callback;
    private final CountDownLatch completed = new CountDownLatch(1);
    private final AtomicBoolean completedOnce = new AtomicBoolean(false);
    private volatile GuardedRecordMetadata value;
    private volatile ResourceGuardException failure;

    GuardedRecordFuture(final ProducerResourceGuard guard, final GuardedCallback callback) {
        this.guard = guard;
        this.callback = callback;
    }

    @Override
    public ProducerResourceGuard guard() {
        return guard;
    }

    @Override
    public void complete(final RecordMetadata metadata, final Object rawEvidence) {
        if (!(rawEvidence instanceof GuardedCompletion.Evidence)) {
            fail(new GuardedCompletion.Failure(ResourceGuardFailureReason.RESPONSE_EVIDENCE_INTEGRITY,
                    "guarded producer returned no structured response evidence", null, null, false));
            return;
        }

        GuardedCompletion.Evidence evidence = (GuardedCompletion.Evidence) rawEvidence;
        if (!guard.equals(evidence.requestContext.guard)) {
            fail(new GuardedCompletion.Failure(ResourceGuardFailureReason.RESPONSE_EVIDENCE_INTEGRITY,
                    "guarded response evidence does not match the request guard", null, evidence, false));
            return;
        }

        try {
            if (metadata == null || metadata.offset() != evidence.baseOffset + evidence.selectedBatchRecordIndex) {
                throw new IllegalArgumentException("guarded metadata offset does not match response evidence");
            }
            GuardedResponseEvidence publicEvidence = new GuardedResponseEvidence(
                    guard.authenticatedClusterId(), guard.canonicalTopic(), guard.expectedTopicId(), guard.partition(),
                    evidence.requestContext.requestVersion, evidence.requestContext.correlationId,
                    evidence.requestContext.brokerNodeId, evidence.errorCode, evidence.baseOffset,
                    evidence.logAppendTimeMs, evidence.responseLeaderEpoch,
                    evidence.requestContext.produceRequestBodySha256,
                    evidence.produceResponseBodySha256,
                    evidence.requestContext.selectedBatchRecordsSha256,
                    evidence.selectedRecordValueSha256,
                    evidence.selectedBatchRecordIndex, evidence.selectedBatchRecordCount);
            finish(new GuardedRecordMetadata(metadata, guard, publicEvidence), null);
        } catch (RuntimeException exception) {
            fail(new GuardedCompletion.Failure(ResourceGuardFailureReason.RESPONSE_EVIDENCE_INTEGRITY,
                    "guarded response evidence failed validation", exception, evidence, false));
        }
    }

    @Override
    public void fail(final Object rawFailure) {
        GuardedCompletion.Failure guardedFailure;
        if (rawFailure instanceof GuardedCompletion.Failure) {
            guardedFailure = (GuardedCompletion.Failure) rawFailure;
        } else {
            Throwable cause = rawFailure instanceof Throwable ? (Throwable) rawFailure : null;
            guardedFailure = new GuardedCompletion.Failure(
                    ResourceGuardFailureReason.AMBIGUOUS_PRIOR_ATTEMPT,
                    "guarded produce failed without a typed sender result", cause, null, false);
        }

        GuardedResponseEvidence publicEvidence = null;
        if (guardedFailure.evidence != null && guardedFailure.evidence.selectedBatchRecordIndex >= 0) {
            try {
                GuardedCompletion.Evidence evidence = guardedFailure.evidence;
                publicEvidence = new GuardedResponseEvidence(
                        guard.authenticatedClusterId(), guard.canonicalTopic(), guard.expectedTopicId(), guard.partition(),
                        evidence.requestContext.requestVersion, evidence.requestContext.correlationId,
                        evidence.requestContext.brokerNodeId, evidence.errorCode, evidence.baseOffset,
                        evidence.logAppendTimeMs, evidence.responseLeaderEpoch,
                        evidence.requestContext.produceRequestBodySha256,
                        evidence.produceResponseBodySha256,
                        evidence.requestContext.selectedBatchRecordsSha256,
                        evidence.selectedRecordValueSha256,
                        evidence.selectedBatchRecordIndex, evidence.selectedBatchRecordCount);
            } catch (RuntimeException evidenceFailure) {
                guardedFailure = new GuardedCompletion.Failure(
                        ResourceGuardFailureReason.RESPONSE_EVIDENCE_INTEGRITY,
                        "guarded failure evidence failed validation", evidenceFailure, null, false);
            }
        }

        ResourceGuardException exception = new ResourceGuardException(
                guardedFailure.message, guardedFailure.cause, guardedFailure.reason, guard,
                Optional.ofNullable(publicEvidence), guardedFailure.definitelyNotPersisted);
        finish(null, exception);
    }

    private void finish(final GuardedRecordMetadata metadata, final ResourceGuardException exception) {
        if (!completedOnce.compareAndSet(false, true)) {
            return;
        }
        value = metadata;
        failure = exception;
        completed.countDown();
        if (callback != null) {
            callback.onCompletion(metadata, exception);
        }
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public GuardedRecordMetadata get() throws InterruptedException, ExecutionException {
        completed.await();
        return valueOrError();
    }

    @Override
    public GuardedRecordMetadata get(final long timeout, final TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (!completed.await(timeout, unit)) {
            throw new TimeoutException("Timed out waiting for a guarded produce response");
        }
        return valueOrError();
    }

    private GuardedRecordMetadata valueOrError() throws ExecutionException {
        if (failure != null) {
            throw new ExecutionException(failure);
        }
        return value;
    }

    @Override
    public boolean isDone() {
        return completed.getCount() == 0L;
    }
}
