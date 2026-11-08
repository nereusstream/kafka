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
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ObjectProviderCapabilities;
import com.nereusstream.storage.object.provider.ObjectProviderTransport;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/** Wraps only physical writes. Reads keep the exact original Provider capabilities and failure classification. */
final class NereusKafkaObjectWalTransport implements ObjectProviderTransport {
    private final ObjectProviderTransport delegate;
    private final NereusKafkaObjectWalDispatchFence dispatch;

    NereusKafkaObjectWalTransport(ObjectProviderTransport delegate, NereusKafkaObjectWalDispatchFence dispatch) {
        this.delegate = delegate;
        this.dispatch = dispatch;
    }

    @Override
    public ObjectProviderCapabilities capabilities() {
        return delegate.capabilities();
    }

    @Override
    public ConditionalCreateResult putIfAbsent(ObjectIdentity identity, InputStream body) throws IOException {
        return putIfAbsentWithEvidence(identity, body).outcome();
    }

    @Override
    public ConditionalCreateResponse putIfAbsentWithEvidence(ObjectIdentity identity, InputStream body)
            throws IOException {
        dispatch.arm(identity);
        var result = delegate.putIfAbsentWithEvidence(identity, body);
        if (result.outcome() != ConditionalCreateResult.RESPONSE_UNKNOWN) {
            dispatch.complete(identity);
        }
        return result;
    }

    @Override
    public void requireUncertainCreateTerminalProof(ObjectIdentity identity, boolean exactObjectPresent)
            throws IOException {
        if (!exactObjectPresent) {
            throw new IOException("Object WAL dispatch remains armed after a negative Provider observation");
        }
        dispatch.completeVerified(identity);
    }

    @Override
    public StreamingObject get(String key, Optional<CanonicalBytes> version) throws IOException {
        return delegate.get(key, version);
    }

    @Override
    public StreamingObject getRange(String key, long start, long end, Optional<CanonicalBytes> version)
            throws IOException {
        return delegate.getRange(key, start, end, version);
    }

    @Override
    public ListPage list(String prefix, Optional<CanonicalBytes> token, int maximumKeys) throws IOException {
        return delegate.list(prefix, token, maximumKeys);
    }

    @Override
    public FailureKind classifyFailure(IOException failure) {
        return delegate.classifyFailure(failure);
    }
}
