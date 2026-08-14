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
package org.apache.kafka.clients.producer.internals;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Small, deterministic SHA-256 helper for guarded wire evidence. */
public final class GuardedEvidenceUtils {
    private GuardedEvidenceUtils() {
    }

    public static byte[] sha256(final ByteBuffer source) {
        ByteBuffer input = source.duplicate();
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JRE does not provide SHA-256", e);
        }
        byte[] chunk = new byte[Math.min(8192, Math.max(1, input.remaining()))];
        while (input.hasRemaining()) {
            int length = Math.min(input.remaining(), chunk.length);
            input.get(chunk, 0, length);
            digest.update(chunk, 0, length);
        }
        return digest.digest();
    }
}
