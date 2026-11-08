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

import kafka.server.KafkaConfig;

import org.apache.kafka.server.config.NereusKafkaConfigs;

import com.nereusstream.storage.object.kms.KmsCellSession;
import com.nereusstream.storage.object.materialization.M5CellProviderProfileV2;
import com.nereusstream.storage.object.vault.VaultTransitRunKeyKms;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.regex.Pattern;

/** Owns the exact Vault wrapping route admitted for this native Object WAL Cell. */
final class NereusKafkaObjectWalKms implements AutoCloseable {
    private static final int MAX_TOKEN_BYTES = 8192;
    private static final int MAX_CELL_RUN_KEYS = 128;
    private static final Pattern KEY_IDENTITY =
            Pattern.compile("vault-transit://[A-Za-z0-9][A-Za-z0-9_-]{0,127}/keys/[A-Za-z0-9][A-Za-z0-9_-]{0,127}");

    private final VaultTransitRunKeyKms transport;
    private final KmsCellSession session;

    private NereusKafkaObjectWalKms(VaultTransitRunKeyKms transport, KmsCellSession session) {
        this.transport = transport;
        this.session = session;
    }

    static NereusKafkaObjectWalKms open(KafkaConfig config, M5CellProviderProfileV2 profile)
            throws IOException, InterruptedException {
        if (!profile.providerConfigurationSha256().equals(NereusKafkaObjectProvider.configurationSha256(config))) {
            throw new IllegalArgumentException("native Object WAL wrapping key is outside the admitted Cell");
        }
        var identity = keyIdentity(config);
        var key = URI.create(identity);
        var route = key.getRawPath().split("/", -1);
        var endpoint = endpoint(config);
        var token = token(Path.of(required(config, NereusKafkaConfigs.OBJECT_WAL_VAULT_TOKEN_FILE_CONFIG)));
        var configured = config.nereusKafkaStorageConfig().append().timeout();
        var maximum = Duration.ofMinutes(2);
        var timeout = configured.compareTo(maximum) <= 0 ? configured : maximum;
        var random = new SecureRandom();
        var client = HttpClient.newBuilder().connectTimeout(timeout).build();
        var transport = new VaultTransitRunKeyKms(client, endpoint, key.getRawAuthority(), route[2],
                () -> token, timeout, random);
        try {
            if (!identity.equals(transport.keyIdentity())) {
                throw new IllegalArgumentException("native Object WAL wrapping key differs from its admitted profile");
            }
            verifyRoundTrip(transport);
            return new NereusKafkaObjectWalKms(transport,
                    new KmsCellSession(transport, profile.scopeId(), identity, MAX_CELL_RUN_KEYS, random));
        } catch (IOException | InterruptedException | RuntimeException | Error failure) {
            transport.close();
            throw failure;
        }
    }

    static String keyIdentity(KafkaConfig config) {
        var value = required(config, NereusKafkaConfigs.OBJECT_WAL_VAULT_KEY_CONFIG);
        if (!KEY_IDENTITY.matcher(value).matches()) {
            throw new IllegalArgumentException("native Object WAL needs a canonical Vault Transit key identity");
        }
        return value;
    }

    static URI endpoint(KafkaConfig config) {
        var configured = required(config, NereusKafkaConfigs.OBJECT_WAL_VAULT_ENDPOINT_CONFIG);
        var uri = URI.create(configured);
        boolean http = "https".equals(uri.getScheme()) || "http".equals(uri.getScheme());
        boolean basePath = "".equals(uri.getRawPath()) || "/".equals(uri.getRawPath());
        if (!http || uri.getHost() == null
                || uri.getRawUserInfo() != null || uri.getRawQuery() != null || uri.getRawFragment() != null
                || !basePath) {
            throw new IllegalArgumentException("native Object WAL Vault endpoint must be an HTTP(S) origin");
        }
        return URI.create(configured.endsWith("/") ? configured : configured + "/");
    }

    private static void verifyRoundTrip(VaultTransitRunKeyKms transport) throws IOException, InterruptedException {
        try (var probe = transport.generateAndWrap()) {
            byte[] recovered = transport.unwrap(probe.envelope());
            try {
                if (!probe.plaintextKey().use(key -> MessageDigest.isEqual(key, recovered))) {
                    throw new IllegalStateException("native Object WAL KMS did not recover its generated key");
                }
            } finally {
                Arrays.fill(recovered, (byte) 0);
            }
        }
    }

    private static String token(Path path) throws IOException {
        try (var input = Files.newInputStream(path)) {
            byte[] bytes = input.readNBytes(MAX_TOKEN_BYTES + 1);
            try {
                var value = new String(bytes, StandardCharsets.UTF_8).strip();
                if (bytes.length > MAX_TOKEN_BYTES || value.isEmpty()
                        || value.chars().anyMatch(Character::isWhitespace)) {
                    throw new IllegalArgumentException("native Object WAL Vault token file is invalid");
                }
                return value;
            } finally {
                Arrays.fill(bytes, (byte) 0);
            }
        }
    }

    private static String required(KafkaConfig config, String key) {
        String value = config.getString(key);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("native Object WAL requires " + key);
        return value;
    }

    KmsCellSession session() {
        return session;
    }

    @Override
    public void close() {
        session.close();
        transport.close();
    }
}
