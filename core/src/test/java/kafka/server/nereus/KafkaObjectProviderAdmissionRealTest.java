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
import kafka.server.nsip.KafkaBookKeeperOwnerRelocationRealTest;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.config.NereusKafkaConfigs;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.storage.object.materialization.M5CellProviderProfileV2;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ProviderObjectOutcome;
import com.nereusstream.storage.object.provider.RepeatableObjectBody;
import com.nereusstream.storage.object.s3.S3C1ObjectProviderTransport;
import com.nereusstream.storage.object.s3.S3M5PhysicalNamespaceAuthorityV2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.oxia.client.api.OxiaClientBuilder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Only the original registered scope can reopen a Kafka Object provider. */
public class KafkaObjectProviderAdmissionRealTest {
    private static final String USER = "m6kafka";
    private static final String PASSWORD = "m6kafka-test-secret";
    private static final String CREDENTIAL_SCOPE = "kafka-object";
    private static final String PREFIX = "v2/m6/kafka-cell";

    @Test
    @Timeout(120)
    public void reopensExactRegisteredScopeAndRejectsForeignBucket(@TempDir Path work) throws Exception {
        var endpoint = URI.create(System.getProperty("nereus.nsip.s3Endpoint"));
        var oxia = System.getProperty("nereus.nsip.oxia");
        try (var client = S3Client.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                     .forcePathStyle(true)
                     .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(USER, PASSWORD)))
                     .build();
                 var control = OxiaClientBuilder.create(oxia).asyncClient().get(30, TimeUnit.SECONDS)) {
            var namespace = OxiaPhysicalMetadataNamespaceV2.provision(control).toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            String bucket = newVersionedBucket(client);
            var physical = S3M5PhysicalNamespaceAuthorityV2.provision(client, bucket, namespace.identity());
            var props = configuration(work, endpoint, oxia, bucket);
            var config = new KafkaConfig(props, false);
            var cell = NereusBookKeeperCapability.metadataPolicy(config).cellIdentity();
            var core = config.nereusKafkaStorageConfig().core();
            var configuration = NereusKafkaObjectProvider.configurationSha256(core);
            var binding = namespace.bind(physical).toCompletableFuture().get(30, TimeUnit.SECONDS);
            var refs = new M5CellProviderProfileV2.References("local-s3", CREDENTIAL_SCOPE, "NONE",
                    "kafka-cell-quota", "kafka-owner", "version-match-delete");
            var profile = new M5CellProviderProfileV2(binding, cell, PREFIX, refs,
                    S3C1ObjectProviderTransport.ADAPTER_VERSION, configuration, 1 << 20, 1 << 20, 100, 1024);
            var profileFile = work.resolve("object-capability");
            Files.write(profileFile, profile.encode().toByteArray());
            var missing = assertThrows(ExecutionException.class,
                    () -> NereusKafkaObjectProvider.open(config, cell, namespace));
            assertTrue(missing.getCause().getMessage().contains("Original Cell Provider scope is absent"));
            namespace.openCellProviderScopes(physical).thenCompose(registry -> registry.prepare(profile).admit())
                    .toCompletableFuture().get(30, TimeUnit.SECONDS);
            try (var provider = NereusKafkaObjectProvider.open(config, cell, namespace)) {
                provider.requireCurrent().toCompletableFuture().get(30, TimeUnit.SECONDS);
                assertEquals(profile.scopeId(), provider.session().providerScopeId());
                assertExactObject(provider);
                provider.beginDrain();
            }
            var foreign = S3M5PhysicalNamespaceAuthorityV2.provision(client, newVersionedBucket(client), namespace.identity());
            var foreignBinding = namespace.bind(foreign).toCompletableFuture().get(30, TimeUnit.SECONDS);
            var wrong = new M5CellProviderProfileV2(foreignBinding, cell, PREFIX, refs,
                    profile.adapterVersion(), configuration, 1 << 20, 1 << 20, 100, 1024);
            Files.write(profileFile, wrong.encode().toByteArray());
            var mismatch = assertThrows(IllegalStateException.class,
                    () -> NereusKafkaObjectProvider.open(config, cell, namespace));
            assertTrue(mismatch.getMessage().contains("S3 physical namespace incarnation differs"));
        }
    }

    private static Map<String, Object> configuration(Path work, URI endpoint, String oxia, String bucket)
            throws Exception {
        KafkaBookKeeperOwnerRelocationRealTest.writeCapability(work, Optional.empty());
        var props = new HashMap<>(KafkaBookKeeperOwnerRelocationRealTest.nativeConfig(
                work, "zk://127.0.0.1:2181/ledgers", oxia));
        props.put("node.id", "0");
        props.put("process.roles", "broker");
        props.put("controller.quorum.voters", "1@127.0.0.1:9193");
        props.put("controller.listener.names", "CONTROLLER");
        props.put("log.dirs", work.resolve("logs").toString());
        props.put(NereusKafkaConfigs.PROFILE_CONFIG, "BOOKKEEPER_WAL_ASYNC_OBJECT");
        props.put(NereusKafkaConfigs.OBJECT_PROVIDER_CONFIG, "s3");
        props.put(NereusKafkaConfigs.OBJECT_BUCKET_CONFIG, bucket);
        props.put(NereusKafkaConfigs.OBJECT_ENDPOINT_CONFIG, endpoint.toString());
        props.put(NereusKafkaConfigs.OBJECT_REGION_CONFIG, Region.US_EAST_1.id());
        props.put(NereusKafkaConfigs.OBJECT_PATH_STYLE_ACCESS_CONFIG, "true");
        props.put(NereusKafkaConfigs.OBJECT_CAPABILITY_FILE_CONFIG, work.resolve("object-capability").toString());
        Path credentials = work.resolve("object-credentials");
        Files.writeString(credentials, "[" + CREDENTIAL_SCOPE + "]\naws_access_key_id=" + USER
                + "\naws_secret_access_key=" + PASSWORD + "\n");
        props.put(NereusKafkaConfigs.OBJECT_CREDENTIALS_FILE_CONFIG, credentials.toString());
        return props;
    }

    private static String newVersionedBucket(S3Client client) {
        String name = "nereus-m6-" + UUID.randomUUID();
        client.createBucket(request -> request.bucket(name));
        client.putBucketVersioning(request -> request.bucket(name)
                .versioningConfiguration(version -> version.status(BucketVersioningStatus.ENABLED)));
        return name;
    }

    private static void assertExactObject(NereusKafkaObjectProvider provider) throws Exception {
        var bytes = CanonicalBytes.copyOf("native-kafka-object".getBytes(StandardCharsets.UTF_8));
        var identity = new ObjectIdentity(PREFIX + "/" + Uuid.randomUuid(), bytes.length(), Sha256Digest.hash(bytes));
        var result = provider.session().conditionalCreate(new RepeatableObjectBody() {
            public ObjectIdentity identity() {
                return identity;
            }

            public InputStream openStream() {
                return new ByteArrayInputStream(bytes.toByteArray());
            }
        });
        assertEquals(ProviderObjectOutcome.APPLIED_EXACT, result.outcome());
        assertTrue(result.versionToken().isPresent());
        assertEquals(bytes, provider.session().readVerifiedObject(identity));
    }
}
