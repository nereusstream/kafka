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

import org.apache.kafka.server.util.Json;
import org.apache.kafka.test.TestUtils;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Controls an owned loopback relay for one exact version in the isolated MinIO fixture. */
final class KafkaObjectFaultRelay implements AutoCloseable {
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final URI control = URI.create(required("NEREUS_M6_TEST_FAULT_CONTROL"));
    private final String token = required("NEREUS_M6_TEST_FAULT_TOKEN");
    private final String path;
    private final String version;

    KafkaObjectFaultRelay(String bucket, String key, String version) {
        this.path = "/" + bucket + "/" + key;
        this.version = version;
    }

    static URI endpoint() {
        return URI.create(required("NEREUS_M6_TEST_FAULT_PROXY"));
    }

    void arm(String mode) throws Exception {
        byte[] body = Json.encodeAsBytes(Map.of("path", path, "version", version, "mode", mode));
        send(request().POST(HttpRequest.BodyPublishers.ofByteArray(body)).build());
    }

    void awaitRequest() throws Exception {
        TestUtils.waitForCondition(() -> {
            var response = send(request().GET().build());
            var state = Json.parseBytesAs(response, Map.class);
            return ((Number) state.get("hits")).intValue() > 0;
        }, 30_000, "the exact Object version was not requested through the fault relay");
    }

    private HttpRequest.Builder request() {
        return HttpRequest.newBuilder(control).timeout(Duration.ofSeconds(5)).header("X-Nereus-Test-Token", token);
    }

    private byte[] send(HttpRequest request) throws Exception {
        var response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(200, response.statusCode(), "the isolated Object fault control request failed");
        return response.body();
    }

    void release() throws Exception {
        send(request().DELETE().build());
    }

    @Override
    public void close() throws Exception {
        release();
    }

    private static String required(String name) {
        return Objects.requireNonNull(System.getenv(name), "missing isolated Object fault setting " + name);
    }
}
