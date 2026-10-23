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
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NereusKafkaM4AdmissionV2Test {
    @Test
    void changingOnlyAsyncCompletionInnerClassRejectsRuntimeArtifact(@TempDir Path directory) throws Exception {
        Class<?> executor = Class.forName("com.nereusstream.storage.object.read.BindingReadAsyncExecutorV1");
        Path original = Path.of(executor.getProtectionDomain().getCodeSource().getLocation().toURI());
        String expected = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(Files.readAllBytes(original)));
        Map<String, String> receipt = Map.of("nereus-storage-object", expected);
        assertDoesNotThrow(() -> NereusKafkaM4AdmissionV2.verifyArtifactDigest(
                receipt, "nereus-storage-object", original));

        Path changed = directory.resolve("changed-read-implementation.jar");
        boolean changedState = false;
        try (var jar = new JarFile(original.toFile());
             var output = new JarOutputStream(Files.newOutputStream(changed))) {
            var entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                byte[] bytes;
                try (var input = jar.getInputStream(entry)) {
                    bytes = input.readAllBytes();
                }
                if (entry.getName().equals(
                        "com/nereusstream/storage/object/read/BindingReadAsyncExecutorV1$State.class")) {
                    bytes[bytes.length - 1] ^= 1;
                    changedState = true;
                }
                output.putNextEntry(new JarEntry(entry.getName()));
                output.write(bytes);
                output.closeEntry();
            }
        }
        assertTrue(changedState);
        assertThrows(IllegalArgumentException.class, () -> NereusKafkaM4AdmissionV2.verifyArtifactDigest(
                receipt, "nereus-storage-object", changed));
    }
}
