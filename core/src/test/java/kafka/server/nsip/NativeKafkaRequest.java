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

import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.test.KafkaClusterTestKit;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** One Kafka protocol exchange with no client retries. */
public final class NativeKafkaRequest implements AutoCloseable {
    private final Socket socket;
    private final RequestHeader header;

    public NativeKafkaRequest(KafkaClusterTestKit cluster, int broker, AbstractRequest request) throws IOException {
        header = new RequestHeader(request.apiKey(), request.version(), "nsip-native", 1);
        socket = new Socket("127.0.0.1",
                cluster.brokers().get(broker).socketServer().boundPort(cluster.nodes().brokerListenerName()));
        try {
            var bytes = request.serializeWithHeader(header);
            var output = new DataOutputStream(socket.getOutputStream());
            output.writeInt(bytes.remaining());
            output.write(bytes.array(), bytes.arrayOffset() + bytes.position(), bytes.remaining());
            output.flush();
        } catch (IOException | RuntimeException failure) {
            try {
                socket.close();
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    public AbstractResponse receive(int timeoutMs) throws IOException {
        socket.setSoTimeout(timeoutMs);
        var input = new DataInputStream(socket.getInputStream());
        int length = input.readInt();
        assertTrue(length > 0 && length < 1048576);
        byte[] response = new byte[length];
        input.readFully(response);
        return AbstractResponse.parseResponse(ByteBuffer.wrap(response), header);
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
