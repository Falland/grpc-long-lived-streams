package io.grpc.longlivedstreams.examples.apps;

public class Payload {

    static final byte[] payload = randomBytes(10240);

    private static byte[] randomBytes(int i) {
        return new byte[i];
    }
}
