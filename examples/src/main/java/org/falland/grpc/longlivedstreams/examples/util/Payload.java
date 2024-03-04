package org.falland.grpc.longlivedstreams.examples.util;

import java.util.concurrent.ThreadLocalRandom;

public enum Payload {

    INSTANCE;

    private final byte[] payload = randomBytes(new byte[10_240]);

    public byte[] getPayload() {
        return payload;
    }

    private byte[] randomBytes(byte[] bytes) {
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }
}
