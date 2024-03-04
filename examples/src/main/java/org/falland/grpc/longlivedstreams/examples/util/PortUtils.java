package org.falland.grpc.longlivedstreams.examples.util;

import java.io.IOException;
import java.net.ServerSocket;

public enum PortUtils {

    INSTANCE;

    public int findFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException ignored) {
        }
        throw new IllegalStateException("Could not find a free TCP/IP port to start embedded Jetty HTTP Server on");
    }
}
