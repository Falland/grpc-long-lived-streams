package org.falland.grpc.longlivedstreams.server;

import io.grpc.BindableService;

public interface BindableServiceProvider {

    BindableService getBindableService();
}
