package org.falland.grpc.longlivedstreams.examples.apps.utils;

import org.falland.grpc.longlivedstreams.server.BindableServiceProvider;

import java.util.Collection;

public interface Service<Req, Resp> extends BindableServiceProvider {

    Collection<Req> getMessages();

    void publishMessage(Resp message);


}
