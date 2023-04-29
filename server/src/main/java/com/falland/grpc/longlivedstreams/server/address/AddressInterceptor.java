package com.falland.grpc.longlivedstreams.server.address;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.net.SocketAddress;

/**
 * This interceptor extracts the remote client address from the {@link io.grpc.ServerCall}
 * Remote address is required to guarantee that only single stream per client is served on server at a time.
 * Double subscription for client is treated as an erroneous state and all excessive streams are closed.
 */
public class AddressInterceptor implements ServerInterceptor {

    public static final Context.Key<SocketAddress> ADDRESS_KEY = Context.key("address-context-key");

    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                                                      ServerCallHandler<ReqT, RespT> next) {
        Context context = Context.current().withValue(ADDRESS_KEY, call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
        return Contexts.interceptCall(context, call, headers, next);
    }
}
