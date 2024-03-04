package org.falland.grpc.longlivedstreams.server.address;

import io.grpc.*;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

class AddressInterceptorTest {

    private final AddressInterceptor underTest = new AddressInterceptor();

    @SuppressWarnings("unchecked")
    private final ServerCall<Long, Long> call = mock(ServerCall.class);

    @SuppressWarnings("unchecked")
    private final ServerCallHandler<Long, Long> handler = mock(ServerCallHandler.class);

    private final Metadata headers = mock(Metadata.class);

    @Test
    public void testInterceptCall_shouldExtractTheAddress() {
        SocketAddress address = new InetSocketAddress("test", 42);
        Attributes attributes = Attributes.newBuilder().set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, address).build();
        doReturn(attributes).when(call).getAttributes();

        AtomicReference<SocketAddress> addressCaptor = new AtomicReference<>();
        doReturn(new ServerCall.Listener<>() {
            @Override
            public void onMessage(Object message) {
                addressCaptor.set(AddressInterceptor.ADDRESS_KEY.get());
            }
        }).when(handler).startCall(call, headers);

        var result = underTest.interceptCall(call, headers, handler);
        result.onMessage(1L);

        assertEquals(address, addressCaptor.get());
    }

}