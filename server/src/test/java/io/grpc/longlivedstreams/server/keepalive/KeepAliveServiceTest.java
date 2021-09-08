package io.grpc.longlivedstreams.server.keepalive;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KeepAliveServiceTest {

    @Mock
    private IdleAware serverCall;
    private KeepAliveServiceImpl keepAliveComponent;

    @BeforeEach
    void setup() throws Exception {
        keepAliveComponent = new KeepAliveServiceImpl(5, 1);
        keepAliveComponent.start();
    }

    @Test
    void testBusyCallIsNotPinged() {
        when(serverCall.idleCount()).thenReturn(0);
        keepAliveComponent.register(serverCall);
        verify(serverCall, timeout(2000).atLeastOnce()).idleCount();
        verify(serverCall, timeout(2000).times(0)).ping();
    }

    @Test
    void testIdleCallIsPinged() {
        when(serverCall.idleCount()).thenReturn(6);
        keepAliveComponent.register(serverCall);
        verify(serverCall, timeout(2000)).idleCount();
        verify(serverCall, timeout(2000)).ping();
    }

    @Test
    void testCallCanBeRemoved() {
        when(serverCall.idleCount()).thenReturn(6);
        CallRegistry.Removable handback = keepAliveComponent.register(serverCall);
        verify(serverCall, timeout(2000)).idleCount();
        verify(serverCall, timeout(2000)).ping();
        handback.remove();
        verifyNoMoreInteractions(serverCall);
    }

}

