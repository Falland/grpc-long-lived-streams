package org.falland.grpc.longlivedstreams.client.streaming;

import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OnReadyHandlerForwarderTest {

    private final Runnable onReadyHandler = mock(Runnable.class);

    private final OnReadyHandlerForwarder underTest = new OnReadyHandlerForwarder();


    @Test
    public void testRun_shouldRunHandler_whenAccepted() {
        underTest.accept(onReadyHandler);

        underTest.run();
        verify(onReadyHandler).run();
    }

    @Test
    public void testRun_shouldNotFail_whenNotAccepted() {
        underTest.run();
    }
}