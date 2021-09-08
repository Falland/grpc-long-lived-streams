package io.grpc.longlivedstreams.server.subscription;

import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class GrpcSubscriptionTest {

    private final Consumer<Long> updatesConsumer = mock(Consumer.class);
    private final GrpcSubscription<Long> underTest = getTestGrpcSubscription();

    @Test
    public void testFilteringSubscription_shouldFilter() {
        GrpcSubscription<Long> filtered = underTest.withFilter(number -> number > 100);
        filtered.processUpdate(100L);
        filtered.processUpdate(101L);

        verify(updatesConsumer).accept(101L);
        verify(updatesConsumer, never()).accept(100L);
    }

    @Test
    public void testTransformingSubscription_shouldTransform() {
        GrpcSubscription<String> filtered = underTest.withTransformation(Long::parseLong);
        filtered.processUpdate("100");
        filtered.processUpdate("101");

        verify(updatesConsumer).accept(100L);
        verify(updatesConsumer).accept(101L);
    }

    @Test
    public void testTransformingSubscription_shouldThrowException_whenTransformerFails() {
        GrpcSubscription<String> filtered = underTest.withTransformation(Long::parseLong);
        assertThrows(TransformingGrpcSubscription.TransformationException.class, () -> filtered.processUpdate("aaa"));
    }

    @Test
    public void testTransformingSubscription_shouldThrowException_whenTransformerReturnsNull() {
        GrpcSubscription<String> filtered = underTest.withTransformation(string -> null);
        assertThrows(TransformingGrpcSubscription.TransformationException.class, () -> filtered.processUpdate("aaa"));
    }

    private GrpcSubscription<Long> getTestGrpcSubscription() {
        return new GrpcSubscription<>() {
            @Override
            public String getAddress() {
                return null;
            }

            @Override
            public SubscriptionType getType() {
                return null;
            }

            @Override
            public String getClientId() {
                return null;
            }

            @Override
            public void processUpdate(Long update) {
                updatesConsumer.accept(update);
            }

            @Override
            public boolean isActive() {
                return false;
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

            }
        };
    }
}
