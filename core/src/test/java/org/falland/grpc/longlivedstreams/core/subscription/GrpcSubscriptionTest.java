package org.falland.grpc.longlivedstreams.core.subscription;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class GrpcSubscriptionTest {

    @SuppressWarnings("unchecked")
    private final Consumer<Long> updatesConsumer = mock(Consumer.class);
    private final GrpcSubscription<Long> underTest = getTestGrpcSubscription();

    @Test
    public void testFilteringSubscription_shouldFilter() {
        GrpcSubscription<Long> filtered = underTest.withFilter(number -> number > 100);
        filtered.processUpdate(100L);
        filtered.processUpdate(101L);

        Mockito.verify(updatesConsumer).accept(101L);
        Mockito.verify(updatesConsumer, Mockito.never()).accept(100L);
    }

    @Test
    public void testTransformingSubscription_shouldTransform() {
        GrpcSubscription<String> filtered = underTest.withTransformation(Long::parseLong);
        filtered.processUpdate("100");
        filtered.processUpdate("101");

        Mockito.verify(updatesConsumer).accept(100L);
        Mockito.verify(updatesConsumer).accept(101L);
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
            public SubscriptionType type() {
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
