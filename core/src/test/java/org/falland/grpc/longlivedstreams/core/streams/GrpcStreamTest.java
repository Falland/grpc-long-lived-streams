package org.falland.grpc.longlivedstreams.core.streams;

import org.falland.grpc.longlivedstreams.core.GrpcStream;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class GrpcStreamTest {

    @SuppressWarnings("unchecked")
    private final Consumer<Long> updatesConsumer = mock(Consumer.class);
    private final GrpcStream<Long> underTest = getTestGrpcSubscription();

    @Test
    public void testFilteringSubscription_shouldFilter() {
        GrpcStream<Long> filtered = underTest.withFilter(number -> number > 100);
        filtered.onNext(100L);
        filtered.onNext(101L);

        Mockito.verify(updatesConsumer).accept(101L);
        Mockito.verify(updatesConsumer, Mockito.never()).accept(100L);
    }

    @Test
    public void testTransformingSubscription_shouldTransform() {
        GrpcStream<String> filtered = underTest.withTransformation(Long::parseLong);
        filtered.onNext("100");
        filtered.onNext("101");

        Mockito.verify(updatesConsumer).accept(100L);
        Mockito.verify(updatesConsumer).accept(101L);
    }

    @Test
    public void testTransformingSubscription_shouldThrowException_whenTransformerFails() {
        GrpcStream<String> filtered = underTest.withTransformation(Long::parseLong);
        assertThrows(TransformingGrpcStream.TransformationException.class, () -> filtered.onNext("aaa"));
    }

    @Test
    public void testTransformingSubscription_shouldThrowException_whenTransformerReturnsNull() {
        GrpcStream<String> filtered = underTest.withTransformation(string -> null);
        assertThrows(TransformingGrpcStream.TransformationException.class, () -> filtered.onNext("aaa"));
    }

    private GrpcStream<Long> getTestGrpcSubscription() {
        return new GrpcStream<>() {

            @Override
            public StreamType type() {
                return null;
            }

            @Override
            public void onNext(Long update) {
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
