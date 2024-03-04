package org.falland.grpc.longlivedstreams.core.streams;

import org.falland.grpc.longlivedstreams.core.ControlledStreamObserver;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class ControlledStreamObserverTest {

    @SuppressWarnings("unchecked")
    private final Consumer<Long> updatesConsumer = mock(Consumer.class);
//    private final ControlledStreamObserver<Long> underTest = getTestGrpcSubscription();

//    @Test
//    public void testFilteringSubscription_shouldFilter() {
//        ControlledStreamObserver<Long> filtered = underTest.withFilter(number -> number > 100);
//        filtered.onNext(100L);
//        filtered.onNext(101L);
//
//        Mockito.verify(updatesConsumer).accept(101L);
//        Mockito.verify(updatesConsumer, Mockito.never()).accept(100L);
//    }
//
//    @Test
//    public void testTransformingSubscription_shouldTransform() {
//        ControlledStreamObserver<String> filtered = underTest.withTransformation(Long::parseLong);
//        filtered.onNext("100");
//        filtered.onNext("101");
//
//        Mockito.verify(updatesConsumer).accept(100L);
//        Mockito.verify(updatesConsumer).accept(101L);
//    }
//
//    @Test
//    public void testTransformingSubscription_shouldThrowException_whenTransformerFails() {
//        ControlledStreamObserver<String> filtered = underTest.withTransformation(Long::parseLong);
//        assertThrows(TransformingStreamObserver.TransformationException.class, () -> filtered.onNext("aaa"));
//    }
//
//    @Test
//    public void testTransformingSubscription_shouldThrowException_whenTransformerReturnsNull() {
//        ControlledStreamObserver<String> filtered = underTest.withTransformation(string -> null);
//        assertThrows(TransformingStreamObserver.TransformationException.class, () -> filtered.onNext("aaa"));
//    }
//
//    private ControlledStreamObserver<Long> getTestGrpcSubscription() {
//        return new ControlledStreamObserver<>() {
//            @Override
//            public void onNext(Long message) {
//                updatesConsumer.accept(message);
//            }
//
//            @Override
//            public boolean isOpened() {
//                return false;
//            }
//
//            @Override
//            public void onError(Throwable t) {
//
//            }
//
//            @Override
//            public void onCompleted() {
//
//            }
//        };
//    }
}
