package org.falland.grpc.longlivedstreams.examples.client;

import com.falland.gprc.longlivedstreams.proto.helloworld.v1.World;
import org.falland.grpc.longlivedstreams.client.UpdateProcessor;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class WorldUpdateProcessor implements UpdateProcessor<World> {

    private static final ThreadLocalRandom rnd = ThreadLocalRandom.current();

    private final List<World> messages = new CopyOnWriteArrayList<>();
    @Override
    public void processUpdate(World update) {
        messages.add(update);
        try {
            TimeUnit.MICROSECONDS.sleep(rnd.nextInt(50) * 10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void awaitOnComplete(int messagesCount, Duration maxAwaitTime) throws InterruptedException {
        Instant endTime = Instant.now().plusNanos(maxAwaitTime.toNanos());
        while (messages.size() < messagesCount && Instant.now().isBefore(endTime)) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
    }

    public List<World> getMessages() {
        return List.copyOf(messages);
    }
}
