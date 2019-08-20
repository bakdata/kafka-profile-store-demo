package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.core.ListeningEventBuilder;
import com.bakdata.profilestore.core.ProfilestoreMain;
import com.bakdata.profilestore.core.TopologyBaseTest;
import com.bakdata.profilestore.core.avro.UserProfile;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class EventCountProcessorTest extends TopologyBaseTest {
    @Test
    void testCount() {
        final ListeningEventBuilder builder = new ListeningEventBuilder();
        builder.setUserId(1L);

        Stream.generate(() ->
                builder
                        .setArtistId(ThreadLocalRandom.current().nextLong())
                        .setAlbumId(ThreadLocalRandom.current().nextLong())
                        .setTrackId(ThreadLocalRandom.current().nextLong())
                        .setTimestamp(Instant.now())
                        .build())
                .limit(50L)
                .forEach(event -> this.testTopology.input(INPUT_TOPIC).add(event.getUserId(), event));

        final KeyValueStore<Long, UserProfile> profileStore =
                this.testTopology.getTestDriver().getKeyValueStore(ProfilestoreMain.PROFILE_STORE_NAME);

        Assertions.assertEquals(50L, profileStore.get(1L).getEventCount());
    }

}