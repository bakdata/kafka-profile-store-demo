package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.ListeningEventBuilder;
import com.bakdata.profilestore.core.ProfilestoreMain;
import com.bakdata.profilestore.core.TopologyBaseTest;
import com.bakdata.profilestore.core.avro.UserProfile;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class LastEventProcessorTest extends TopologyBaseTest {
    @Test
    void testInOrderStream() {
        final ListeningEventBuilder builder = new ListeningEventBuilder();
        builder.setUserId(1L);
        // truncation is necessary because the serialization truncates
        final Instant firstInstant = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        final List<ListeningEvent> timestamps = IntStream
                .range(0, 20)
                .mapToObj(i ->
                        builder
                                .setArtistId(ThreadLocalRandom.current().nextLong())
                                .setAlbumId(ThreadLocalRandom.current().nextLong())
                                .setTrackId(ThreadLocalRandom.current().nextLong())
                                .setTimestamp(firstInstant.plusSeconds(i))
                                .build())
                .collect(Collectors.toList());

        timestamps.forEach(event -> this.testTopology.input("listening-events").add(event.getUserId(), event));

        final KeyValueStore<Long, UserProfile> profileStore =
                this.testTopology.getTestDriver().getKeyValueStore(ProfilestoreMain.PROFILE_STORE_NAME);

        Assertions.assertEquals(firstInstant.plusSeconds(19), profileStore.get(1L).getLastListeningEvent());
    }

    @Test
    void testOutOfOrderStream() {
        // truncation is necessary because the serialization truncates
        final Instant firstInstant = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        final ListeningEventBuilder builder = new ListeningEventBuilder();
        builder.setUserId(1L).setArtistId(2L).setAlbumId(3L).setTrackId(4L);

        this.testTopology.input("listening-events")
                .add(1L, builder.setTimestamp(firstInstant.plusSeconds(20)).build())
                .add(1L, builder.setTimestamp(firstInstant.plusSeconds(25)).build())
                .add(1L, builder.setTimestamp(firstInstant.plusSeconds(18)).build())
                .add(1L, builder.setTimestamp(firstInstant.plusSeconds(30)).build())
                .add(1L, builder.setTimestamp(firstInstant).build())
                .add(1L, builder.setTimestamp(firstInstant.plusSeconds(35)).build());
        final KeyValueStore<Long, UserProfile> profileStore =
                this.testTopology.getTestDriver().getKeyValueStore(ProfilestoreMain.PROFILE_STORE_NAME);

        Assertions.assertEquals(firstInstant.plusSeconds(35), profileStore.get(1L).getLastListeningEvent());
    }


}