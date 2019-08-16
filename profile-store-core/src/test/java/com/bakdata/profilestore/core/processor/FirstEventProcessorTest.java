package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.core.ProfilestoreMain;
import com.bakdata.profilestore.core.TopologyBaseTest;
import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.avro.UserProfile;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class FirstEventProcessorTest extends TopologyBaseTest {

    @Test
    void testInOrderStream() {
        final Random random = new Random();
        // truncation is necessary because the serialization truncates
        final Instant firstInstant = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        final List<ListeningEvent> timestamps = IntStream.range(0, 20).mapToObj(i ->
                new ListeningEvent(1L, random.nextLong(), random.nextLong(), random.nextLong(),
                        firstInstant.plusSeconds(i))
        ).collect(Collectors.toList());

        timestamps.forEach(event -> this.testTopology.input("listening-events").add(event));

        final KeyValueStore<Long, UserProfile> profileStore =
                this.testTopology.getTestDriver().getKeyValueStore(ProfilestoreMain.PROFILE_STORE_NAME);

        Assertions.assertEquals(firstInstant, profileStore.get(1L).getFirstListeningEvent());
    }

    @Test
    void testOutOfOrderStream() {
        // truncation is necessary because the serialization truncates
        final Instant firstInstant = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        this.testTopology.input("listening-events")
                .add(1L, new ListeningEvent(1L, 2L, 3L, 4L, firstInstant.plusSeconds(20)))
                .add(1L, new ListeningEvent(1L, 2L, 3L, 4L, firstInstant.plusSeconds(25)))
                .add(1L, new ListeningEvent(1L, 2L, 3L, 4L, firstInstant.plusSeconds(18)))
                .add(1L, new ListeningEvent(1L, 2L, 3L, 4L, firstInstant.plusSeconds(30)))
                .add(1L, new ListeningEvent(1L, 2L, 3L, 4L, firstInstant))
                .add(1L, new ListeningEvent(1L, 2L, 3L, 4L, firstInstant.plusSeconds(35)));

        final KeyValueStore<Long, UserProfile> profileStore =
                this.testTopology.getTestDriver().getKeyValueStore(ProfilestoreMain.PROFILE_STORE_NAME);

        Assertions.assertEquals(firstInstant, profileStore.get(1L).getFirstListeningEvent());
    }


}