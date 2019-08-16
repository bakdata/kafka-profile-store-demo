package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.common.avro.ListeningEvent;
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
        Stream.generate(() ->
                new ListeningEvent(1L, ThreadLocalRandom.current().nextLong(),
                        ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong(),
                        Instant.now()))
                .limit(50L)
                .forEach(event -> this.testTopology.input("listening-events").add(1L, event));

        final KeyValueStore<Long, UserProfile> profileStore =
                this.testTopology.getTestDriver().getKeyValueStore(ProfilestoreMain.PROFILE_STORE_NAME);

        Assertions.assertEquals(50L, profileStore.get(1L).getEventCount());
    }

}