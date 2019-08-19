package com.bakdata.profilestore.core;

import com.bakdata.profilestore.core.avro.ChartTuple;
import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.avro.UserProfile;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.apache.kafka.streams.state.KeyValueStore;
import org.hamcrest.MatcherAssert;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UserProfileTest extends TopologyBaseTest {

    @Test
    void testSingleUser() {
        final Instant firstInstant = java.time.Instant.now().truncatedTo(ChronoUnit.MILLIS);

        this.testTopology.input("listening-events")
                .add(1L, new ListeningEvent(1L, 1L, 1L, 1L, firstInstant.plusSeconds(20)))
                .add(1L, new ListeningEvent(1L, 1L, 1L, 2L, firstInstant.plusSeconds(25)))
                .add(1L, new ListeningEvent(1L, 1L, 1L, 3L, firstInstant.plusSeconds(18)))
                .add(1L, new ListeningEvent(1L, 2L, 10L, 10L, firstInstant.plusSeconds(30)))
                .add(1L, new ListeningEvent(1L, 1L, 2L, 4L, firstInstant.plusSeconds(5)))
                .add(1L, new ListeningEvent(1L, 2L, 10L, 25L, firstInstant.plusSeconds(12)))
                .add(1L, new ListeningEvent(1L, 2L, 10L, 11L, firstInstant.plusSeconds(521)))
                .add(1L, new ListeningEvent(1L, 1L, 1L, 1L, firstInstant.plusSeconds(235)))
                .add(1L, new ListeningEvent(1L, 1L, 1L, 1L, firstInstant.plusSeconds(346)));

        final KeyValueStore<Long, UserProfile> profileStore =
                this.testTopology.getTestDriver().getKeyValueStore(ProfilestoreMain.PROFILE_STORE_NAME);
        final UserProfile userProfile = profileStore.get(1L);

        Assertions.assertEquals(firstInstant.plusSeconds(5), userProfile.getFirstListeningEvent());
        Assertions.assertEquals(firstInstant.plusSeconds(521), userProfile.getLastListeningEvent());

        MatcherAssert.assertThat(userProfile.getTopTenAlbums(),
                IsIterableContainingInOrder
                        .contains(new ChartTuple(1L, 5L),
                                new ChartTuple(10L, 3L),
                                new ChartTuple(2L, 1L)));

        MatcherAssert.assertThat(userProfile.getTopTenArtist(),
                IsIterableContainingInOrder
                        .contains(new ChartTuple(1L, 6L),
                                new ChartTuple(2L, 3L)));

        Assertions.assertEquals(new ChartTuple(1L, 3L), userProfile.getTopTenTracks().get(0));

        MatcherAssert.assertThat(userProfile.getTopTenTracks().subList(1, userProfile.getTopTenTracks().size()),
                IsIterableContainingInAnyOrder.containsInAnyOrder(
                        new ChartTuple(2L, 1L),
                        new ChartTuple(3L, 1L),
                        new ChartTuple(10L, 1L),
                        new ChartTuple(4L, 1L),
                        new ChartTuple(25L, 1L),
                        new ChartTuple(11L, 1L)
                ));

    }
}
