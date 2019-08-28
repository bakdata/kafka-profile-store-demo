package com.bakdata.profilestore.core;

import com.bakdata.profilestore.core.avro.ChartRecord;
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
        final ListeningEventBuilder builder = new ListeningEventBuilder();

        builder.setUserId(1L);
        this.testTopology.input(INPUT_TOPIC)
                .add(1L, builder.setArtistId(1L).setAlbumId(1L)
                        .setTrackId(1L).setTimestamp(firstInstant.plusSeconds(20)).build())
                .add(1L, builder.setArtistId(1L).setAlbumId(1L)
                        .setTrackId(2L).setTimestamp(firstInstant.plusSeconds(25)).build())
                .add(1L, builder.setArtistId(1L).setAlbumId(1L)
                        .setTrackId(3L).setTimestamp(firstInstant.plusSeconds(18)).build())
                .add(1L, builder.setArtistId(2L).setAlbumId(10L)
                        .setTrackId(10L).setTimestamp(firstInstant.plusSeconds(30)).build())
                .add(1L, builder.setArtistId(1L).setAlbumId(2L)
                        .setTrackId(4L).setTimestamp(firstInstant.plusSeconds(5)).build())
                .add(1L, builder.setArtistId(2L).setAlbumId(10L)
                        .setTrackId(25L).setTimestamp(firstInstant.plusSeconds(12)).build())
                .add(1L, builder.setArtistId(2L).setAlbumId(10L)
                        .setTrackId(11L).setTimestamp(firstInstant.plusSeconds(52)).build())
                .add(1L, builder.setArtistId(1L).setAlbumId(1L)
                        .setTrackId(1L).setTimestamp(firstInstant.plusSeconds(235)).build())
                .add(1L, builder.setArtistId(1L).setAlbumId(1L)
                        .setTrackId(1L).setTimestamp(firstInstant.plusSeconds(346)).build());

        final KeyValueStore<Long, UserProfile> profileStore =
                this.testTopology.getTestDriver().getKeyValueStore(ProfilestoreMain.PROFILE_STORE_NAME);
        final UserProfile userProfile = profileStore.get(1L);

        Assertions.assertEquals(firstInstant.plusSeconds(5), userProfile.getFirstListeningEvent());
        Assertions.assertEquals(firstInstant.plusSeconds(346), userProfile.getLastListeningEvent());

        MatcherAssert.assertThat(namedToUnnamedRecord(userProfile.getTopTenAlbums()),
                IsIterableContainingInOrder
                        .contains(new ChartRecord(1L, 5L),
                                new ChartRecord(10L, 3L),
                                new ChartRecord(2L, 1L)));

        MatcherAssert.assertThat(namedToUnnamedRecord(userProfile.getTopTenArtist()),
                IsIterableContainingInOrder
                        .contains(new ChartRecord(1L, 6L),
                                new ChartRecord(2L, 3L)));

        Assertions.assertEquals(new ChartRecord(1L, 3L), namedToUnnamedRecord(userProfile.getTopTenTracks()).get(0));

        MatcherAssert.assertThat(
                namedToUnnamedRecord(userProfile.getTopTenTracks().subList(1, userProfile.getTopTenTracks().size())),
                IsIterableContainingInAnyOrder.containsInAnyOrder(
                        new ChartRecord(2L, 1L),
                        new ChartRecord(3L, 1L),
                        new ChartRecord(10L, 1L),
                        new ChartRecord(4L, 1L),
                        new ChartRecord(25L, 1L),
                        new ChartRecord(11L, 1L)
                ));
    }
}
