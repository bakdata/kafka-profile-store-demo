package com.bakdata.profilestore.core;

import com.bakdata.profilestore.core.avro.NamedChartRecord;
import com.bakdata.profilestore.core.avro.UserProfile;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.apache.kafka.streams.state.KeyValueStore;
import org.hamcrest.MatcherAssert;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class UserProfileTest extends TopologyBaseTest {
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
                this.testTopology.getTestDriver().getKeyValueStore(ProfileStoreMain.PROFILE_STORE_NAME);
        final UserProfile userProfile = profileStore.get(1L);

        Assertions.assertEquals(firstInstant.plusSeconds(5), userProfile.getFirstListeningEvent());
        Assertions.assertEquals(firstInstant.plusSeconds(346), userProfile.getLastListeningEvent());

        final NamedChartRecord[] expectedAlbumCharts = {
                NamedChartRecord.newBuilder().setId(1).setCountPlays(5).setName("ALBUM_1").build(),
                NamedChartRecord.newBuilder().setId(10).setCountPlays(3).setName("ALBUM_10").build(),
                NamedChartRecord.newBuilder().setId(2).setCountPlays(1).setName("ALBUM_2").build()
        };

        final NamedChartRecord[] expectedArtistCharts = {
                NamedChartRecord.newBuilder().setId(1).setCountPlays(6).setName("ARTIST_1").build(),
                NamedChartRecord.newBuilder().setId(2).setCountPlays(3).setName("ARTIST_2").build()
        };

        final NamedChartRecord[] expectedTrackCharts = {
                NamedChartRecord.newBuilder().setId(2).setCountPlays(1).setName("TRACK_2").build(),
                NamedChartRecord.newBuilder().setId(3).setCountPlays(1).setName("TRACK_3").build(),
                NamedChartRecord.newBuilder().setId(10).setCountPlays(1).setName("TRACK_10").build(),
                NamedChartRecord.newBuilder().setId(4).setCountPlays(1).setName("TRACK_4").build(),
                NamedChartRecord.newBuilder().setId(25).setCountPlays(1).setName("TRACK_25").build(),
                NamedChartRecord.newBuilder().setId(11).setCountPlays(1).setName("TRACK_11").build()
        };

        MatcherAssert
                .assertThat(userProfile.getTopTenAlbums(), IsIterableContainingInOrder.contains(expectedAlbumCharts));

        MatcherAssert.assertThat(userProfile.getTopTenArtist(),
                IsIterableContainingInOrder
                        .contains(expectedArtistCharts));

        Assertions.assertEquals(NamedChartRecord.newBuilder().setId(1).setCountPlays(3).setName("TRACK_1").build(),
                userProfile.getTopTenTracks().get(0));

        MatcherAssert.assertThat(
                userProfile.getTopTenTracks().subList(1, userProfile.getTopTenTracks().size()),
                IsIterableContainingInAnyOrder.containsInAnyOrder(expectedTrackCharts));
    }
}
