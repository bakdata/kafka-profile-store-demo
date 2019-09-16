package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.core.ListeningEventBuilder;
import com.bakdata.profilestore.core.ProfileStoreMain;
import com.bakdata.profilestore.core.TopologyBaseTest;
import com.bakdata.profilestore.core.avro.NamedChartRecord;
import com.bakdata.profilestore.core.avro.UserProfile;
import java.time.Instant;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ChartProcessorTest extends TopologyBaseTest {
    @Test
    void testAlbumCharts() {
        final ListeningEventBuilder builder = new ListeningEventBuilder();

        builder.setTimestamp(Instant.now())
                .setArtistId(2L);

        this.testTopology.input(INPUT_TOPIC)
                .add(1L, builder.setUserId(1L).setAlbumId(3L).setTrackId(4L).build())
                .add(1L, builder.setUserId(1L).setAlbumId(3L).setTrackId(5L).build())
                .add(1L, builder.setUserId(1L).setAlbumId(2L).setTrackId(4L).build())
                .add(1L, builder.setUserId(1L).setAlbumId(3L).setTrackId(3L).build())
                .add(2L, builder.setUserId(2L).setAlbumId(3L).setTrackId(4L).build());

        final KeyValueStore<Long, UserProfile> chartStore =
                this.testTopology.getTestDriver().getKeyValueStore(ProfileStoreMain.PROFILE_STORE_NAME);

        final NamedChartRecord first = chartStore.get(1L).getTopTenAlbums().get(0);
        final NamedChartRecord second = chartStore.get(1L).getTopTenAlbums().get(1);

        final NamedChartRecord expectedFirst =
                NamedChartRecord.newBuilder().setId(3).setCountPlays(3).setName("ALBUM_3").build();
        final NamedChartRecord expectedSecond =
                NamedChartRecord.newBuilder().setId(2).setCountPlays(1).setName("ALBUM_2").build();

        Assertions.assertEquals(expectedFirst, first);
        Assertions.assertEquals(expectedSecond, second);
    }

    @Test
    void testArtistCharts() {
        final ListeningEventBuilder builder = new ListeningEventBuilder();

        builder.setTimestamp(Instant.now())
                .setUserId(1L);

        this.testTopology.input(INPUT_TOPIC)
                .add(1L, builder.setArtistId(2L).setAlbumId(3L).setTrackId(4L).build())
                .add(1L, builder.setArtistId(2L).setAlbumId(3L).setTrackId(5L).build())
                .add(1L, builder.setArtistId(2L).setAlbumId(3L).setTrackId(4L).build())
                .add(1L, builder.setArtistId(2L).setAlbumId(3L).setTrackId(4L).build())
                .add(1L, builder.setArtistId(3L).setAlbumId(4L).setTrackId(5L).build())
                .add(1L, builder.setArtistId(3L).setAlbumId(4L).setTrackId(5L).build())
                .add(1L, builder.setArtistId(2L).setAlbumId(3L).setTrackId(4L).build());

        final KeyValueStore<Long, UserProfile> chartStore =
                this.testTopology.getTestDriver().getKeyValueStore(ProfileStoreMain.PROFILE_STORE_NAME);

        final NamedChartRecord first = chartStore.get(1L).getTopTenArtist().get(0);
        final NamedChartRecord second = chartStore.get(1L).getTopTenArtist().get(1);

        Assertions.assertEquals(5, first.getCountPlays());
        Assertions.assertEquals(2L, first.getId());

        Assertions.assertEquals(2, second.getCountPlays());
        Assertions.assertEquals(3L, second.getId());

        final NamedChartRecord expectedFirst =
                NamedChartRecord.newBuilder().setId(2).setCountPlays(5).setName("ARTIST_2").build();
        final NamedChartRecord expectedSecond =
                NamedChartRecord.newBuilder().setId(3).setCountPlays(2).setName("ARTIST_3").build();

        Assertions.assertEquals(expectedFirst, first);
        Assertions.assertEquals(expectedSecond, second);
    }

    @Test
    void testTrackCharts() {
        final ListeningEventBuilder builder = new ListeningEventBuilder();

        builder.setUserId(1L)
                .setArtistId(2L)
                .setAlbumId(3L)
                .setTimestamp(Instant.now());

        this.testTopology.input(INPUT_TOPIC)
                .add(1L, builder.setTrackId(4L).build())
                .add(1L, builder.setTrackId(5L).build())
                .add(1L, builder.setTrackId(4L).build())
                .add(1L, builder.setTrackId(4L).build())
                .add(1L, builder.setTrackId(4L).build());

        final KeyValueStore<Long, UserProfile> chartStore =
                this.testTopology.getTestDriver().getKeyValueStore(ProfileStoreMain.PROFILE_STORE_NAME);

        final NamedChartRecord first = chartStore.get(1L).getTopTenTracks().get(0);
        final NamedChartRecord second = chartStore.get(1L).getTopTenTracks().get(1);

        final NamedChartRecord expectedFirst =
                NamedChartRecord.newBuilder().setId(4).setCountPlays(4).setName("TRACK_4").build();
        final NamedChartRecord expectedSecond =
                NamedChartRecord.newBuilder().setId(5).setCountPlays(1).setName("TRACK_5").build();

        Assertions.assertEquals(expectedFirst, first);
        Assertions.assertEquals(expectedSecond, second);
    }
}