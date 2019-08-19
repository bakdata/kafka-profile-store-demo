package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.core.ListeningEventBuilder;
import com.bakdata.profilestore.core.ProfilestoreMain;
import com.bakdata.profilestore.core.TopologyBaseTest;
import com.bakdata.profilestore.core.avro.ChartRecord;
import com.bakdata.profilestore.core.avro.UserProfile;
import java.time.Instant;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ChartProcessorTest extends TopologyBaseTest {
    @Test
    void testAlbumCharts() {
        final ListeningEventBuilder builder = new ListeningEventBuilder();

        builder.setTimestamp(Instant.now())
                .setArtistId(2L);

        this.testTopology.input("listening-events")
                .add(1L, builder.setUserId(1L).setAlbumId(3L).setTrackId(4L).build())
                .add(1L, builder.setUserId(1L).setAlbumId(3L).setTrackId(5L).build())
                .add(1L, builder.setUserId(1L).setAlbumId(2L).setTrackId(4L).build())
                .add(1L, builder.setUserId(1L).setAlbumId(3L).setTrackId(3L).build())
                .add(2L, builder.setUserId(2L).setAlbumId(3L).setTrackId(4L).build());

        final KeyValueStore<Long, UserProfile> chartStore =
                this.testTopology.getTestDriver().getKeyValueStore(ProfilestoreMain.PROFILE_STORE_NAME);

        final ChartRecord first = chartStore.get(1L).getTopTenAlbums().get(0);
        final ChartRecord second = chartStore.get(1L).getTopTenAlbums().get(1);

        Assertions.assertEquals(3, first.getCountPlays());
        Assertions.assertEquals(3L, first.getId());

        Assertions.assertEquals(1, second.getCountPlays());
        Assertions.assertEquals(2L, second.getId());
    }

    @Test
    void testArtistCharts() {
        final ListeningEventBuilder builder = new ListeningEventBuilder();

        builder.setTimestamp(Instant.now())
                .setUserId(1L);

        this.testTopology.input("listening-events")
                .add(1L, builder.setArtistId(2L).setAlbumId(3L).setTrackId(4L).build())
                .add(1L, builder.setArtistId(2L).setAlbumId(3L).setTrackId(5L).build())
                .add(1L, builder.setArtistId(2L).setAlbumId(3L).setTrackId(4L).build())
                .add(1L, builder.setArtistId(2L).setAlbumId(3L).setTrackId(4L).build())
                .add(1L, builder.setArtistId(3L).setAlbumId(4L).setTrackId(5L).build())
                .add(1L, builder.setArtistId(3L).setAlbumId(4L).setTrackId(5L).build())
                .add(1L, builder.setArtistId(2L).setAlbumId(3L).setTrackId(4L).build());

        final KeyValueStore<Long, UserProfile> chartStore =
                this.testTopology.getTestDriver().getKeyValueStore(ProfilestoreMain.PROFILE_STORE_NAME);

        final ChartRecord first = chartStore.get(1L).getTopTenArtist().get(0);
        final ChartRecord second = chartStore.get(1L).getTopTenArtist().get(1);

        Assertions.assertEquals(5, first.getCountPlays());
        Assertions.assertEquals(2L, first.getId());

        Assertions.assertEquals(2, second.getCountPlays());
        Assertions.assertEquals(3L, second.getId());
    }

    @Test
    void testTrackCharts() {
        final ListeningEventBuilder builder = new ListeningEventBuilder();

        builder.setUserId(1L)
                .setArtistId(2L)
                .setAlbumId(3L)
                .setTimestamp(Instant.now());

        this.testTopology.input("listening-events")
                .add(1L, builder.setTrackId(4L).build())
                .add(1L, builder.setTrackId(5L).build())
                .add(1L, builder.setTrackId(4L).build())
                .add(1L, builder.setTrackId(4L).build())
                .add(1L, builder.setTrackId(4L).build());

        final KeyValueStore<Long, UserProfile> chartStore =
                this.testTopology.getTestDriver().getKeyValueStore(ProfilestoreMain.PROFILE_STORE_NAME);

        final ChartRecord first = chartStore.get(1L).getTopTenTracks().get(0);
        final ChartRecord second = chartStore.get(1L).getTopTenTracks().get(1);

        Assertions.assertEquals(4, first.getCountPlays());
        Assertions.assertEquals(4L, first.getId());

        Assertions.assertEquals(1, second.getCountPlays());
        Assertions.assertEquals(5L, second.getId());
    }
}