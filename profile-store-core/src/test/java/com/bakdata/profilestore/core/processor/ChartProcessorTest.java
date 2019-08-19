package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.common.avro.ListeningEvent;
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
        this.testTopology.input("listening-events")
                .add(1L, new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()))
                .add(1L, new ListeningEvent(1L, 2L, 3L, 5L, Instant.now()))
                .add(1L, new ListeningEvent(1L, 2L, 2L, 4L, Instant.now()))
                .add(1L, new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()))
                .add(2L, new ListeningEvent(2L, 2L, 3L, 4L, Instant.now()));

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
        this.testTopology.input("listening-events")
                .add(1L, new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()))
                .add(1L, new ListeningEvent(1L, 2L, 3L, 5L, Instant.now()))
                .add(1L, new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()))
                .add(1L, new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()))
                .add(1L, new ListeningEvent(1L, 3L, 4L, 5L, Instant.now()))
                .add(1L, new ListeningEvent(1L, 3L, 4L, 5L, Instant.now()))
                .add(1L, new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()));

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
        this.testTopology.input("listening-events")
                .add(1L, new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()))
                .add(1L, new ListeningEvent(1L, 2L, 3L, 5L, Instant.now()))
                .add(1L, new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()))
                .add(1L, new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()))
                .add(1L, new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()));

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