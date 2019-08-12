package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.common.FieldType;
import com.bakdata.profilestore.core.TopKList;
import com.bakdata.profilestore.core.avro.ChartTuple;
import java.time.Instant;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TopKProcessorTest extends ProcessorBaseTest {
    @Test
    void testAlbumCharts() {
        this.testTopology.input()
                .add(new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()))
                .add(new ListeningEvent(1L, 2L, 3L, 5L, Instant.now()))
                .add(new ListeningEvent(1L, 2L, 2L, 4L, Instant.now()))
                .add(new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()))
                .add(new ListeningEvent(2L, 2L, 3L, 4L, Instant.now()));

        final KeyValueStore<Long, TopKList> chartStore =
                this.testTopology.getTestDriver().getKeyValueStore(TopKProcessor.getChartStoreName(FieldType.ALBUM));

        final ChartTuple first = chartStore.get(1L).values().get(0);
        final ChartTuple second = chartStore.get(1L).values().get(1);

        Assertions.assertEquals(3, first.getCountPlays());
        Assertions.assertEquals(3L, first.getId());

        Assertions.assertEquals(1, second.getCountPlays());
        Assertions.assertEquals(2L, second.getId());
    }

    @Test
    void testArtistCharts() {
        this.testTopology.input()
                .add(new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()))
                .add(new ListeningEvent(1L, 2L, 3L, 5L, Instant.now()))
                .add(new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()))
                .add(new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()))
                .add(new ListeningEvent(1L, 3L, 4L, 5L, Instant.now()))
                .add(new ListeningEvent(1L, 3L, 4L, 5L, Instant.now()))
                .add(new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()));

        final KeyValueStore<Long, TopKList> chartStore =
                this.testTopology.getTestDriver().getKeyValueStore(TopKProcessor.getChartStoreName(FieldType.ARTIST));

        final ChartTuple first = chartStore.get(1L).values().get(0);
        final ChartTuple second = chartStore.get(1L).values().get(1);

        Assertions.assertEquals(5, first.getCountPlays());
        Assertions.assertEquals(2L, first.getId());

        Assertions.assertEquals(2, second.getCountPlays());
        Assertions.assertEquals(3L, second.getId());
    }

    @Test
    void testTrackCharts() {
        this.testTopology.input()
                .add(new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()))
                .add(new ListeningEvent(1L, 2L, 3L, 5L, Instant.now()))
                .add(new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()))
                .add(new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()))
                .add(new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()));

        final KeyValueStore<Long, TopKList> chartStore =
                this.testTopology.getTestDriver().getKeyValueStore(TopKProcessor.getChartStoreName(FieldType.TRACK));

        final ChartTuple first = chartStore.get(1L).values().get(0);
        final ChartTuple second = chartStore.get(1L).values().get(1);

        Assertions.assertEquals(4, first.getCountPlays());
        Assertions.assertEquals(4L, first.getId());

        Assertions.assertEquals(1, second.getCountPlays());
        Assertions.assertEquals(5L, second.getId());
    }


    @Test
    void testMultipleUser() {
        this.testTopology.input()
                .add(new ListeningEvent(1L, 2L, 9L, 4L, Instant.now()))
                .add(new ListeningEvent(2L, 5L, 3L, 5L, Instant.now()))
                .add(new ListeningEvent(1L, 3L, 5L, 12L, Instant.now()))
                .add(new ListeningEvent(4L, 2L, 1L, 6L, Instant.now()))
                .add(new ListeningEvent(1L, 2L, 12L, 4L, Instant.now()))
                .add(new ListeningEvent(4L, 1L, 2L, 8L, Instant.now()))
                .add(new ListeningEvent(3L, 5L, 3L, 7L, Instant.now()));

    }


}