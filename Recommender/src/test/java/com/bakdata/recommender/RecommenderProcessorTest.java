package com.bakdata.recommender;

import com.bakdata.fluent_kafka_streams_tests.TestInput;
import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import com.bakdata.recommender.avro.ListeningEvent;
import com.bakdata.recommender.graph.BipartiteGraph;
import com.bakdata.recommender.graph.KeyValueGraph;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.RegisterExtension;

class RecommenderProcessorTest {
    private final RecommenderMain main = new RecommenderMain();

    @RegisterExtension
    final TestTopologyExtension<String, ListeningEvent> testTopology =
            new TestTopologyExtension<String, ListeningEvent>(this.main::buildTopology, this.main.getProperties());


    @Test
    public void testAlbumSingleInput() {
        this.testTopology.input()
                .add(new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()));
        EnumMap<RecommendationType, BipartiteGraph> graphMap = this.getGraphMap();

        Assertions.assertEquals(Collections.singletonList(3L),
                graphMap.get(RecommendationType.ALBUM).getLeftNodeNeighbors(1));
        Assertions.assertEquals(Collections.singletonList(1L),
                graphMap.get(RecommendationType.ALBUM).getRightNodeNeighbors(3));
    }

    @Test
    public void testArtistSingleInput() {
        this.testTopology.input()
                .add(new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()));
        EnumMap<RecommendationType, BipartiteGraph> graphMap = this.getGraphMap();

        Assertions.assertEquals(Collections.singletonList(2L),
                graphMap.get(RecommendationType.ALBUM).getLeftNodeNeighbors(1));
        Assertions.assertEquals(Collections.singletonList(1L),
                graphMap.get(RecommendationType.ALBUM).getRightNodeNeighbors(2));
    }

    @Test
    public void testTrackSingleInput() {
        this.testTopology.input()
                .add(new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()));
        EnumMap<RecommendationType, BipartiteGraph> graphMap = this.getGraphMap();

        Assertions.assertEquals(Collections.singletonList(4L),
                graphMap.get(RecommendationType.ALBUM).getLeftNodeNeighbors(1));
        Assertions.assertEquals(Collections.singletonList(1L),
                graphMap.get(RecommendationType.ALBUM).getRightNodeNeighbors(4));
    }

    @Test
    public void testMultipleInputs() {
        long[] users = {1, 5, 1, 6, 1};
        long[] artists = {2, 3, 4, 5, 2};
        long[] album = {3, 3, 4, 5, 6};
        long[] track = {4, 8, 2, 8, 7};

        TestInput<String, ListeningEvent> testInput = this.testTopology.input();

        for (int i = 0; i < users.length; i++) {
            testInput.add(new ListeningEvent(users[i], artists[i], album[i], track[i], Instant.now()));
        }

        EnumMap<RecommendationType, BipartiteGraph> graphMap = this.getGraphMap();

        Assertions.assertEquals(Arrays.asList(3L, 4L, 6L),
                graphMap.get(RecommendationType.ALBUM).getLeftNodeNeighbors(1));
        Assertions.assertEquals(Collections.singletonList(1L),
                graphMap.get(RecommendationType.ALBUM).getRightNodeNeighbors(4));

        Assertions.assertEquals(Arrays.asList(2L, 4L, 2L),
                graphMap.get(RecommendationType.ARTIST).getLeftNodeNeighbors(1));
        Assertions.assertEquals(Arrays.asList(1L, 1L),
                graphMap.get(RecommendationType.ARTIST).getRightNodeNeighbors(2));

        Assertions.assertEquals(Arrays.asList(4L, 2L, 7L),
                graphMap.get(RecommendationType.TRACK).getLeftNodeNeighbors(1));
        Assertions.assertEquals(Arrays.asList(5L, 6L),
                graphMap.get(RecommendationType.TRACK).getRightNodeNeighbors(8));

    }

    public EnumMap<RecommendationType, BipartiteGraph> getGraphMap() {
        EnumMap<RecommendationType, BipartiteGraph> graphs =
                new EnumMap<>(RecommendationType.class);
        for (RecommendationType type : RecommendationType.values()) {
            KeyValueGraph graph = new KeyValueGraph(
                    this.testTopology.getTestDriver().getKeyValueStore(type.getLeftIndexName()),
                    this.testTopology.getTestDriver().getKeyValueStore(type.getRightIndexName())
            );
            graphs.put(type, graph);
        }
        return graphs;
    }


}