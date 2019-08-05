package com.bakdata.recommender;

import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import com.bakdata.recommender.avro.ListeningEvent;
import com.bakdata.recommender.graph.KeyValueGraph;
import java.time.Instant;
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
        EnumMap<RecommendationType, KeyValueGraph> graphMap = this.getGraphMap();

        Assertions.assertEquals(Collections.singletonList(3L),
                graphMap.get(RecommendationType.ALBUM).getLeftNodeNeighbors(1));
        Assertions.assertEquals(Collections.singletonList(1L),
                graphMap.get(RecommendationType.ALBUM).getRightNodeNeighbors(3));
    }

    @Test
    public void testArtistSingleInput() {
        this.testTopology.input()
                .add(new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()));
        EnumMap<RecommendationType, KeyValueGraph> graphMap = this.getGraphMap();

        Assertions.assertEquals(Collections.singletonList(2L),
                graphMap.get(RecommendationType.ALBUM).getLeftNodeNeighbors(1));
        Assertions.assertEquals(Collections.singletonList(1L),
                graphMap.get(RecommendationType.ALBUM).getRightNodeNeighbors(2));
    }

    @Test
    public void testTrackSingleInput() {
        this.testTopology.input()
                .add(new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()));
        EnumMap<RecommendationType, KeyValueGraph> graphMap = this.getGraphMap();

        Assertions.assertEquals(Collections.singletonList(4L),
                graphMap.get(RecommendationType.ALBUM).getLeftNodeNeighbors(1));
        Assertions.assertEquals(Collections.singletonList(1L),
                graphMap.get(RecommendationType.ALBUM).getRightNodeNeighbors(4));
    }

    private EnumMap<RecommendationType, KeyValueGraph> getGraphMap() {
        EnumMap<RecommendationType, KeyValueGraph> graphs =
                new EnumMap<RecommendationType, KeyValueGraph>(RecommendationType.class);
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