package com.bakdata.recommender;

import com.bakdata.recommender.avro.AdjacencyList;
import com.bakdata.recommender.avro.ListeningEvent;
import com.bakdata.recommender.graph.WritableKeyValueGraph;
import com.bakdata.recommender.graph.WriteableBipartiteGraph;
import java.util.EnumMap;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Processor updates the left and right index for the random walks
 */
public class RecommenderProcessor implements Processor<byte[], ListeningEvent> {
    private final EnumMap<RecommendationType, WriteableBipartiteGraph> graphs =
            new EnumMap<>(RecommendationType.class);

    @Override
    public void init(final ProcessorContext processorContext) {
        for (final RecommendationType type : RecommendationType.values()) {
            final KeyValueStore<Long, AdjacencyList> leftIndex = (KeyValueStore<Long, AdjacencyList>) processorContext
                    .getStateStore(type.getLeftIndexName());
            final KeyValueStore<Long, AdjacencyList> rightIndex = (KeyValueStore<Long, AdjacencyList>) processorContext
                    .getStateStore(type.getRightIndexName());

            final WriteableBipartiteGraph graph = new WritableKeyValueGraph(leftIndex, rightIndex);

            this.graphs.put(type, graph);
        }
    }

    @Override
    public void process(final byte[] bytes, final ListeningEvent event) {
        this.graphs.get(RecommendationType.ALBUM).addEdge(event.getUserId(), event.getAlbumId());
        this.graphs.get(RecommendationType.ARTIST).addEdge(event.getUserId(), event.getArtistId());
        this.graphs.get(RecommendationType.TRACK).addEdge(event.getUserId(), event.getTrackId());
    }

    @Override
    public void close() {
    }


}