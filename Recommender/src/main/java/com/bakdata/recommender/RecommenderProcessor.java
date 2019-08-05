package com.bakdata.recommender;

import com.bakdata.recommender.avro.AdjacencyList;
import com.bakdata.recommender.avro.ListeningEvent;
import com.bakdata.recommender.graph.WritableKeyValueGraph;
import java.util.EnumMap;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Processor updates the left and right index for the random walks
 */
public class RecommenderProcessor extends AbstractProcessor<byte[], ListeningEvent> {
    private final EnumMap<RecommendationType, WritableKeyValueGraph> graphs =
            new EnumMap<RecommendationType, WritableKeyValueGraph>(RecommendationType.class);

    @Override
    public void init(ProcessorContext processorContext) {
        super.init(processorContext);
        for (RecommendationType type : RecommendationType.values()) {
            WritableKeyValueGraph graph = new WritableKeyValueGraph(
                    (KeyValueStore<Long, AdjacencyList>) processorContext
                            .getStateStore(type.getLeftIndexName()),
                    (KeyValueStore<Long, AdjacencyList>) processorContext
                            .getStateStore(type.getRightIndexName())
            );

            this.graphs.put(type, graph);
        }
    }

    @Override
    public void process(byte[] bytes, ListeningEvent event) {
        this.graphs.get(RecommendationType.ALBUM).addEdge(event.getUserId(), event.getAlbumId());
        this.graphs.get(RecommendationType.ARTIST).addEdge(event.getUserId(), event.getArtistId());
        this.graphs.get(RecommendationType.TRACK).addEdge(event.getUserId(), event.getAlbumId());
    }


}