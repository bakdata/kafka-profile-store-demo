package com.bakdata.profilestore.recommender;

import com.bakdata.profilestore.common.FieldType;
import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.recommender.avro.AdjacencyList;
import com.bakdata.profilestore.recommender.graph.WritableKeyValueGraph;
import com.bakdata.profilestore.recommender.graph.WriteableBipartiteGraph;
import java.util.EnumMap;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Processor updates the left and right index for the random walks
 */
public class RecommenderProcessor implements Processor<byte[], ListeningEvent> {
    private final EnumMap<FieldType, WriteableBipartiteGraph> graphs =
            new EnumMap<>(FieldType.class);

    @Override
    public void init(final ProcessorContext processorContext) {
        for (final FieldType type : FieldType.values()) {
            final KeyValueStore<Long, AdjacencyList> leftIndex = (KeyValueStore<Long, AdjacencyList>) processorContext
                    .getStateStore(getLeftIndexName(type));
            final KeyValueStore<Long, AdjacencyList> rightIndex = (KeyValueStore<Long, AdjacencyList>) processorContext
                    .getStateStore(getRightIndexName(type));

            final WriteableBipartiteGraph graph = new WritableKeyValueGraph(leftIndex, rightIndex);

            this.graphs.put(type, graph);
        }
    }

    @Override
    public void process(final byte[] bytes, final ListeningEvent event) {
        this.graphs.get(FieldType.ALBUM).addEdge(event.getUserId(), event.getAlbumId());
        this.graphs.get(FieldType.ARTIST).addEdge(event.getUserId(), event.getArtistId());
        this.graphs.get(FieldType.TRACK).addEdge(event.getUserId(), event.getTrackId());
    }

    @Override
    public void close() {
    }


    public static String getLeftIndexName(final FieldType fieldType) {
        return String.format("%s_%s", fieldType, RecommenderMain.LEFT_INDEX_NAME);
    }

    public static String getRightIndexName(final FieldType fieldType) {
        return String.format("%s_%s", fieldType, RecommenderMain.RIGHT_INDEX_NAME);
    }
}