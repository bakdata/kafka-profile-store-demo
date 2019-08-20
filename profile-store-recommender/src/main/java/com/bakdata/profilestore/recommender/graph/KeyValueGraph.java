package com.bakdata.profilestore.recommender.graph;

import com.bakdata.profilestore.recommender.avro.AdjacencyList;
import java.util.List;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public class KeyValueGraph implements BipartiteGraph {
    private final ReadOnlyKeyValueStore<Long, AdjacencyList> leftIndex;
    private final ReadOnlyKeyValueStore<Long, AdjacencyList> rightIndex;

    public KeyValueGraph(final ReadOnlyKeyValueStore<Long, AdjacencyList> leftIndex,
            final ReadOnlyKeyValueStore<Long, AdjacencyList> rightIndex) {
        this.leftIndex = leftIndex;
        this.rightIndex = rightIndex;
    }

    @Override
    public List<Long> getLeftNodeNeighbors(final long nodeId) {
        return this.leftIndex.get(nodeId).getNeighbors();
    }

    @Override
    public List<Long> getRightNodeNeighbors(final long nodeId) {
        return this.rightIndex.get(nodeId).getNeighbors();
    }

    @Override
    public long getLeftNodeDegree(final long leftNodeId) {
        return this.getLeftNodeNeighbors(leftNodeId).size();
    }

    @Override
    public long getRightNodeDegree(final long nodeId) {
        return this.getRightNodeNeighbors(nodeId).size();
    }

}
