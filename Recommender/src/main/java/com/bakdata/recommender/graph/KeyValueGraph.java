package com.bakdata.recommender.graph;

import com.bakdata.recommender.avro.AdjacencyList;
import java.util.List;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public class KeyValueGraph implements BipartiteGraph {
    private final ReadOnlyKeyValueStore<Long, AdjacencyList> leftIndex;
    private final ReadOnlyKeyValueStore<Long, AdjacencyList> rightIndex;

    public KeyValueGraph(ReadOnlyKeyValueStore<Long, AdjacencyList> leftIndex,
            ReadOnlyKeyValueStore<Long, AdjacencyList> rightIndex) {
        this.leftIndex = leftIndex;
        this.rightIndex = rightIndex;
    }

    @Override
    public List<Long> getLeftNodeNeighbors(long nodeId) {
        return this.leftIndex.get(nodeId).getNeighbors();
    }

    @Override
    public List<Long> getRightNodeNeighbors(long nodeId) {
        return this.rightIndex.get(nodeId).getNeighbors();
    }

    @Override
    public long getLeftNodeDegree(long leftNodeId) {
        return this.getLeftNodeNeighbors(leftNodeId).size();
    }

    @Override
    public long getRightNodeDegree(long nodeId) {
        return this.getRightNodeNeighbors(nodeId).size();
    }

}
