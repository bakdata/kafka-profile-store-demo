package com.bakdata.recommender.graph;

import com.bakdata.recommender.avro.AdjacencyList;
import java.util.Collections;
import org.apache.kafka.streams.state.KeyValueStore;

public class WritableKeyValueGraph extends KeyValueGraph implements WriteableBipartiteGraph {
    private final KeyValueStore<Long, AdjacencyList> leftIndex;
    private final KeyValueStore<Long, AdjacencyList> rightIndex;

    public WritableKeyValueGraph(KeyValueStore<Long, AdjacencyList> leftIndex,
            KeyValueStore<Long, AdjacencyList> rightIndex) {
        super(leftIndex, rightIndex);
        this.leftIndex = leftIndex;
        this.rightIndex = rightIndex;
    }

    @Override
    public void addEdge(long leftNodeId, long rightNodeId) {
        this.updateIndex(leftNodeId, rightNodeId, this.leftIndex);
        this.updateIndex(rightNodeId, leftNodeId, this.rightIndex);
    }

    /**
     * Adds new interaction to respective index
     *
     * @param keyId id of the key node
     * @param valueId id of the value node
     * @param index key value store index
     */
    private void updateIndex(long keyId, long valueId, KeyValueStore<Long, AdjacencyList> index) {
        AdjacencyList adjacencyList = this.getAdjacencyList(keyId, valueId, index);
        index.put(keyId, adjacencyList);
    }

    private AdjacencyList getAdjacencyList(Long leftId, Long rightId, KeyValueStore<Long, AdjacencyList> index) {
        AdjacencyList currentNeighbors = index.get(leftId);
        if (currentNeighbors == null) {
            currentNeighbors = new AdjacencyList(Collections.singletonList(rightId));
        } else {
            currentNeighbors.getNeighbors().add(rightId);
        }
        return currentNeighbors;
    }

}
