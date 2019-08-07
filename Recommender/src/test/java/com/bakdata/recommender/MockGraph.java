package com.bakdata.recommender;

import com.bakdata.recommender.graph.BipartiteGraph;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class MockGraph implements BipartiteGraph {
    private final Map<Long, List<Long>> leftIndex;
    private final Map<Long, List<Long>> rightIndex;

    public MockGraph(final Map<Long, List<Long>> leftIndex, final Map<Long, List<Long>> rightIndex) {
        this.leftIndex = leftIndex;
        this.rightIndex = rightIndex;
    }

    @Override
    public List<Long> getLeftNodeNeighbors(final long leftNodeId) {
        return this.leftIndex.get(leftNodeId);
    }

    @Override
    public long getLeftNodeDegree(final long leftNodeId) {
        return this.getLeftNodeNeighbors(leftNodeId).size();
    }

    @Override
    public List<Long> getRightNodeNeighbors(final long rightNodeId) {
        return this.rightIndex.get(rightNodeId);
    }

    @Override
    public long getRightNodeDegree(final long rightNodeId) {
        return this.getRightNodeNeighbors(rightNodeId).size();
    }

    public static MockGraph fromLeftIndex(final Map<Long, List<Long>> leftIndex) {
        return new MockGraph(leftIndex, fillIndex(leftIndex));
    }

    public static MockGraph fromRightIndex(final Map<Long, List<Long>> rightIndex) {
        return new MockGraph(fillIndex(rightIndex), rightIndex);
    }

    private static Map<Long, List<Long>> fillIndex(final Map<Long, List<Long>> givenIndex) {
        final Map<Long, List<Long>> newIndex = new HashMap<>(givenIndex.size());
        for (final Entry<Long, List<Long>> entry : givenIndex.entrySet()) {
            for (final long value : entry.getValue()) {
                final List<Long> neighbors = newIndex.getOrDefault(value, new LinkedList<Long>());
                neighbors.add(entry.getKey());
                newIndex.putIfAbsent(value, neighbors);
            }
        }
        return newIndex;
    }

    @Override
    public String toString() {
        return this.leftIndex + "\n" + this.rightIndex;
    }
}
