package com.bakdata.profilestore.recommender.graph;

import java.util.List;

public interface BipartiteGraph {
    List<Long> getLeftNodeNeighbors(long leftNodeId);

    long getLeftNodeDegree(long leftNodeId);

    List<Long> getRightNodeNeighbors(long rightNodeId);

    long getRightNodeDegree(long rightNodeId);
}
