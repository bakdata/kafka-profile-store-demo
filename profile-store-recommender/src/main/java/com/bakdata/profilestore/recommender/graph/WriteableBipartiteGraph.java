package com.bakdata.profilestore.recommender.graph;

public interface WriteableBipartiteGraph extends BipartiteGraph {
    void addEdge(long leftNodeId, long rightNodeId);
}
