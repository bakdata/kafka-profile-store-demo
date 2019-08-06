package com.bakdata.recommender.algorithm;

import com.bakdata.recommender.graph.BipartiteGraph;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Salsa {
    private final Logger log = LoggerFactory.getLogger(Salsa.class);
    private final Map<Long, Integer> currentLeftNodeVisits;
    private final Map<Long, Integer> currentRightNodeVisits;
    private final Map<Long, Integer> totalRightNodeVisits;
    private final BipartiteGraph graph;
    private final Random random;

    public Salsa(BipartiteGraph graph, Random random) {
        this.graph = graph;
        this.random = random;
        this.currentLeftNodeVisits = new HashMap<>();
        this.currentRightNodeVisits = new HashMap<>();
        this.totalRightNodeVisits = new HashMap<>();
    }

    /**
     * Computes a list of recommendations for an id
     *
     * @param rootNode the id of the user the recommendations are made for
     * @param walks number of random walks in the monte carlo simulation
     * @param length number of steps in a random walk
     * @param resetProbability probability to jump back to query node * @param limit number of recommendations
     * @return List of size limit with ids for recommendations as elements
     */
    public List<Long> compute(long rootNode, int walks, int length, double resetProbability, int limit) {
        boolean isLeftToRight = true;

        // Initialize seed set on left side
        this.currentLeftNodeVisits.put(rootNode, walks);

        // Perform forward and backward iterations between users and tweets
        for (int i = 0; i < length; i++) {
            if (isLeftToRight) {
                this.leftIteration(rootNode, resetProbability);
            } else {
                this.rightIteration();
            }
            isLeftToRight = !isLeftToRight;
        }

        if (this.log.isDebugEnabled()) {
            // Print out results (unordered)
            for (Map.Entry<Long, Integer> rightNodeVisit : this.totalRightNodeVisits.entrySet()) {
                this.log.debug("Visited {} {} times", rightNodeVisit.getKey(), rightNodeVisit.getValue());
            }
        }

        return this.mapVisitsToRecommendations(this.totalRightNodeVisits, limit, rootNode);
    }

    /**
     * Maps the map of nodes and counts to a list of the top k ids
     *
     * @param visits count for every node of the right hand side
     * @param limit number of recommendations
     * @param rootNodeId query node's id
     * @return List of size limit with ids for recommendations as elements
     */
    private List<Long> mapVisitsToRecommendations(Map<Long, Integer> visits, int limit, long rootNodeId) {
        Collection<Long> knownNodes = new HashSet<>(this.graph.getLeftNodeNeighbors(rootNodeId));

        return visits.entrySet()
                .stream()
                .filter(entry -> !knownNodes.contains(entry.getKey())) // filter nodes which the user knows
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .limit(limit)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

    }

    /**
     * Performs for every currently visited left node a random step to the right side
     *
     * @param rootNode query node
     * @param resetProbability probability jumping back to query node
     */
    private void leftIteration(long rootNode, double resetProbability) {
        int totalResets = 0;

        // For each left node
        for (Entry<Long, Integer> entry : this.currentLeftNodeVisits.entrySet()) {
            // Get previous visits
            Integer visits = entry.getValue();
            int walks = 0;
            int resets = 0;

            // Calculate how many new walks should be performed and how many resets will happen
            for (int i = 0; i < visits; i++) {

                if (this.random.nextDouble() > resetProbability) {
                    walks++;
                } else {
                    resets++;
                }
            }

            // Sample out edges and pick random walks to the right side
            List<Long> edges = this.graph.getLeftNodeNeighbors(entry.getKey());

            // Perform walks to the right side
            for (int i = 0; i < walks; i++) {
                // Ignore nodes without out links
                if (!edges.isEmpty()) {
                    int randomPosition = this.random.nextInt(edges.size());
                    Long edge = edges.get(randomPosition);
                    this.currentRightNodeVisits.put(edge, this.currentRightNodeVisits.getOrDefault(edge, 0) + 1);
                    this.totalRightNodeVisits.put(edge, this.totalRightNodeVisits.getOrDefault(edge, 0) + 1);
                }
            }

            // Add resets to currentLeftNodeVisits
            totalResets += resets;
        }

        this.currentLeftNodeVisits.clear();
        this.currentLeftNodeVisits.put(rootNode, totalResets);
    }

    /**
     * Performs a random step for every right node that is currently visited
     */
    private void rightIteration() {
        for (Entry<Long, Integer> entry : this.currentRightNodeVisits.entrySet()) {
            Integer visits = entry.getValue();

            // Sample left edges for all walks
            List<Long> edges = this.graph.getRightNodeNeighbors(entry.getKey());

            // Perform walks back to the left side
            for (int i = 0; i < visits; i++) {
                // Ignore nodes without out links
                if (!edges.isEmpty()) {
                    int randomPosition = this.random.nextInt(edges.size());
                    long edge = edges.get(randomPosition);
                    this.currentLeftNodeVisits.put(edge, this.currentLeftNodeVisits.getOrDefault(edge, 0) + 1);
                }
            }
        }
        this.currentRightNodeVisits.clear();
    }

}
