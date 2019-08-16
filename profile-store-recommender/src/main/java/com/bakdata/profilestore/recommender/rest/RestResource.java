package com.bakdata.profilestore.recommender.rest;

import static org.apache.kafka.common.requests.DeleteAclsResponse.log;

import com.bakdata.profilestore.recommender.FieldType;
import com.bakdata.profilestore.recommender.algorithm.Salsa;
import com.bakdata.profilestore.recommender.graph.BipartiteGraph;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("/recommendation")
public class RestResource {
    private final Map<FieldType, BipartiteGraph> graphs;

    public RestResource(final Map<FieldType, BipartiteGraph> graphs) {
        this.graphs = graphs;
    }

    /**
     * Gets a list of recommendations for an id
     *
     * @param userId the id of the user the recommendations are made for
     * @param limit number of recommendations
     * @param walks number of random walks in the monte carlo simulation
     * @param walkLength number of steps in a random walk
     * @param resetProbability probability to jump back to query node
     * @return List of size limit with ids for recommendations as elements
     */
    @GET
    @Path("/{userId}/{type}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<Long> getRecommendationsForUser(
            @PathParam("userId") final long userId,
            @PathParam("type") final String type,
            @DefaultValue("10") @QueryParam("limit") final int limit,
            @DefaultValue("1000") @QueryParam("walks") final int walks,
            @DefaultValue("100") @QueryParam("walkLength") final int walkLength,
            @DefaultValue("0.1") @QueryParam("resetProbability") final float resetProbability) {
        log.info("Request for user {} and type {}", userId, type);
        final FieldType recommendationType = FieldType.valueOf(type.toUpperCase());
        try {
            return new Salsa(this.graphs.get(recommendationType), new Random())
                    .compute(userId, walks, walkLength, resetProbability, limit);
        } catch (RuntimeException e) {
            return Collections.emptyList();
        }
    }
}
