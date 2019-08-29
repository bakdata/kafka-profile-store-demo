package com.bakdata.profilestore.recommender.rest;

import static org.apache.kafka.common.requests.DeleteAclsResponse.log;

import com.bakdata.profilestore.common.avro.NamedRecord;
import com.bakdata.profilestore.recommender.FieldType;
import com.bakdata.profilestore.recommender.algorithm.Salsa;
import com.bakdata.profilestore.recommender.graph.BipartiteGraph;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

@Path("/recommendation")
public class RestResource {
    private final Map<FieldType, BipartiteGraph> graphs;
    private final KafkaStreams steams;
    private final Map<FieldType, String> storeNames;

    public RestResource(final Map<FieldType, BipartiteGraph> graphs,
            final KafkaStreams streams,
            final Map<FieldType, String> storeNames) {
        this.graphs = graphs;
        this.steams = streams;
        this.storeNames = storeNames;
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
    public List<NamedRecord> getRecommendationsForUser(
            @PathParam("userId") final long userId,
            @PathParam("type") final String type,
            @DefaultValue("10") @QueryParam("limit") final int limit,
            @DefaultValue("1000") @QueryParam("walks") final int walks,
            @DefaultValue("100") @QueryParam("walkLength") final int walkLength,
            @DefaultValue("0.1") @QueryParam("resetProbability") final float resetProbability) {
        log.info("Request for user {} and type {}", userId, type);
        final FieldType recommendationType = FieldType.valueOf(type.toUpperCase());
        List<Long> ids;
        try {
            ids = new Salsa(this.graphs.get(recommendationType), new Random())
                    .compute(userId, walks, walkLength, resetProbability, limit);
        } catch (final RuntimeException e) {
            log.info("No recommendation computed", e);
            ids = Collections.emptyList();
        }

        final ReadOnlyKeyValueStore<Long, String> nameTable =
                this.steams.store(this.storeNames.get(recommendationType), QueryableStoreTypes.keyValueStore());
        return ids.stream().map(id -> new NamedRecord(id, nameTable.get(id))).collect(Collectors.toList());

    }
}
