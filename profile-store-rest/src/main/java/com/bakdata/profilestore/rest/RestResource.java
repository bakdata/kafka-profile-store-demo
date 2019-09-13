package com.bakdata.profilestore.rest;

import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.HostInfo;
import org.glassfish.jersey.jackson.JacksonFeature;

@Slf4j
@Path("/")
public class RestResource {
    private static final String ALL_HOSTS_PATH = "applications/all";
    private static final long TIMEOUT = 50_000;

    private final HostInfo profileHost;
    private final HostInfo recommenderHost;
    private final Client client;

    private Map<Integer, String> partitionToHostMap;
    private long lastUpdate = 0L;

    public RestResource(final HostInfo profileHost, final HostInfo recommenderHost) {
        this.profileHost = profileHost;
        this.recommenderHost = recommenderHost;
        this.partitionToHostMap = new HashMap<>();
        this.client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
    }

    @GET
    @Path("/profile/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getUserProfile(@PathParam("userId") final long userId, @Context final UriInfo uriInfo) {
        if (this.partitionToHostMap.isEmpty() || (System.currentTimeMillis() - this.lastUpdate) > TIMEOUT) {
            log.debug("Update current profile store hosts");
            this.partitionToHostMap =
                    this.client.target(getURL(this.profileHost, ALL_HOSTS_PATH))
                            .request(MediaType.APPLICATION_JSON)
                            // removing explicit type leads to compiler bug
                            .get(new GenericType<Map<Integer, String>>() {});
            log.debug("Current hosts are: {}", this.partitionToHostMap);
            this.lastUpdate = System.currentTimeMillis();
        }

        // Try to get the target host directly by using the partitioner
        // Calculating a wrong partition because of changes in the meantime is not a problem
        // because the target host can forward the request to the correct host
        final int partition = UserPartitioner.calculatePartition(userId, this.partitionToHostMap.size());
        final String targetHost = this.partitionToHostMap.getOrDefault(partition, getAddress(this.profileHost));

        final String url = getURL(targetHost, uriInfo.getPath());
        log.debug("Forward request to {}", url);
        return this.client.target(url)
                .request(MediaType.APPLICATION_JSON)
                .get();
    }

    @GET
    @Path("/recommendation/{userId}/{type}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRecommendationsForUser(
            @PathParam("userId") final long userId,
            @PathParam("type") final String type,
            @Context final UriInfo uriInfo,
            @DefaultValue("10") @QueryParam("limit") final int limit,
            @DefaultValue("1000") @QueryParam("walks") final int walks,
            @DefaultValue("100") @QueryParam("walkLength") final int walkLength,
            @DefaultValue("0.1") @QueryParam("resetProbability") final float resetProbability) {
        // Every host has all data, so it does not matter which one we call
        // This assumes there is a load balancer in place
        final String url = getURL(this.recommenderHost, uriInfo.getPath());
        log.debug("Forward request to {}", url);
        return this.client.target(url)
                .request(MediaType.APPLICATION_JSON)
                .get();
    }

    private static String getAddress(final HostInfo address) {
        return String.format("http://%s", address);
    }

    private static String getURL(final String address, final String path) {
        return String.format("http://%s/%s", address, path);
    }

    private static String getURL(final HostInfo hostInfo, final String path) {
        return String.format("http://%s:%d/%s", hostInfo.host(), hostInfo.port(), path);
    }

}
