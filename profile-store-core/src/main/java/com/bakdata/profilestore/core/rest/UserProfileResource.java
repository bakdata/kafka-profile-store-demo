package com.bakdata.profilestore.core.rest;


import com.bakdata.profilestore.core.ProfileStoreMain;
import com.bakdata.profilestore.core.avro.UserProfile;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.glassfish.jersey.jackson.JacksonFeature;

@Path("/profile")
@Slf4j
public class UserProfileResource {
    private final KafkaStreams streams;
    private final HostInfo hostInfo;
    private final Client client;

    public UserProfileResource(
            final KafkaStreams kafkaStreams, final HostInfo hostInfo) {
        this.streams = kafkaStreams;
        this.hostInfo = hostInfo;
        this.client = ClientBuilder.newBuilder()
                .register(JacksonFeature.class)
                .register(UserProfileResolver.class).build();
    }

    @GET
    @Path("/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    public UserProfile getUserProfile(@PathParam("userId") final long userId, @Context final UriInfo uriInfo) {
        log.info("Request for user {}", userId);
        final StreamsMetadata metadata =
                this.streams.metadataForKey(ProfileStoreMain.PROFILE_STORE_NAME, userId, Serdes.Long().serializer());

        if (metadata == null) {
            throw new NotFoundException();
        }

        if (!metadata.hostInfo().equals(this.hostInfo)) {
            return this.fetchUserProfile(metadata.hostInfo(), uriInfo.getPath());
        }

        final ReadOnlyKeyValueStore<Long, UserProfile> store =
                this.streams.store(ProfileStoreMain.PROFILE_STORE_NAME, QueryableStoreTypes.keyValueStore());
        if (store == null) {
            throw new NotFoundException();
        }

        final UserProfile profile = store.get(userId);
        if (profile == null) {
            throw new NotFoundException();
        }

        return profile;
    }

    private UserProfile fetchUserProfile(final HostInfo host, final String path) {
        return this.client.target(String.format("http://%s:%d/%s", host.host(), host.port(), path))
                .request(MediaType.APPLICATION_JSON_TYPE)
                .get(UserProfile.class);
    }

}
