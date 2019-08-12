package com.bakdata.profilestore.core.rest;


import com.bakdata.profilestore.core.ProfilestoreTopology;
import com.bakdata.profilestore.core.avro.UserProfile;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

@Path("/profile")
@Slf4j
public class UserProfileResource {
    private final KafkaStreams streams;

    public UserProfileResource(
            final KafkaStreams kafkaStreams) {
        this.streams = kafkaStreams;
    }

    @GET
    @Path("/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getRecommendationsForUser(
            @PathParam("userId") final long userId) {
        log.info("Request for user {}", userId);
        final ReadOnlyKeyValueStore<Long, UserProfile> profileStore = this.streams.store(ProfilestoreTopology.PROFILE_STORE_NAME,
                QueryableStoreTypes.keyValueStore());
        return profileStore.get(userId).toString();
    }
}
