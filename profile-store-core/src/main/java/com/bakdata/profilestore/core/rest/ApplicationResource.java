package com.bakdata.profilestore.core.rest;

import com.bakdata.profilestore.core.ProfileStoreMain;
import java.util.Map;
import java.util.stream.Collectors;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.jooq.lambda.Seq;

@Path("/applications")
public class ApplicationResource {
    private final KafkaStreams streams;

    public ApplicationResource(final KafkaStreams streams) {
        this.streams = streams;
    }

    @GET
    @Path("/all")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<Integer, String> getApplicationHosts() {
        return Seq.seq(this.streams.allMetadataForStore(ProfileStoreMain.PROFILE_STORE_NAME))
                .flatMap(ApplicationResource::getAddressesForPartitions)
                .distinct(PartitionAddress::getPartition)
                .collect(Collectors.toMap(PartitionAddress::getPartition, PartitionAddress::getAddress));
    }

    private static Seq<PartitionAddress> getAddressesForPartitions(final StreamsMetadata metadata) {
        return Seq.seq(metadata.topicPartitions())
                .map(partition ->
                        new PartitionAddress(partition.partition(), metadata.host(), metadata.port()));
    }

    @AllArgsConstructor
    @Getter
    private static class PartitionAddress {
        private final int partition;
        private final String host;
        private final int port;

        private String getAddress() {
            return String.format("%s:%d", this.host, this.port);
        }
    }

}
