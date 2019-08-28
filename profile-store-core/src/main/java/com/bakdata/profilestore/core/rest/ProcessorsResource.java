package com.bakdata.profilestore.core.rest;

import com.bakdata.profilestore.core.ProfileStoreMain;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;

@Path("/processors")
public class ProcessorsResource {
    private final KafkaStreams streams;

    public ProcessorsResource(final KafkaStreams streams) {
        this.streams = streams;
    }

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public List<ProcessorMetadata> getProcessors() {
        return this.streams.allMetadataForStore(ProfileStoreMain.PROFILE_STORE_NAME)
                .stream()
                .map(metadata -> new ProcessorMetadata(
                        metadata.host(),
                        metadata.port(),
                        metadata.topicPartitions().stream()
                                .map(TopicPartition::partition)
                                .collect(Collectors.toSet()))
                )
                .collect(Collectors.toList());
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public class ProcessorMetadata {
        private String host;
        private int port;
        private Set<Integer> topicPartitions;
    }
}
