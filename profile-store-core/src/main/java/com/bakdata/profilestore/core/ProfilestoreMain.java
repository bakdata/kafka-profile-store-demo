package com.bakdata.profilestore.core;

import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.avro.CompositeKey;
import com.bakdata.profilestore.core.avro.UserProfile;
import com.bakdata.profilestore.core.ids.AlbumIdExtractor;
import com.bakdata.profilestore.core.ids.ArtistIdExtractor;
import com.bakdata.profilestore.core.ids.IdExtractor;
import com.bakdata.profilestore.core.ids.TrackIdExtractor;
import com.bakdata.profilestore.core.processor.FirstEventProcessor;
import com.bakdata.profilestore.core.processor.LastEventProcessor;
import com.bakdata.profilestore.core.processor.TopKProcessor;
import com.bakdata.profilestore.core.serde.TopKListSerde;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import picocli.CommandLine;

public class ProfilestoreMain {
    public static final String PROFILE_STORE_NAME = "profile_store";

    @CommandLine.Option(names = "--application-id", required = true, description = "name of streams application")
    private String applicationId = "music-recommender";

    @CommandLine.Option(names = "--host", required = true, description = "address of host machine")
    private String host = "localhost";

    @CommandLine.Option(names = "--port", defaultValue = "8080", description = "port of REST service")
    private int port = 8080;

    @CommandLine.Option(names = "--brokers", required = true, description = "address of kafka broker")
    private String brokers = "localhost:29092";

    @CommandLine.Option(names = "--schema-registry-url", required = true, description = "address of schema registry")
    private String schemaRegistryUrl = "localhost:8081";

    @CommandLine.Option(names = "--topic", defaultValue = "listening-events",
            description = "name of topic with incoming interactions")
    private String topicName = "listening-events";

    public Properties getProperties() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, this.applicationId);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, String.format("%s:%s", this.host, this.port));
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryUrl);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokers);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        return props;
    }


    public Topology buildTopology(final Properties properties) {
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                properties.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));

        final SpecificAvroSerde<ListeningEvent> listeningEventSpecificAvroSerde = new SpecificAvroSerde<>();
        listeningEventSpecificAvroSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<CompositeKey> compositeKeySerde = new SpecificAvroSerde<>();
        compositeKeySerde.configure(serdeConfig, false);

        final SpecificAvroSerde<UserProfile> userProfileSerde = new SpecificAvroSerde<>();
        userProfileSerde.configure(serdeConfig, false);

        Topology topology = new Topology();
        topology = topology
                .addSource("source", this.topicName)
                .addStateStore(
                        Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(PROFILE_STORE_NAME), Serdes.Long(),
                                userProfileSerde));
        topology = this.addTopKProcessor(topology,
                Arrays.asList(new AlbumIdExtractor(), new ArtistIdExtractor(), new TrackIdExtractor()),
                compositeKeySerde);
        topology = this.addFirstListeningEventProcessor(topology, userProfileSerde);
        topology = this.addLastListeningEventProcessor(topology, userProfileSerde);


        return topology;
    }

    private Topology addTopKProcessor(final Topology topology, final Iterable<IdExtractor> idExtractors,
            final SpecificAvroSerde<CompositeKey> compositeKeySpecificAvroSerde) {

        Topology newTopology = topology;
        final Serde<Long> longSerdes = Serdes.Long();
        final Serde<TopKList> topKListSerde = new TopKListSerde();

        for (final IdExtractor extractor : idExtractors) {
            final String processorName = TopKProcessor.getProcessorName(extractor.type());
            newTopology = newTopology
                    .addProcessor(processorName, () -> new TopKProcessor(10, extractor), "source")
                    .addStateStore(Stores.keyValueStoreBuilder(
                            Stores.inMemoryKeyValueStore(TopKProcessor.getCountStoreName(extractor.type())),
                            compositeKeySpecificAvroSerde, longSerdes), processorName)
                    .addStateStore(Stores.keyValueStoreBuilder(
                            Stores.inMemoryKeyValueStore(TopKProcessor.getChartStoreName(extractor.type())),
                            longSerdes, topKListSerde), processorName);
        }
        return newTopology;
    }

    private Topology addFirstListeningEventProcessor(final Topology topology,
            final SpecificAvroSerde<UserProfile> userProfileSpecificAvroSerde) {
        return topology.addProcessor("first_event_processor", FirstEventProcessor::new, "source")
                .connectProcessorAndStateStores("first_event_processor", PROFILE_STORE_NAME);
    }

    private Topology addLastListeningEventProcessor(final Topology topology,
            final SpecificAvroSerde<UserProfile> userProfileSpecificAvroSerde) {
        return topology.addProcessor("last_event_processor", LastEventProcessor::new, "source")
                .connectProcessorAndStateStores("last_event_processor", PROFILE_STORE_NAME);
    }
}
