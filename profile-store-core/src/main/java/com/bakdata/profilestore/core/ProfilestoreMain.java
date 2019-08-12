package com.bakdata.profilestore.core;

import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.avro.CompositeKey;
import com.bakdata.profilestore.core.avro.UserProfile;
import com.bakdata.profilestore.core.rest.ProfilestoreRestService;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Slf4j
@Command(name = "recommender", mixinStandardHelpOptions = true,
        description = "Start KafkaStreams application recommender")
public class ProfilestoreMain implements Callable<Void> {

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

    public static void main(final String[] args) {
        System.exit(new CommandLine(new ProfilestoreMain()).execute(args));
    }

    @Override
    public Void call() throws Exception {
        final Properties properties = this.getProperties();
        final Topology topology = this.buildTopology(properties);
        final KafkaStreams streams = new KafkaStreams(topology, properties);

        streams.cleanUp();
        streams.start();

        final ProfilestoreRestService restService = new ProfilestoreRestService(new HostInfo(this.host, this.port),
                streams.store(ProfilestoreTopology.PROFILE_STORE_NAME,
                        QueryableStoreTypes.keyValueStore()));
        restService.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
                restService.stop();
            } catch (final Exception e) {
                log.warn("Error in shutdown", e);
            }
        }));

        return null;
    }

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

        return new ProfilestoreTopology(this.topicName, compositeKeySerde, userProfileSerde);
    }

}
