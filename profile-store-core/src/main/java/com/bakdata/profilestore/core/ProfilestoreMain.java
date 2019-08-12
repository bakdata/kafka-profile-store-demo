package com.bakdata.profilestore.core;

import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.avro.CompositeKey;
import com.bakdata.profilestore.core.avro.UserProfile;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import picocli.CommandLine;

public class ProfilestoreMain {

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

        return new ProfilestoreTopology(this.topicName, compositeKeySerde, userProfileSerde);
    }

}
