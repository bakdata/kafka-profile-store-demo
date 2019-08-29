package com.bakdata.profilestore.recommender;

import com.bakdata.profilestore.recommender.avro.AdjacencyList;
import com.bakdata.profilestore.recommender.graph.BipartiteGraph;
import com.bakdata.profilestore.recommender.graph.KeyValueGraph;
import com.bakdata.profilestore.recommender.rest.RestService;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.Stores;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@NoArgsConstructor
@Slf4j
@Command(name = "recommender", mixinStandardHelpOptions = true,
        description = "Start KafkaStreams application recommender")
public class RecommenderMain implements Callable<Void> {
    public static final String LEFT_INDEX_NAME = "recommender-left-index";
    public static final String RIGHT_INDEX_NAME = "recommender-right-index";
    public static final Map<FieldType, String> storeNames = getStoreNames();


    @CommandLine.Option(names = "--application-id", required = true, description = "name of streams application")
    private String applicationId;

    @CommandLine.Option(names = "--host", required = true, description = "address of host machine")
    private String host;

    @CommandLine.Option(names = "--port", defaultValue = "8080", description = "port of REST service")
    private int port;

    @CommandLine.Option(names = "--brokers", required = true, description = "address of kafka broker")
    private String brokers;

    @CommandLine.Option(names = "--schema-registry-url", required = true, description = "address of schema registry")
    private String schemaRegistryUrl;

    @CommandLine.Option(names = "--listening-event-topic", defaultValue = "listening-events",
            description = "name of topic with incoming interactions")
    private String listeningEventTopicName;

    @CommandLine.Option(names = "--artist-topic", defaultValue = "artists",
            description = "name of topic with incoming artists")
    private String artistTopicName;

    @CommandLine.Option(names = "--album-topic", defaultValue = "albums",
            description = "name of topic with incoming albums")
    private String albumTopicName;

    @CommandLine.Option(names = "--track-topic", defaultValue = "tracks",
            description = "name of topic with incoming tracks")
    private String trackTopicName;

    public RecommenderMain(final String listeningEventTopicName, final String artistTopicName,
            final String albumTopicName, final String trackTopicName) {
        this.listeningEventTopicName = listeningEventTopicName;
        this.artistTopicName = artistTopicName;
        this.albumTopicName = albumTopicName;
        this.trackTopicName = trackTopicName;
    }

    public static void main(final String[] args) {
        System.exit(new CommandLine(new RecommenderMain()).execute(args));
    }

    @Override
    public Void call() throws Exception {
        final Properties properties = this.getProperties();
        final Topology topology = this.buildTopology(properties);
        final KafkaStreams streams = new KafkaStreams(topology, properties);

        streams.cleanUp();
        streams.start();
        this.waitForKafkaStreams(streams);

        final Map<FieldType, BipartiteGraph> graph = this.getGraph(streams);

        final RestService restService = new RestService(new HostInfo(this.host, this.port), graph, streams, storeNames);
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

    /**
     * Build and get the properties for the KafkaStreams application
     *
     * @return the Properties of the application
     */
    public Properties getProperties() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, this.applicationId);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, String.format("%s:%s", this.host, this.port));
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryUrl);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokers);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        return props;
    }

    /**
     * Get the bipartite graph of the application
     *
     * @param streams the KafkaStreams instance
     * @return BipartiteGraph instance that represents the left and right index
     */
    public Map<FieldType, BipartiteGraph> getGraph(final KafkaStreams streams) {
        final Map<FieldType, BipartiteGraph> graphs = new EnumMap<>(FieldType.class);
        for (final FieldType type : FieldType.values()) {
            graphs.put(type, new KeyValueGraph(
                    streams.store(RecommenderProcessor.getLeftIndexName(type), QueryableStoreTypes.keyValueStore()),
                    streams.store(RecommenderProcessor.getRightIndexName(type),
                            QueryableStoreTypes.keyValueStore())));
        }
        return graphs;
    }

    /**
     * Build the KafkaStreams topology
     *
     * @param properties the properties of the KafkaStreams application
     * @return the topology of the application
     */
    public Topology buildTopology(final Properties properties) {
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                properties.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
        final SpecificAvroSerde<AdjacencyList> adjacencyListSerde = new SpecificAvroSerde<>();
        adjacencyListSerde.configure(serdeConfig, true);

        final StreamsBuilder builder = new StreamsBuilder();

        this.addGlobalNameStore(builder, this.albumTopicName, storeNames.get(FieldType.ALBUM));
        this.addGlobalNameStore(builder, this.artistTopicName, storeNames.get(FieldType.ARTIST));
        this.addGlobalNameStore(builder, this.trackTopicName, storeNames.get(FieldType.TRACK));

        Topology topology = builder.build();

        topology
                .addSource("interaction-source", this.listeningEventTopicName)
                .addProcessor("interaction-processor", RecommenderProcessor::new, "interaction-source");

        for (final FieldType type : FieldType.values()) {
            topology = this.addStateStores(topology, type, adjacencyListSerde);
        }
        return topology;
    }

    /**
     * Adds a new state store for every RecommendationType
     *
     * @param topology base topology
     * @param type type for which the processor should be added
     * @param adjacencyListSerde serde
     * @return updated Topology
     */
    private Topology addStateStores(final Topology topology, final FieldType type,
            final SpecificAvroSerde<AdjacencyList> adjacencyListSerde) {
        return topology
                .addStateStore(Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(RecommenderProcessor.getLeftIndexName(type)),
                        Serdes.Long(), adjacencyListSerde), "interaction-processor")
                .addStateStore(Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(RecommenderProcessor.getRightIndexName(type)),
                        Serdes.Long(), adjacencyListSerde), "interaction-processor");
    }

    private void addGlobalNameStore(final StreamsBuilder builder, final String topicName, final String storeName) {
        builder.globalTable(topicName,
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as(storeName)
                        .withKeySerde(Serdes.Long())
                        .withValueSerde(Serdes.String()));
    }

    /**
     * Wait for the application to change status to running
     *
     * @param streams the KafkaStreams instance
     */
    private void waitForKafkaStreams(final KafkaStreams streams) throws Exception {
        while (true) {
            try {
                this.getGraph(streams);
                return;
            } catch (final InvalidStateStoreException ignored) {
                // store not yet ready for querying
                log.debug("Store not available");
                Thread.sleep(1000);
            }
        }
    }

    private static Map<FieldType, String> getStoreNames() {
        return Stream.of(FieldType.values()).collect(Collectors.toMap(
                type -> type,
                type -> type.toString().toLowerCase() + "-store"));
    }

}
