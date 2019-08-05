package com.bakdata.recommender;

import com.bakdata.recommender.avro.AdjacencyList;
import com.bakdata.recommender.graph.BipartiteGraph;
import com.bakdata.recommender.graph.KeyValueGraph;
import com.bakdata.recommender.rest.RestService;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;


@Command(name = "recommender", mixinStandardHelpOptions = true,
        description = "Start KafkaStreams application recommender")
public class RecommenderMain implements Callable<Void> {
    private final Logger log = LoggerFactory.getLogger(RecommenderMain.class);
    public static final String LEFT_INDEX_NAME = "leftIndex";
    public static final String RIGHT_INDEX_NAME = "rightIndex";

    @CommandLine.Option(names = "--application-id", required = true, description = "name of streams application")
    private final String applicationId = "recommender";

    @CommandLine.Option(names = "--host", required = true, description = "address of host machine")
    private final String host = "localhost";

    @CommandLine.Option(names = "--port", defaultValue = "8080", description = "port of REST service")
    private final int port = 8080;

    @CommandLine.Option(names = "--brokers", required = true, description = "address of kafka broker")
    private final String brokers = "localhost:29092";

    @CommandLine.Option(names = "--schema-registry-url", required = true, description = "address of schema registry")
    private final String schemaRegistryUrl = "localhost:8081";

    @CommandLine.Option(names = "--topic", defaultValue = "interactions",
            description = "name of topic with incoming interactions")
    private final String topicName = "interactions";

    @Override
    public Void call() throws Exception {
        final Properties properties = this.getProperties();
        final Topology topology = this.buildTopology(properties);
        final KafkaStreams streams = new KafkaStreams(topology, properties);

        streams.cleanUp();
        streams.start();
        this.waitForKafkaStreams(streams);

        final Map<RecommendationType, BipartiteGraph> graph = this.getGraph(streams);
        final RestService restService = new RestService(new HostInfo(this.host, this.port), graph);
        restService.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
                restService.stop();
            } catch (Exception e) {
                this.log.warn("Error in shutdown", e);
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
        Properties props = new Properties();
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
    public Map<RecommendationType, BipartiteGraph> getGraph(KafkaStreams streams) {
        Map<RecommendationType, BipartiteGraph> graphs = new EnumMap<>(RecommendationType.class);
        for (RecommendationType type : RecommendationType.values()) {
            graphs.put(type, new KeyValueGraph(
                    streams.store(type.getLeftIndexName(), QueryableStoreTypes.keyValueStore()),
                    streams.store(type.getRightIndexName(),
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
    public Topology buildTopology(Properties properties) {
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                properties.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
        final SpecificAvroSerde<AdjacencyList> adjacencyListSerde = new SpecificAvroSerde<>();
        adjacencyListSerde.configure(serdeConfig, true);

        Topology topology = new Topology()
                .addSource("interaction-source", this.topicName)
                .addProcessor("interaction-processor", RecommenderProcessor::new, "interaction-source");

        for (RecommendationType type : RecommendationType.values()) {
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
    private Topology addStateStores(Topology topology, RecommendationType type,
            SpecificAvroSerde<AdjacencyList> adjacencyListSerde) {
        return topology
                .addStateStore(Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(type.getLeftIndexName()),
                        Serdes.Long(), adjacencyListSerde), "interaction-processor")
                .addStateStore(Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(type.getRightIndexName()),
                        Serdes.Long(), adjacencyListSerde), "interaction-processor");
    }

    /**
     * Check if KafkaStreams is running by trying to get the state stores
     *
     * @param streams the KafkaStreams instance
     */
    private void waitForKafkaStreams(KafkaStreams streams) throws Exception {
        while (true) {
            try {
                this.getGraph(streams);
                return;
            } catch (InvalidStateStoreException ignored) {
                // store not yet ready for querying
                Thread.sleep(1000);
            }
        }
    }

}
