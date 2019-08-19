package com.bakdata.profilestore.core;

import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.avro.ChartRecord;
import com.bakdata.profilestore.core.avro.CompositeKey;
import com.bakdata.profilestore.core.avro.UserProfile;
import com.bakdata.profilestore.core.fields.AlbumHandler;
import com.bakdata.profilestore.core.fields.ArtistHandler;
import com.bakdata.profilestore.core.fields.FieldHandler;
import com.bakdata.profilestore.core.fields.TrackHandler;
import com.bakdata.profilestore.core.processor.ChartsProcessor;
import com.bakdata.profilestore.core.processor.EventCountProcessor;
import com.bakdata.profilestore.core.processor.FirstEventProcessor;
import com.bakdata.profilestore.core.processor.LastEventProcessor;
import com.bakdata.profilestore.core.rest.ProfilestoreRestService;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.Stores;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Slf4j
@Command(name = "profile-store", mixinStandardHelpOptions = true,
        description = "Start KafkaStreams application profile store")
public class ProfilestoreMain implements Callable<Void> {
    public static final String PROFILE_STORE_NAME = "profile-store";
    public static final String COUNT_TOPIC_PREFIX = "profiler-event-count-";
    public static final int TOP_K = 10;

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

    @CommandLine.Option(names = "--topic", defaultValue = "listening-events",
            description = "name of topic with incoming interactions")
    private String topicName;

    public static void main(final String[] args) {
        System.exit(new CommandLine(new ProfilestoreMain()).execute(args));
    }

    @Override
    public Void call() throws Exception {
        final Properties properties = this.getProperties();
        final Topology topology = this.buildTopology(properties);
        log.debug(topology.describe().toString());
        final KafkaStreams streams = new KafkaStreams(topology, properties);

        streams.cleanUp();
        streams.start();

        final ProfilestoreRestService restService =
                new ProfilestoreRestService(new HostInfo(this.host, this.port), streams);
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
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, LongSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        return props;
    }

    public Topology buildTopology(final Properties properties) {
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                properties.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));

        final SpecificAvroSerde<ListeningEvent> listeningEventSerde = new SpecificAvroSerde<>();
        listeningEventSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<CompositeKey> compositeKeySerde = new SpecificAvroSerde<>();
        compositeKeySerde.configure(serdeConfig, true);

        final SpecificAvroSerde<UserProfile> userProfileSerde = new SpecificAvroSerde<>();
        userProfileSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<ChartRecord> chartRecordSerde = new SpecificAvroSerde<>();
        chartRecordSerde.configure(serdeConfig, false);

        final StreamsBuilder builder = new StreamsBuilder();

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(PROFILE_STORE_NAME),
                        Serdes.Long(),
                        userProfileSerde)
        );

        final KStream<Long, ListeningEvent> inputStream = builder.stream(this.topicName);

        inputStream.process(EventCountProcessor::new, PROFILE_STORE_NAME);
        inputStream.process(FirstEventProcessor::new, PROFILE_STORE_NAME);
        inputStream.process(LastEventProcessor::new, PROFILE_STORE_NAME);

        this.addTopKProcessor(compositeKeySerde, chartRecordSerde, inputStream);

        return builder.build();
    }

    private void addTopKProcessor(final SpecificAvroSerde<CompositeKey> compositeKeySerde,
            final SpecificAvroSerde<ChartRecord> chartRecordSerde,
            final KStream<Long, ListeningEvent> inputStream) {

        final Serde<Long> longSerde = Serdes.Long();
        final Grouped<CompositeKey, Long> groupedSerde = Grouped.with(compositeKeySerde, longSerde);
        final Produced<Long, ChartRecord> producedSerde = Produced.with(longSerde, chartRecordSerde);

        final FieldHandler[] fieldHandlers = {new AlbumHandler(), new ArtistHandler(), new TrackHandler()};
        for (final FieldHandler fieldHandler : fieldHandlers) {

            final KTable<CompositeKey, Long> fieldCountsPerUser = inputStream
                    .map((key, event) -> KeyValue.pair(key, fieldHandler.extractId(event)))
                    .groupBy(CompositeKey::new, groupedSerde)
                    .count();

            // create a stream of counts per user and field and repartition it so that the a count for userId is on
            // the same partition as a event for a user.
            // To trigger the repartition, it is necessary to call through()

            final KStream<Long, ChartRecord> countUpdateStream = fieldCountsPerUser
                    .toStream()
                    .map((key, count) ->
                            KeyValue.pair(
                                    key.getPrimaryKey(),
                                    new ChartRecord(key.getSecondaryKey(), count)
                            ))
                    .through(COUNT_TOPIC_PREFIX + fieldHandler.type().toString().toLowerCase(), producedSerde);

            countUpdateStream.process(() -> new ChartsProcessor(TOP_K, fieldHandler), PROFILE_STORE_NAME);
        }
    }

}
