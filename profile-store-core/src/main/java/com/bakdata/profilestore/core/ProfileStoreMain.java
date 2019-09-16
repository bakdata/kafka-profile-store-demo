package com.bakdata.profilestore.core;

import com.bakdata.profilestore.common.avro.FieldRecord;
import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.avro.ChartRecord;
import com.bakdata.profilestore.core.avro.CompositeKey;
import com.bakdata.profilestore.core.avro.NamedChartRecord;
import com.bakdata.profilestore.core.avro.UserProfile;
import com.bakdata.profilestore.core.fields.AlbumHandler;
import com.bakdata.profilestore.core.fields.ArtistHandler;
import com.bakdata.profilestore.core.fields.FieldHandler;
import com.bakdata.profilestore.core.fields.TrackHandler;
import com.bakdata.profilestore.core.processor.ChartsProcessor;
import com.bakdata.profilestore.core.processor.EventCountProcessor;
import com.bakdata.profilestore.core.processor.FirstEventProcessor;
import com.bakdata.profilestore.core.processor.LastEventProcessor;
import com.bakdata.profilestore.core.rest.ProfileStoreRestService;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.jooq.lambda.Seq;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@NoArgsConstructor
@Slf4j
@Command(name = "profile-store", mixinStandardHelpOptions = true,
        description = "Start KafkaStreams application profile store")
public class ProfileStoreMain implements Callable<Void> {
    public static final String PROFILE_STORE_NAME = "profile-store";
    public static final String ARTIST_STORE_NAME = "artist_store";
    public static final String ALBUM_STORE_NAME = "album_store";
    public static final String TRACK_STORE_NAME = "track_store";
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

    @CommandLine.Option(names = "--listening-events-topic", defaultValue = "listening-events",
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


    public ProfileStoreMain(final String listeningEventTopicName, final String artistTopicName,
            final String albumTopicName,
            final String trackTopicName) {
        this.listeningEventTopicName = listeningEventTopicName;
        this.artistTopicName = artistTopicName;
        this.albumTopicName = albumTopicName;
        this.trackTopicName = trackTopicName;
    }

    public static void main(final String[] args) {
        System.exit(new CommandLine(new ProfileStoreMain()).execute(args));
    }

    @Override
    public Void call() throws Exception {
        final Properties properties = this.getProperties();
        final Topology topology = this.buildTopology(properties, this.listeningEventTopicName);
        log.debug(topology.describe().toString());
        final KafkaStreams streams = new KafkaStreams(topology, properties);

        streams.cleanUp();
        streams.start();

        final ProfileStoreRestService restService =
                new ProfileStoreRestService(new HostInfo(this.host, this.port), streams);
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

    public Topology buildTopology(final Properties properties, final String inputTopic) {

        final ProfileStoreSerDeCollection serDes =
                this.createSerDes(properties.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));

        final StreamsBuilder builder = new StreamsBuilder();

        // add GlobalKTables that are used to join the ids of the fields like track or artist with their names
        final Map<FieldType, GlobalKTable<Long, FieldRecord>> joinTables = this.createGlobalTables(builder, serDes);

        // add StateStore for the profile store
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(PROFILE_STORE_NAME),
                        Serdes.Long(),
                        serDes.getCompositeKeySerDe())
        );

        final KStream<Long, ListeningEvent> inputStream = builder.stream(inputTopic);

        inputStream.process(EventCountProcessor::new, PROFILE_STORE_NAME);
        inputStream.process(FirstEventProcessor::new, PROFILE_STORE_NAME);
        inputStream.process(LastEventProcessor::new, PROFILE_STORE_NAME);

        this.addTopKProcessor(serDes, inputStream, joinTables);

        return builder.build();
    }

    private Map<FieldType, GlobalKTable<Long, FieldRecord>> createGlobalTables(final StreamsBuilder builder,
            final ProfileStoreSerDeCollection serDeCollection) {

        final List<GlobalTableData> globalTableDataList = List.of(
                GlobalTableData.of(FieldType.ARTIST, this.artistTopicName, ARTIST_STORE_NAME),
                GlobalTableData.of(FieldType.ALBUM, this.albumTopicName, ALBUM_STORE_NAME),
                GlobalTableData.of(FieldType.TRACK, this.trackTopicName, TRACK_STORE_NAME)
        );

        return Seq.seq(globalTableDataList)
                .toMap(GlobalTableData::getFieldType,
                        globalTableData -> this.createGlobalKTable(globalTableData, builder, serDeCollection));
    }

    private GlobalKTable<Long, FieldRecord> createGlobalKTable(final GlobalTableData globalTableData,
            final StreamsBuilder builder,
            final ProfileStoreSerDeCollection serDeCollection) {
        return builder.globalTable(globalTableData.getInputTopic(),
                Materialized.<Long, FieldRecord, KeyValueStore<Bytes, byte[]>>as(
                        globalTableData.getStoreName())
                        .withKeySerde(serDeCollection.getLongSerDe())
                        .withValueSerde(serDeCollection.getFieldRecordSerDe()));
    }

    private void addTopKProcessor(final ProfileStoreSerDeCollection serdeCollection,
            final KStream<Long, ListeningEvent> inputStream,
            final Map<FieldType, GlobalKTable<Long, FieldRecord>> joinTables) {

        final Grouped<CompositeKey, Long> groupedSerde =
                Grouped.with(serdeCollection.getCompositeKeySerDe(), serdeCollection.getLongSerDe());
        final Produced<Long, ChartRecord> producedSerde =
                Produced.with(serdeCollection.getLongSerDe(), serdeCollection.getChartRecordSerDe());

        // add a processor for every field type
        final List<FieldHandler> fieldHandlers = List.of(new AlbumHandler(), new ArtistHandler(), new TrackHandler());

        for (final FieldHandler fieldHandler : fieldHandlers) {

            final GlobalKTable<Long, FieldRecord> joinTable = joinTables.get(fieldHandler.type());
            final KStream<Long, NamedChartRecord> namedCountUpdateStream =
                    this.createNamedCountUpdateStream(inputStream, fieldHandler, joinTable, groupedSerde,
                            producedSerde);

            namedCountUpdateStream.process(() -> new ChartsProcessor(TOP_K, fieldHandler), PROFILE_STORE_NAME);
        }
    }

    /**
     * Create a stream with the userId as key and a tuple (id, name for id, count of plays) as value
     */
    private KStream<Long, NamedChartRecord> createNamedCountUpdateStream(
            final KStream<Long, ListeningEvent> inputStream,
            final FieldHandler fieldHandler, final GlobalKTable<Long, FieldRecord> joinTable,
            final Grouped<CompositeKey, Long> groupedSerde,
            final Produced<Long, ChartRecord> producedSerde) {

        final KStream<Long, ChartRecord> countUpdateStream =
                this.createCountUpdateStream(inputStream, fieldHandler, groupedSerde, producedSerde);

        final KStream<Long, NamedChartRecord> namedCountUpdateStream = countUpdateStream
                .join(joinTable,
                        (userId, chartRecord) -> chartRecord.getId(),
                        (chartRecord, fieldRecord) -> new NamedChartRecord(chartRecord.getId(),
                                fieldRecord.getName(),
                                chartRecord.getCountPlays()));

        return namedCountUpdateStream;
    }

    /**
     * Create a stream with the userId as key the tuple (id, count of plays for user) as value
     */
    private KStream<Long, ChartRecord> createCountUpdateStream(final KStream<Long, ListeningEvent> inputStream,
            final FieldHandler fieldHandler, final Grouped<CompositeKey, Long> groupedSerde,
            final Produced<Long, ChartRecord> producedSerde) {

        // get counts for tuple (userId, [artist|album|track]Id) as KTable
        final KTable<CompositeKey, Long> fieldCountsPerUser = inputStream
                .map((key, event) -> KeyValue.pair(key, fieldHandler.extractId(event)))
                .groupBy(CompositeKey::new, groupedSerde)
                .count();

        // create a stream of counts per user and field and repartition it so that the count for userId is on
        // the same partition as the event.
        // To trigger the repartition, it is necessary to call through()
        final KStream<Long, ChartRecord> countUpdateStream = fieldCountsPerUser
                .toStream()
                .map((key, count) ->
                        KeyValue.pair(
                                key.getPrimaryKey(),
                                new ChartRecord(key.getSecondaryKey(), count)
                        ))
                .through(COUNT_TOPIC_PREFIX + fieldHandler.type().toString().toLowerCase(), producedSerde);

        return countUpdateStream;
    }

    private ProfileStoreSerDeCollection createSerDes(final String schemaRegistryUrl) {
        final Map<String, String> serdeConfig =
                Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        final SpecificAvroSerde<ListeningEvent> listeningEventSerDe = new SpecificAvroSerde<>();
        listeningEventSerDe.configure(serdeConfig, false);

        final SpecificAvroSerde<CompositeKey> compositeKeySerDe = new SpecificAvroSerde<>();
        compositeKeySerDe.configure(serdeConfig, true);

        final SpecificAvroSerde<UserProfile> userProfileSerDe = new SpecificAvroSerde<>();
        userProfileSerDe.configure(serdeConfig, false);

        final SpecificAvroSerde<ChartRecord> chartRecordSerDe = new SpecificAvroSerde<>();
        chartRecordSerDe.configure(serdeConfig, false);

        final SpecificAvroSerde<FieldRecord> fieldRecordSerDe = new SpecificAvroSerde<>();
        fieldRecordSerDe.configure(serdeConfig, false);

        return new ProfileStoreSerDeCollection(Serdes.Long(), listeningEventSerDe, compositeKeySerDe, userProfileSerDe,
                chartRecordSerDe, fieldRecordSerDe);
    }

    @Value
    private static class ProfileStoreSerDeCollection {
        final Serde<Long> longSerDe;
        final SpecificAvroSerde<ListeningEvent> listeningEventSerDe;
        final SpecificAvroSerde<CompositeKey> compositeKeySerDe;
        final SpecificAvroSerde<UserProfile> userProfileSerDe;
        final SpecificAvroSerde<ChartRecord> chartRecordSerDe;
        final SpecificAvroSerde<FieldRecord> fieldRecordSerDe;
    }

    @Value(staticConstructor = "of")
    private static class GlobalTableData {
        final FieldType fieldType;
        final String inputTopic;
        final String storeName;
    }

}
