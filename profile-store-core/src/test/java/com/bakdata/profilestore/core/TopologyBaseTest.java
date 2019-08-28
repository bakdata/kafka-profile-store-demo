package com.bakdata.profilestore.core;

import com.bakdata.fluent_kafka_streams_tests.TestInput;
import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.common.avro.Metadata;
import com.bakdata.profilestore.core.avro.ChartRecord;
import com.bakdata.profilestore.core.avro.NamedChartRecord;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

public abstract class TopologyBaseTest {
    public static final String INPUT_TOPIC = "listening-events";
    public static final int GLOBAL_STORE_SIZE = 50;
    private final ProfilestoreMain main = new ProfilestoreMain();

    @RegisterExtension
    protected final TestTopologyExtension<Long, ListeningEvent> testTopology =
            new TestTopologyExtension<>(props -> this.main.buildTopology(props, INPUT_TOPIC), this.getProperties());

    @BeforeEach
    void fillGlobalTables() {
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                this.testTopology.getSchemaRegistryUrl());

        final SpecificAvroSerde<Metadata> metadataSerde = new SpecificAvroSerde<>();
        metadataSerde.configure(serdeConfig, false);

        for (final String inputTopic : new String[]{this.main.getAlbumTopicName(), this.main.getArtistTopicName(),
                this.main.getTrackTopicName()}) {

            final TestInput<Long, Metadata> input = this.testTopology.input(inputTopic).withSerde(Serdes.Long(), metadataSerde);
            LongStream.range(0, GLOBAL_STORE_SIZE).forEach(i -> input.add(i, new Metadata(i, inputTopic + i)));
        }

    }

    public static List<ChartRecord> namedToUnnamedRecord(final Collection<NamedChartRecord> records) {
        return records.stream()
                .map(namedChartRecord -> new ChartRecord(namedChartRecord.getId(), namedChartRecord.getCountPlays()))
                .collect(Collectors.toList());
    }

    private Properties getProperties() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "profile-topology-test");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "dummy:1234");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, LongSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        return props;
    }

}
