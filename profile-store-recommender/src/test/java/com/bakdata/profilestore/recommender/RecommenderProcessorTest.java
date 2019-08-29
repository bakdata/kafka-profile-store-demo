package com.bakdata.profilestore.recommender;

import com.bakdata.fluent_kafka_streams_tests.TestInput;
import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.recommender.graph.BipartiteGraph;
import com.bakdata.profilestore.recommender.graph.KeyValueGraph;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Properties;
import java.util.stream.LongStream;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class RecommenderProcessorTest {
    static final String LISTENING_EVENT_INPUT = "listening-events";
    static final String ARTIST_INPUT = "artist-input";
    static final String ALBUM_INPUT = "album-input";
    static final String TRACK_INPUT = "track-input";
    private final RecommenderMain main =
            new RecommenderMain(LISTENING_EVENT_INPUT, ARTIST_INPUT, ALBUM_INPUT, TRACK_INPUT);

    @RegisterExtension
    final TestTopologyExtension<String, ListeningEvent> testTopology =
            new TestTopologyExtension<>(this.main::buildTopology, this.getProperties());

    @BeforeEach
    void fillTables() {
        for (final String input : Arrays.asList(ARTIST_INPUT, ALBUM_INPUT, TRACK_INPUT)) {
            final TestInput<Long, String> globalInput = testTopology.input(input).withSerde(Serdes.Long(), Serdes.String());
            LongStream.range(0, 50).forEach(i -> globalInput.add(i, input + i));
        }
    }


    @Test
    void testAlbumSingleInput() {
        this.testTopology.input(LISTENING_EVENT_INPUT)
                .add(new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()));
        final EnumMap<FieldType, BipartiteGraph> graphMap = this.getGraphMap();

        Assertions.assertEquals(Collections.singletonList(3L),
                graphMap.get(FieldType.ALBUM).getLeftNodeNeighbors(1));
        Assertions.assertEquals(Collections.singletonList(1L),
                graphMap.get(FieldType.ALBUM).getRightNodeNeighbors(3));
    }

    @Test
    void testArtistSingleInput() {
        this.testTopology.input(LISTENING_EVENT_INPUT)
                .add(new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()));
        final EnumMap<FieldType, BipartiteGraph> graphMap = this.getGraphMap();

        Assertions.assertEquals(Collections.singletonList(2L),
                graphMap.get(FieldType.ARTIST).getLeftNodeNeighbors(1));
        Assertions.assertEquals(Collections.singletonList(1L),
                graphMap.get(FieldType.ARTIST).getRightNodeNeighbors(2));
    }

    @Test
    void testTrackSingleInput() {
        this.testTopology.input(LISTENING_EVENT_INPUT)
                .add(new ListeningEvent(1L, 2L, 3L, 4L, Instant.now()));
        final EnumMap<FieldType, BipartiteGraph> graphMap = this.getGraphMap();

        Assertions.assertEquals(Collections.singletonList(4L),
                graphMap.get(FieldType.TRACK).getLeftNodeNeighbors(1));
        Assertions.assertEquals(Collections.singletonList(1L),
                graphMap.get(FieldType.TRACK).getRightNodeNeighbors(4));
    }

    @Test
    void testMultipleInputs() {
        final long[] users = {1, 5, 1, 6, 1};
        final long[] artists = {2, 3, 4, 5, 2};
        final long[] album = {3, 3, 4, 5, 6};
        final long[] track = {4, 8, 2, 8, 7};

        final TestInput<String, ListeningEvent> testInput = this.testTopology.input(LISTENING_EVENT_INPUT);

        for (int i = 0; i < users.length; i++) {
            testInput.add(new ListeningEvent(users[i], artists[i], album[i], track[i], Instant.now()));
        }

        final EnumMap<FieldType, BipartiteGraph> graphMap = this.getGraphMap();

        Assertions.assertEquals(Arrays.asList(3L, 4L, 6L),
                graphMap.get(FieldType.ALBUM).getLeftNodeNeighbors(1));
        Assertions.assertEquals(Collections.singletonList(1L),
                graphMap.get(FieldType.ALBUM).getRightNodeNeighbors(4));

        Assertions.assertEquals(Arrays.asList(2L, 4L, 2L),
                graphMap.get(FieldType.ARTIST).getLeftNodeNeighbors(1));
        Assertions.assertEquals(Arrays.asList(1L, 1L),
                graphMap.get(FieldType.ARTIST).getRightNodeNeighbors(2));

        Assertions.assertEquals(Arrays.asList(4L, 2L, 7L),
                graphMap.get(FieldType.TRACK).getLeftNodeNeighbors(1));
        Assertions.assertEquals(Arrays.asList(5L, 6L),
                graphMap.get(FieldType.TRACK).getRightNodeNeighbors(8));

    }

    EnumMap<FieldType, BipartiteGraph> getGraphMap() {
        final EnumMap<FieldType, BipartiteGraph> graphs =
                new EnumMap<>(FieldType.class);
        for (final FieldType type : FieldType.values()) {
            final BipartiteGraph graph = new KeyValueGraph(
                    this.testTopology.getTestDriver().getKeyValueStore(RecommenderProcessor.getLeftIndexName(type)),
                    this.testTopology.getTestDriver().getKeyValueStore(RecommenderProcessor.getRightIndexName(type))
            );
            graphs.put(type, graph);
        }
        return graphs;
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