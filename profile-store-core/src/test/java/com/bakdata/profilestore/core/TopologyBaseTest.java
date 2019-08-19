package com.bakdata.profilestore.core;

import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import com.bakdata.profilestore.common.avro.ListeningEvent;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.extension.RegisterExtension;

public abstract class TopologyBaseTest {
    private final ProfilestoreMain main = new ProfilestoreMain();

    @RegisterExtension
    protected final TestTopologyExtension<Long, ListeningEvent> testTopology =
            new TestTopologyExtension<>(this.main::buildTopology, this.getProperties());

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
