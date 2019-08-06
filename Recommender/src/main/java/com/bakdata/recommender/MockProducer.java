package com.bakdata.recommender;

import com.bakdata.recommender.avro.ListeningEvent;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockProducer {
    private static final Logger log = LoggerFactory.getLogger(MockProducer.class);

    public static void main(final String[] args) throws Exception {
        String bootstrapServers;
        String schemaRegistryUrl;
        String topic;

        if (args.length != 3) {
            bootstrapServers = "localhost:29092";
            schemaRegistryUrl = "http://localhost:8081";
            topic = "listening-events";
        } else {
            bootstrapServers = args[0];
            schemaRegistryUrl = args[1];
            topic = args[2];
        }

        log.info("Connecting to Kafka cluster via bootstrap servers {}", bootstrapServers);
        log.info("Connecting to Confluent schema registry at {}", schemaRegistryUrl);

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        final SpecificAvroSerializer<ListeningEvent> edgeSpecificAvroSerializer = new SpecificAvroSerializer<>();
        edgeSpecificAvroSerializer.configure(serdeConfig, false);

        final KafkaProducer<String, ListeningEvent> eventProducer = new KafkaProducer<>(props,
                Serdes.String().serializer(),
                edgeSpecificAvroSerializer);

        while (true) {
            ListeningEvent listeningEvent = new ListeningEvent(ThreadLocalRandom.current().nextLong(100),
                    ThreadLocalRandom.current().nextLong(200),
                    ThreadLocalRandom.current().nextLong(300),
                    ThreadLocalRandom.current().nextLong(400),
                    Instant.now());
            System.out.println("alds");
            eventProducer.send(
                    new ProducerRecord<>(topic,
                            "", listeningEvent));
            Thread.sleep(100L);
        }
    }
}
