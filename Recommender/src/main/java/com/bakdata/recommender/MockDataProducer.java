package com.bakdata.recommender;

import com.bakdata.recommender.avro.ListeningEvent;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Slf4j
@Command(name = "mock-data-producer")
public class MockDataProducer implements Callable<Void> {
    private static final long TIMEOUT = 100L;

    @CommandLine.Option(names = "--bootstrap-server", defaultValue = "localhost:29092")
    private String bootstrapServers;

    @CommandLine.Option(names = "--schema-registry-url", defaultValue = "http://localhost:8081")
    private String schemaRegistryUrl;

    @CommandLine.Option(names = "--topic", defaultValue = "listening-events")
    private String topic;

    @Override
    public Void call() throws Exception {

        log.info("Connecting to Kafka cluster via bootstrap servers {}", this.bootstrapServers);
        log.info("Connecting to Confluent schema registry at {}", this.schemaRegistryUrl);

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);

        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryUrl);
        final SpecificAvroSerializer<ListeningEvent> specificAvroSerializer = new SpecificAvroSerializer<>();
        specificAvroSerializer.configure(serdeConfig, false);

        final KafkaProducer<String, ListeningEvent> eventProducer = new KafkaProducer<>(props,
                Serdes.String().serializer(),
                specificAvroSerializer);

        while (true) {
            final ListeningEvent listeningEvent = new ListeningEvent(ThreadLocalRandom.current().nextLong(100),
                    ThreadLocalRandom.current().nextLong(200),
                    ThreadLocalRandom.current().nextLong(300),
                    ThreadLocalRandom.current().nextLong(400),
                    Instant.now());
            eventProducer.send(new ProducerRecord<>(this.topic, "", listeningEvent));
            Thread.sleep(TIMEOUT);
        }
    }

    public static void main(final String[] args) {
        System.exit(new CommandLine(new MockDataProducer()).execute(args));
    }
}
