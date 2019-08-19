package com.bakdata.profilestore.recommender;

import com.bakdata.profilestore.common.avro.ListeningEvent;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
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
    private final Map<Long, Long> trackToAlbum = new HashMap<>();
    private final Map<Long, Long> albumToArtist = new HashMap<>();

    @CommandLine.Option(names = "--bootstrap-server", defaultValue = "localhost:29092")
    private String bootstrapServers;

    @CommandLine.Option(names = "--schema-registry-url", defaultValue = "http://localhost:8081")
    private String schemaRegistryUrl;

    @CommandLine.Option(names = "--topic", defaultValue = "listening-events")
    private String topic;

    @CommandLine.Option(names = "--user", defaultValue = "200", description = "number of max distinct user")
    private int maxDistinctUser;

    @CommandLine.Option(names = "--artists", defaultValue = "200", description = "number of max distinct artists")
    private int maxDistinctArtists;

    @CommandLine.Option(names = "--albums", defaultValue = "500", description = "number of max distinct albums")
    private int maxDistinctAlbums;

    @CommandLine.Option(names = "--tracks", defaultValue = "1000", description = "number of max distinct tracks")
    private int maxDistinctTracks;

    @CommandLine.Option(names = "--delay", defaultValue = "900", description = "max delay for processing time")
    private int delay;

    @Override
    public Void call() throws Exception {

        log.info("Connecting to Kafka cluster via bootstrap servers {}", this.bootstrapServers);
        log.info("Connecting to schema registry at {}", this.schemaRegistryUrl);

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);

        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryUrl);
        final SpecificAvroSerializer<ListeningEvent> specificAvroSerializer = new SpecificAvroSerializer<>();
        specificAvroSerializer.configure(serdeConfig, false);

        final KafkaProducer<Long, ListeningEvent> eventProducer = new KafkaProducer<>(props,
                Serdes.Long().serializer(),
                specificAvroSerializer);

        while (true) {
            final ListeningEvent listeningEvent = this.createRandomEvent();
            log.info("Write event {}", listeningEvent);

            eventProducer.send(new ProducerRecord<>(this.topic, listeningEvent.getUserId(), listeningEvent));
            Thread.sleep(TIMEOUT);
        }
    }

    private ListeningEvent createRandomEvent() {
        final long trackId = ThreadLocalRandom.current().nextLong(this.maxDistinctTracks);
        final long albumId = this.trackToAlbum.getOrDefault(trackId, ThreadLocalRandom.current().nextLong(
                this.maxDistinctAlbums));
        this.trackToAlbum.putIfAbsent(trackId, albumId);
        final long artistId =
                this.albumToArtist.getOrDefault(albumId, ThreadLocalRandom.current().nextLong(this.maxDistinctArtists));
        this.albumToArtist.putIfAbsent(albumId, trackId);

        return new ListeningEvent(ThreadLocalRandom.current().nextLong(this.maxDistinctUser), artistId, albumId, trackId,
                Instant.now().minusMillis(ThreadLocalRandom.current().nextLong(this.delay)));
    }


    public static void main(final String[] args) {
        System.exit(new CommandLine(new MockDataProducer()).execute(args));
    }
}
