package com.bakdata.profilestore.generator;

import static net.sourceforge.argparse4j.impl.Arguments.store;

import com.bakdata.profilestore.common.avro.FieldRecord;
import com.bakdata.profilestore.common.avro.ListeningEvent;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.joda.time.DateTime;

public final class GeneratorMain {
    static final String DATAPATH_PREFIX = "data/LFM-1b_";
    static final String TRACKSFILENAME = "tracks.txt";
    static final String ALBUMSFILENAME = "albums.txt";
    static final String ARTISTSFILENAME = "artists.txt";
    static final String LISTENINGEVENTSFILENAME = "LEs.txt";


    public static void main(final String[] args) {
        final ArgumentParser parser = argParser();

        try {
            final Namespace res = parser.parseArgs(args);
            final String bootstrapServer = res.getString("bootstrapServer");
            final String schemaRegistryUrl = res.getString("schemaRegistryUrl");
            final String listeningEventTopic = res.getString("listeningEventTopic");
            final String albumTopic = res.getString("albumTopic");
            final String artistTopic = res.getString("artistTopic");
            final String trackTopic = res.get("trackTopic");
            final boolean shouldDrawRandom = res.getBoolean("shouldDrawRandom");

            final Properties props = new Properties();
            props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

            final Map<String, String> serdeConfig = Collections.singletonMap(
                    AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

            final SpecificAvroSerializer<ListeningEvent> listeningEventSerializer = new SpecificAvroSerializer<>();
            listeningEventSerializer.configure(serdeConfig, false);

            final KafkaProducer<Long, ListeningEvent> listeningEventProducer =
                    new KafkaProducer<Long, ListeningEvent>(props,
                            Serdes.Long().serializer(), listeningEventSerializer);

            System.out.println("loading artists..");
            final List<FieldRecord> artists =
                    loadFile(ARTISTSFILENAME, strings -> new FieldRecord(Long.parseLong(strings[0]),
                            strings[1]));
            System.out.println("loading albums..");
            final List<FieldRecord> albums =
                    loadFile(ALBUMSFILENAME, strings -> new FieldRecord(Long.parseLong(strings[0]),
                            strings[1]));
            System.out.println("loading tracks..");
            final List<FieldRecord> tracks =
                    loadFile(TRACKSFILENAME, strings -> new FieldRecord(Long.parseLong(strings[0]),
                            strings[1]));
            System.out.println("loading listening events...");
            final List<ListeningEvent> listeningEvents =
                    loadFile(LISTENINGEVENTSFILENAME, strings -> new ListeningEvent(Long.parseLong(strings[0]),
                            Long.parseLong(strings[1]), Long.parseLong(strings[2]), Long.parseLong(strings[3]),
                            new DateTime(Long.parseLong(strings[4]))));

            System.out.println("filling albums topic...");
            fillInformationTopics(albumTopic, albums, props, serdeConfig);
            System.out.println("filling artists topic...");
            fillInformationTopics(artistTopic, artists, props, serdeConfig);
            System.out.println("filling tracks topic...");
            fillInformationTopics(trackTopic, tracks, props, serdeConfig);

            System.out.println("filling listening events topic...");
            if (shouldDrawRandom) {
                fillListeningEventTopicWithRandomDraws(listeningEvents, listeningEventProducer, listeningEventTopic);
            } else {
                fillListeningEventTopic(listeningEvents, listeningEventProducer, listeningEventTopic);
            }


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void fillListeningEventTopic(final List<ListeningEvent> listeningEvents,
            final KafkaProducer<Long, ListeningEvent> listeningEventProducer, final String listeningEventTopic) {
        for (final ListeningEvent listeningEvent : listeningEvents) {
            System.out.println("Writing listening event: " + listeningEvent.getUserId() + ","
                    + listeningEvent.getTrackId() + "," + listeningEvent.getTimestamp());
            listeningEventProducer
                    .send(new ProducerRecord<Long, ListeningEvent>(listeningEventTopic, listeningEvent.getUserId(),
                            listeningEvent));
        }
        listeningEventProducer.close();
    }

    private static void fillListeningEventTopicWithRandomDraws(final List<ListeningEvent> listeningEvents,
            final KafkaProducer<Long, ListeningEvent> listeningEventProducer, final String listeningEventTopic)
            throws InterruptedException {
        final Random random = new Random();
        while (true) {
            final ListeningEvent listeningEvent = listeningEvents.get(random.nextInt(listeningEvents.size()));
            listeningEvent.setUserId(ThreadLocalRandom.current().nextLong(5000));
            listeningEvent.setTimestamp(new DateTime(System.currentTimeMillis() / 1000L)); //override original timestamp
            System.out.println("Writing listening event: " + listeningEvent.getUserId() + ","
                    + listeningEvent.getTrackId() + "," + listeningEvent.getTimestamp());

            listeningEventProducer
                    .send(new ProducerRecord<Long, ListeningEvent>(listeningEventTopic, listeningEvent.getUserId(),
                            listeningEvent));
            Thread.sleep(100L);
        }
    }


    private static <T extends SpecificRecord> void fillInformationTopics(final String topicName, final List<T> events,
            final Properties props, final Map<String, String> serdeConfig) {
        final SpecificAvroSerializer<T> eventSerializer = new SpecificAvroSerializer<T>();
        eventSerializer.configure(serdeConfig, false);
        final KafkaProducer<Long, T> eventProducer =
                new KafkaProducer<Long, T>(props, Serdes.Long().serializer(), eventSerializer);

        Long count = 0L;
        for (final T nextEvent : events) {
            count++;
            System.out.println("Writing field record: " + nextEvent.toString());
            eventProducer.send(new ProducerRecord<>(topicName, count, nextEvent));
        }
        eventProducer.close();

    }

    private static <T> List<T> loadFile(final String filename, final Function<String[], T> mapper) throws IOException {
        final List<T> elements = new ArrayList<>();
        final InputStream inputStream = GeneratorMain.class.getClassLoader()
                .getResourceAsStream(DATAPATH_PREFIX + filename);
        final InputStreamReader streamReader = new InputStreamReader(inputStream);
        final BufferedReader br = new BufferedReader(streamReader);
        String line;
        while ((line = br.readLine()) != null) {
            final String[] values = line.split("\t");
            final T element = mapper.apply(values);
            elements.add(element);
        }

        streamReader.close();
        return elements;
    }


    private static ArgumentParser argParser() {
        final ArgumentParser parser = ArgumentParsers
                .newFor("music_streams").build()
                .defaultHelp(true);

        parser.addArgument("--bootstrap-server")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("BOOTSTRAP-SERVER")
                .dest("bootstrapServer");

        parser.addArgument("--schema-registry-url")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("SCHEMA-REGISTRY-URL")
                .dest("schemaRegistryUrl");

        parser.addArgument("--listening-event-topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("LISTENING-EVENT-TOPIC")
                .dest("listeningEventTopic");

        parser.addArgument("--album-topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("ALBUM-TOPIC")
                .dest("albumTopic");

        parser.addArgument("--track-topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TRACK-TOPIC")
                .dest("trackTopic");

        parser.addArgument("--artist-topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("ARTIST-TOPIC")
                .dest("artistTopic");

        parser.addArgument("--random-drawing")
                .action(store())
                .required(false)
                .type(Boolean.class)
                .setDefault(false)
                .metavar("RANDOM-DRAWING")
                .dest("shouldDrawRandom");

        return parser;
    }

}
