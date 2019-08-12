package com.bakdata.profilestore.core;

import com.bakdata.profilestore.core.avro.CompositeKey;
import com.bakdata.profilestore.core.avro.UserProfile;
import com.bakdata.profilestore.core.ids.AlbumIdExtractor;
import com.bakdata.profilestore.core.ids.ArtistIdExtractor;
import com.bakdata.profilestore.core.ids.IdExtractor;
import com.bakdata.profilestore.core.ids.TrackIdExtractor;
import com.bakdata.profilestore.core.processor.CheckProfileProcessor;
import com.bakdata.profilestore.core.processor.FirstEventProcessor;
import com.bakdata.profilestore.core.processor.LastEventProcessor;
import com.bakdata.profilestore.core.processor.TopKProcessor;
import com.bakdata.profilestore.core.serde.TopKListSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

public class ProfilestoreTopology extends Topology {
    public static final String PROFILE_STORE_NAME = "profile_store";
    private static final String CHECK_PROCESSOR_NAME = "check_profile_for_null_processor";
    private static final String LAST_EVENT_PROCESSOR_NAME = "last_event_processor";
    private static final String FIRST_EVENT_PROCESSOR_NAME = "first_event_processor";
    private static final String SOURCE_NAME = "input";

    private final String inputTopic;

    private final IdExtractor[] idExtractors =
            {new AlbumIdExtractor(), new ArtistIdExtractor(), new TrackIdExtractor()};

    private final SpecificAvroSerde<CompositeKey> compositeKeySerde;
    private final SpecificAvroSerde<UserProfile> userProfileSerde;


    public ProfilestoreTopology(
            final String inputTopic,
            final SpecificAvroSerde<CompositeKey> compositeKeySerde,
            final SpecificAvroSerde<UserProfile> userProfileSerde) {
        this.inputTopic = inputTopic;
        this.compositeKeySerde = compositeKeySerde;
        this.userProfileSerde = userProfileSerde;
        this.build();
    }

    private void build() {
        this.addSource();
        this.addProfileStore();
        this.addCheckProfileProcessor();
        this.addTopKProcessor();
        this.addFirstListeningEventProcessor();
        this.addLastListeningEventProcessor();
    }

    private void addSource() {
        this.addSource(SOURCE_NAME, this.inputTopic);
    }

    private void addProfileStore() {
        this.addStateStore(
                Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(PROFILE_STORE_NAME), Serdes.Long(),
                        this.userProfileSerde));
    }

    private void addCheckProfileProcessor() {
        this.addProcessor(CHECK_PROCESSOR_NAME, CheckProfileProcessor::new, SOURCE_NAME)
                .connectProcessorAndStateStores(CHECK_PROCESSOR_NAME, PROFILE_STORE_NAME);
    }

    private void addFirstListeningEventProcessor() {
        this.addProcessor(FIRST_EVENT_PROCESSOR_NAME, FirstEventProcessor::new, SOURCE_NAME)
                .connectProcessorAndStateStores(FIRST_EVENT_PROCESSOR_NAME, PROFILE_STORE_NAME);
    }

    private void addLastListeningEventProcessor() {
        this.addProcessor(LAST_EVENT_PROCESSOR_NAME, LastEventProcessor::new, SOURCE_NAME)
                .connectProcessorAndStateStores(LAST_EVENT_PROCESSOR_NAME, PROFILE_STORE_NAME);
    }

    private void addTopKProcessor() {
        final Serde<Long> longSerde = Serdes.Long();
        final Serde<TopKList> topKListSerde = new TopKListSerde();

        for (final IdExtractor extractor : this.idExtractors) {
            final String processorName = TopKProcessor.getProcessorName(extractor.type());
            this.addProcessor(processorName, () -> new TopKProcessor(10, extractor), SOURCE_NAME)
                    .addStateStore(Stores.keyValueStoreBuilder(
                            Stores.inMemoryKeyValueStore(TopKProcessor.getCountStoreName(extractor.type())),
                            this.compositeKeySerde, longSerde), processorName)
                    .addStateStore(Stores.keyValueStoreBuilder(
                            Stores.inMemoryKeyValueStore(TopKProcessor.getChartStoreName(extractor.type())),
                            longSerde, topKListSerde), processorName);
        }
    }

}
