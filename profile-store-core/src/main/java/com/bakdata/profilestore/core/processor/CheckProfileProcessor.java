package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.ProfilestoreTopology;
import com.bakdata.profilestore.core.avro.UserProfile;
import java.util.Collections;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class CheckProfileProcessor implements Processor<byte[], ListeningEvent> {
    private KeyValueStore<Long, UserProfile> profileStore;

    @Override
    public void init(final ProcessorContext processorContext) {
        this.profileStore =
                (KeyValueStore<Long, UserProfile>) processorContext.getStateStore(ProfilestoreTopology.PROFILE_STORE_NAME);
    }

    @Override
    public void process(final byte[] bytes, final ListeningEvent listeningEvent) {
        final UserProfile profile = this.profileStore.get(listeningEvent.getUserId());
        if (profile == null) {
            final UserProfile userProfile = new UserProfile(listeningEvent.getTimestamp(), listeningEvent.getTimestamp(),
                    Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
            this.profileStore.put(listeningEvent.getUserId(), userProfile);
        }
    }

    @Override
    public void close() {

    }
}
