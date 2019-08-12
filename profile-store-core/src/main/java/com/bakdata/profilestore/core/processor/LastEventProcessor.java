package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.ProfilestoreMain;
import com.bakdata.profilestore.core.avro.UserProfile;
import java.util.Collections;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class LastEventProcessor implements Processor<byte[], ListeningEvent> {
    private KeyValueStore<Long, UserProfile> profileStore;

    @Override
    public void init(final ProcessorContext processorContext) {
        this.profileStore =
                (KeyValueStore<Long, UserProfile>) processorContext.getStateStore(ProfilestoreMain.PROFILE_STORE_NAME);
    }

    @Override
    public void process(final byte[] bytes, final ListeningEvent listeningEvent) {
        UserProfile profile = this.profileStore.get(listeningEvent.getUserId());

        if (profile == null) {
            profile = new UserProfile(listeningEvent.getTimestamp(), listeningEvent.getTimestamp(),
                    Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        }

        if (profile.getLastListeningEvent() == null
                || profile.getLastListeningEvent().compareTo(listeningEvent.getTimestamp()) < 0) {
            profile.setLastListeningEvent(listeningEvent.getTimestamp());
        }
        this.profileStore.put(listeningEvent.getUserId(), profile);
    }

    @Override
    public void close() {

    }
}
