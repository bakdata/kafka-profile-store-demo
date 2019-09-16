package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.core.ProfileStoreMain;
import com.bakdata.profilestore.core.avro.UserProfile;
import com.bakdata.profilestore.common.avro.ListeningEvent;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class LastEventProcessor implements Processor<Long, ListeningEvent> {
    private KeyValueStore<Long, UserProfile> profileStore;

    @Override
    public void init(final ProcessorContext processorContext) {
        this.profileStore =
                (KeyValueStore<Long, UserProfile>) processorContext.getStateStore(ProfileStoreMain.PROFILE_STORE_NAME);
    }

    @Override
    public void process(final Long userId, final ListeningEvent listeningEvent) {
        final UserProfile profile = DefaultUserProfile.getOrDefault(this.profileStore.get(userId));
        if (profile.getEventCount() == 0
                || profile.getLastListeningEvent().compareTo(listeningEvent.getTimestamp()) < 0) {
            profile.setLastListeningEvent(listeningEvent.getTimestamp());
        }
        this.profileStore.put(userId, profile);
    }

    @Override
    public void close() {

    }
}
