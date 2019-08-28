package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.core.ProfileStoreMain;
import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.avro.UserProfile;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class EventCountProcessor implements Processor<Long, ListeningEvent> {
    private KeyValueStore<Long, UserProfile> profileStore;

    @Override
    public void init(final ProcessorContext processorContext) {
        this.profileStore =
                (KeyValueStore<Long, UserProfile>) processorContext.getStateStore(ProfileStoreMain.PROFILE_STORE_NAME);
    }

    @Override
    public void process(final Long userId, final ListeningEvent listeningEvent) {
        final UserProfile userProfile = DefaultUserProfile.getOrDefault(this.profileStore.get(userId));
        final long eventCount = userProfile.getEventCount();
        userProfile.setEventCount(eventCount + 1);
        this.profileStore.put(userId, userProfile);
    }

    @Override
    public void close() {

    }
}
