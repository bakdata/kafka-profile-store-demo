package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.ProfilestoreMain;
import com.bakdata.profilestore.core.avro.UserProfile;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class ProfileChecker implements Transformer<Long, ListeningEvent, KeyValue<Long, ListeningEvent>> {
    private KeyValueStore<Long, UserProfile> profileStore;

    @Override
    public void init(final ProcessorContext processorContext) {
        this.profileStore =
                (KeyValueStore<Long, UserProfile>) processorContext.getStateStore(ProfilestoreMain.PROFILE_STORE_NAME);
    }

    @Override
    public KeyValue<Long, ListeningEvent> transform(final Long userId, final ListeningEvent listeningEvent) {
        final UserProfile profile = this.profileStore.get(userId);
        // add default user profile so that the other processors don't have to check for NPE
        if (profile == null) {
            final UserProfile userProfile = new UserProfile(0L, listeningEvent.getTimestamp(), listeningEvent.getTimestamp(),
                    Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
            this.profileStore.put(userId, userProfile);
        }
        return KeyValue.pair(userId, listeningEvent);
    }


    @Override
    public void close() {

    }
}
