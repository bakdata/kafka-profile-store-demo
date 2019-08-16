package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.core.ProfilestoreMain;
import com.bakdata.profilestore.core.avro.UserProfile;
import com.bakdata.profilestore.common.avro.ListeningEvent;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class CheckProfileProcessor implements Processor<Long, ListeningEvent> {
    private KeyValueStore<Long, UserProfile> profileStore;

    @Override
    public void init(final ProcessorContext processorContext) {
        this.profileStore =
                (KeyValueStore<Long, UserProfile>) processorContext.getStateStore(ProfilestoreMain.PROFILE_STORE_NAME);
    }

    @Override
    public void process(final Long userId, final ListeningEvent listeningEvent) {
        final UserProfile profile = this.profileStore.get(userId);
        if (profile == null) {
            final UserProfile userProfile = new UserProfile(0L, listeningEvent.getTimestamp(), listeningEvent.getTimestamp(),
                    Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
            this.profileStore.put(userId, userProfile);
        }
    }

    @Override
    public void close() {

    }
}
