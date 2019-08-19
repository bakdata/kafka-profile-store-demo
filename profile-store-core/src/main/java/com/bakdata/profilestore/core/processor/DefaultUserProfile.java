package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.core.avro.UserProfile;
import java.time.Instant;
import java.util.Collections;

public class DefaultUserProfile {
    public static UserProfile getOrDefault(UserProfile userProfile) {
        if (userProfile == null) {
            userProfile = new UserProfile(0L, Instant.now(), Instant.now(),
                    Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        }
        return userProfile;
    }
}
