package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.core.avro.UserProfile;
import java.time.Instant;
import java.util.Collections;

public class DefaultUserProfile {
    public static UserProfile getOrDefault(UserProfile userProfile) {
        if (userProfile == null) {
            userProfile = new UserProfile(0L,
                    // set timestamps to min and max so that they are overwritten by the corresponding processor
                    // Instant.MIN() and .MAX() result in long overflow in UserProfile
                    Instant.ofEpochMilli(Long.MAX_VALUE),
                    Instant.ofEpochMilli(Long.MIN_VALUE),
                    Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        }
        return userProfile;
    }
}
