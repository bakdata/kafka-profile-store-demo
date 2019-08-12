package com.bakdata.profilestore.core.fields;

import com.bakdata.profilestore.core.avro.ChartTuple;
import com.bakdata.profilestore.core.avro.UserProfile;
import java.util.List;

public interface FieldUpdater {
    UserProfile updateProfile(UserProfile userProfile, List<ChartTuple> charts);
}
