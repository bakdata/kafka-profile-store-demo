package com.bakdata.profilestore.core.fields;


import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.FieldType;
import com.bakdata.profilestore.core.avro.ChartTuple;
import com.bakdata.profilestore.core.avro.UserProfile;
import java.util.List;

public interface FieldHandler {
    UserProfile updateProfile(UserProfile userProfile, List<ChartTuple> charts);

    List<ChartTuple> getCharts(UserProfile userProfile);

    long extractId(ListeningEvent listeningEvent);

    FieldType type();
}
