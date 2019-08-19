package com.bakdata.profilestore.core.fields;


import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.FieldType;
import com.bakdata.profilestore.core.avro.ChartRecord;
import com.bakdata.profilestore.core.avro.UserProfile;
import java.util.List;

public interface FieldHandler {
    UserProfile updateProfile(UserProfile userProfile, List<ChartRecord> charts);

    List<ChartRecord> getCharts(UserProfile userProfile);

    long extractId(ListeningEvent listeningEvent);

    FieldType type();
}
