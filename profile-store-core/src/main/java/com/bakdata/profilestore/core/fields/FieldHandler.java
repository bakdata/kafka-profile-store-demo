package com.bakdata.profilestore.core.fields;


import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.FieldType;
import com.bakdata.profilestore.core.avro.NamedChartRecord;
import com.bakdata.profilestore.core.avro.UserProfile;
import java.util.List;

public interface FieldHandler {
    UserProfile updateProfile(UserProfile userProfile, List<NamedChartRecord> charts);

    List<NamedChartRecord> getCharts(UserProfile userProfile);

    long extractId(ListeningEvent listeningEvent);

    FieldType type();
}
