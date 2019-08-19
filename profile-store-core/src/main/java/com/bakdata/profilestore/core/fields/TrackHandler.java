package com.bakdata.profilestore.core.fields;

import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.FieldType;
import com.bakdata.profilestore.core.avro.ChartRecord;
import com.bakdata.profilestore.core.avro.UserProfile;
import java.util.List;

public class TrackHandler implements FieldHandler {

    @Override
    public long extractId(final ListeningEvent listeningEvent) {
        return listeningEvent.getTrackId();
    }

    @Override
    public UserProfile updateProfile(final UserProfile userProfile, final List<ChartRecord> charts) {
        userProfile.setTopTenTracks(charts);
        return userProfile;
    }

    @Override
    public List<ChartRecord> getCharts(final UserProfile userProfile) {
        return userProfile.getTopTenTracks();
    }

    @Override
    public FieldType type() {
        return FieldType.TRACK;
    }
}
