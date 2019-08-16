package com.bakdata.profilestore.core.fields;

import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.FieldType;
import com.bakdata.profilestore.core.avro.ChartTuple;
import com.bakdata.profilestore.core.avro.UserProfile;
import java.util.List;

public class AlbumHandler implements FieldHandler {
    @Override
    public long extractId(final ListeningEvent listeningEvent) {
        return listeningEvent.getAlbumId();
    }

    @Override
    public UserProfile updateProfile(final UserProfile userProfile, final List<ChartTuple> charts) {
        userProfile.setTopTenAlbums(charts);
        return userProfile;
    }

    @Override
    public List<ChartTuple> getCharts(final UserProfile userProfile) {
        return userProfile.getTopTenAlbums();
    }

    @Override
    public FieldType type() {
        return FieldType.ALBUM;
    }
}