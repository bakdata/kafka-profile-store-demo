package com.bakdata.profilestore.core.fields;

import com.bakdata.profilestore.common.FieldType;
import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.avro.ChartTuple;
import com.bakdata.profilestore.core.avro.UserProfile;
import java.util.List;

public class AlbumHandler extends FieldHandler {

    public AlbumHandler() {
        super(FieldType.ALBUM);
    }

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
    public FieldType type() {
        return FieldType.ALBUM;
    }
}
