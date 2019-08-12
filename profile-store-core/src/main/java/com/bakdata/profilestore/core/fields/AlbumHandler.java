package com.bakdata.profilestore.core.ids;

import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.common.FieldType;

public class AlbumIdExtractor implements IdExtractor, FieldUpdater {
    @Override
    public long extractId(final ListeningEvent listeningEvent) {
        return listeningEvent.getAlbumId();
    }



    @Override
    public FieldType type() {
        return FieldType.ALBUM;
    }
}
