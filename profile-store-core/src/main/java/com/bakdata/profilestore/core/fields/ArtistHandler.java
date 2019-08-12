package com.bakdata.profilestore.core.ids;

import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.common.FieldType;

public class ArtistIdExtractor implements IdExtractor {
    @Override
    public long extractId(final ListeningEvent listeningEvent) {
        return listeningEvent.getArtistId();
    }

    @Override
    public FieldType type() {
        return FieldType.ARTIST;
    }
}
