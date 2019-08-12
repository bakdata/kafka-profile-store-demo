package com.bakdata.profilestore.core.ids;

import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.FieldType;

public class TrackIdExtractor implements IdExtractor {
    @Override
    public long extractId(final ListeningEvent listeningEvent) {
        return listeningEvent.getTrackId();
    }

    @Override
    public FieldType type() {
        return FieldType.TRACK;
    }
}
