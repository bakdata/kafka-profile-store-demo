package com.bakdata.profilestore.core.ids;

import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.FieldType;

public interface IdExtractor {
    long extractId(ListeningEvent listeningEvent);
    FieldType type();
}
