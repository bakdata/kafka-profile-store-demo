package com.bakdata.profilestore.core.fields;

import com.bakdata.profilestore.common.avro.ListeningEvent;

public interface IdExtractor {
    long extractId(ListeningEvent listeningEvent);
}
