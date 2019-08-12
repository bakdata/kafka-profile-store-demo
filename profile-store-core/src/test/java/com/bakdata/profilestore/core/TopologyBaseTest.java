package com.bakdata.profilestore.core;

import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import com.bakdata.profilestore.common.avro.ListeningEvent;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TopologyBaseTest {
    private final ProfilestoreMain main = new ProfilestoreMain();

    @RegisterExtension
    protected final TestTopologyExtension<String, ListeningEvent> testTopology =
            new TestTopologyExtension<String, ListeningEvent>(this.main::buildTopology, this.main.getProperties());
}
