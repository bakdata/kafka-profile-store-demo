package com.bakdata.profilestore.core.processor;

import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.ProfilestoreMain;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ProcessorBaseTest {
    private final ProfilestoreMain main = new ProfilestoreMain();

    @RegisterExtension
    final TestTopologyExtension<String, ListeningEvent> testTopology =
            new TestTopologyExtension<String, ListeningEvent>(this.main::buildTopology, this.main.getProperties());
}
