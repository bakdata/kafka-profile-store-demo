package com.bakdata.profilestore.core;

import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import com.bakdata.profilestore.common.avro.ListeningEvent;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
public class TopologyBaseTest {
    private final ProfilestoreMain main = new ProfilestoreMain();

    @RegisterExtension
    protected final TestTopologyExtension<Long, ListeningEvent> testTopology =
            new TestTopologyExtension<>(this.main::buildTopology, this.main.getProperties());

}
