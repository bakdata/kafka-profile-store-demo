package com.bakdata.profilestore.recommender.rest;

import com.bakdata.profilestore.recommender.FieldType;
import com.bakdata.profilestore.recommender.graph.BipartiteGraph;
import java.net.SocketException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

@Slf4j
public class RestService {
    private final HostInfo hostInfo;
    private final RestResource resource;
    private Server server;

    public RestService(final HostInfo hostInfo, final Map<FieldType, BipartiteGraph> graphs,
            final KafkaStreams kafkaStreams, final Map<FieldType, String> storeNames) {
        this.hostInfo = hostInfo;
        this.resource = new RestResource(graphs, kafkaStreams, storeNames);
    }

    public void start() throws Exception {
        final ServletContextHandler contextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        contextHandler.setContextPath("/");

        this.server = new Server();
        this.server.setHandler(contextHandler);

        final ResourceConfig config = new ResourceConfig();
        config.register(JacksonFeature.class);
        config.register(this.resource);

        final ServletContainer container = new ServletContainer(config);
        final ServletHolder holder = new ServletHolder(container);
        contextHandler.addServlet(holder, "/*");

        final ServerConnector connector = new ServerConnector(this.server);
        connector.setHost(this.hostInfo.host());
        connector.setPort(this.hostInfo.port());
        this.server.addConnector(connector);

        contextHandler.start();
        log.info("Server started");

        try {
            this.server.start();
            this.server.join();
        } catch (final SocketException exception) {
            log.error("Unavailable: {} : {}", this.hostInfo.host(), this.hostInfo.port());
            throw new Exception(exception.toString());
        }
    }

    public void stop() throws Exception {
        if (this.server != null) {
            this.server.stop();
        }
    }
}