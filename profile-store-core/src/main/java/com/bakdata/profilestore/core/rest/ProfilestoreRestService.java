package com.bakdata.profilestore.core.rest;

import java.net.SocketException;
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
public class ProfilestoreRestService {
    private final HostInfo hostInfo;
    private final UserProfileResource profileResource;
    private final ApplicationResource applicationResource;
    private Server server;

    public ProfilestoreRestService(final HostInfo hostInfo, final KafkaStreams streams) {
        this.hostInfo = hostInfo;
        this.profileResource = new UserProfileResource(streams, hostInfo);
        this.applicationResource = new ApplicationResource(streams);
    }

    public void start() throws Exception {
        final ServletContextHandler contextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        contextHandler.setContextPath("/");

        this.server = new Server();
        this.server.setHandler(contextHandler);

        final ResourceConfig config = new ResourceConfig();
        config.register(JacksonFeature.class);
        config.register(this.profileResource);
        config.register(this.applicationResource);

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
