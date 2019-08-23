package com.bakdata.profilestore.rest;

import java.net.SocketException;
import java.net.URI;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
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
    private final Set<Object> resources;
    private Server server;

    public RestService(final HostInfo hostInfo, final Object... resources) {
        this.hostInfo = hostInfo;
        this.resources = Set.of(resources);
    }

    public void start() throws Exception {
        final ServletContextHandler contextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        contextHandler.setContextPath("/");

        this.server = new Server();
        this.server.setHandler(contextHandler);

        final ResourceConfig config = new ResourceConfig();
        config.register(JacksonFeature.class);
        this.resources.forEach(config::register);

        final ServletContainer container = new ServletContainer(config);
        final ServletHolder holder = new ServletHolder(container);
        contextHandler.addServlet(holder, "/*");

        final ServerConnector connector = new ServerConnector(this.server);
        connector.setHost(this.hostInfo.host());
        // if port is 0, the connector will automatically find a free port
        if (this.hostInfo.port() != 0) {
            connector.setPort(this.hostInfo.port());
        }
        this.server.addConnector(connector);

        contextHandler.start();
        log.info("Server started");

        try {
            this.server.start();
        } catch (final SocketException exception) {
            log.error("Unavailable: {} : {}", this.hostInfo.host(), this.hostInfo.port());
            throw new Exception(exception.toString());
        }
    }

    public void join() {
        try {
            this.server.join();
        } catch (InterruptedException e) {
            log.error("Server could not join thread", e);
        }
    }

    public void stop() throws Exception {
        if (this.server != null) {
            this.server.stop();
            log.info("Server stopped");
        }
    }

    public URI getHost() {
        return this.server.getURI();
    }
}
