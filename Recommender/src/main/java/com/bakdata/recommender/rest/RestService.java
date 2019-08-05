package com.bakdata.recommender.rest;

import com.bakdata.recommender.graph.BipartiteGraph;
import com.bakdata.recommender.RecommendationType;
import java.net.SocketException;
import java.util.Map;
import org.apache.kafka.streams.state.HostInfo;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RestService {
    private final HostInfo hostInfo;
    private final RestResource resource;
    private Server server;

    private static final Logger log = LoggerFactory.getLogger(RestService.class);

    public RestService(HostInfo hostInfo, Map<RecommendationType, BipartiteGraph> graphs) {
        this.hostInfo = hostInfo;
        this.resource = new RestResource(graphs);

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
        } catch (SocketException e) {
            log.error("Unavailable: {} : {}", this.hostInfo.host(), this.hostInfo.port());
            throw new Exception(e.toString());
        }
    }

    public void stop() throws Exception {
        if (this.server != null) {
            this.server.stop();
        }
    }
}