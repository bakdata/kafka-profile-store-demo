package com.bakdata.profilestore.rest;

import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.state.HostInfo;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Slf4j
@Command(name = "rest-service", mixinStandardHelpOptions = true, description = "Start REST service for profile-store")
public class RestApplication implements Callable<Void> {
    @Option(names = "--host", required = true)
    private String host;
    @Option(names = "--port", required = true)
    private int port;
    @Option(names = "--profile-store", required = true)
    private String profileStoreAddress;
    @Option(names = "--recommender", required = true)
    private String recommenderAddress;

    public static void main(final String[] args) {
        System.exit(new CommandLine(new RestApplication()).execute(args));
    }

    @Override
    public Void call() throws Exception {
        final HostInfo restHost = new HostInfo(this.host, this.port);
        final HostInfo profileStoreInfo =
                new HostInfo(Utils.getHost(this.profileStoreAddress), Utils.getPort(this.profileStoreAddress));
        final HostInfo recommenderHostInfo =
                new HostInfo(Utils.getHost(this.recommenderAddress), Utils.getPort(this.recommenderAddress));

        final RestService restService =
                new RestService(restHost, new RestResource(profileStoreInfo, recommenderHostInfo));
        restService.start();
        log.info("REST Server started");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                restService.stop();
            } catch (final Exception e) {
                log.warn("Error in shutdown", e);
            }
        }));

        return null;
    }
}
