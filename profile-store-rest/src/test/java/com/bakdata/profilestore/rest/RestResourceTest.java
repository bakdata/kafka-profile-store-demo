package com.bakdata.profilestore.rest;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
class RestResourceTest {
    private static final Pattern PORT_PATTERN = Pattern.compile("PORT");
    private WireMockServer wireMockServer;
    private RestService restService;
    private final String recommendations;
    private final String profile;
    private final String hosts;

    RestResourceTest() throws Exception {
        this.recommendations =
                Files.readString(Paths.get("src/test/resources/data/recommendations.json"), StandardCharsets.UTF_8);
        this.profile = Files.readString(Paths.get("src/test/resources/data/userProfile.json"), StandardCharsets.UTF_8);
        this.hosts = Files.readString(Paths.get("src/test/resources/data/hosts.json"), StandardCharsets.UTF_8);
    }

    @BeforeEach
    void setup() throws Exception {
        this.wireMockServer = new WireMockServer(wireMockConfig().dynamicPort());
        this.wireMockServer.start();

        final HostInfo serviceHostInfo = new HostInfo("localhost", 0);
        final HostInfo mockInfo = new HostInfo("localhost", this.wireMockServer.port());

        this.restService = new RestService(serviceHostInfo, new RestResource(mockInfo, mockInfo));
        this.restService.start();
        this.setupStub();
    }

    @AfterEach
    void teardown() throws Exception {
        this.wireMockServer.stop();
        this.restService.stop();
    }

    @Test
    void testRecommendation() {
        final List<Integer> expected = new JsonPath(this.recommendations).getList("$", Integer.class);

        given()
                .port(this.restService.getHost().getPort())
                .contentType(ContentType.JSON)
                .when()
                .get("recommendation/24/track")
                .then()
                .body("$", contains(expected.toArray()));
    }

    @Test
    void testUserProfile() {
        final JsonPath jsonPath = new JsonPath(this.profile);
        final List<Integer> topTenArtists = jsonPath.getList("topTenArtist");
        final List<Integer> topTenAlbums = jsonPath.getList("topTenAlbums");
        final List<Integer> topTenTracks = jsonPath.getList("topTenTracks");
        final String firstEvent = jsonPath.getString("firstListeningEvent");
        final String lastEvent = jsonPath.getString("lastListeningEvent");
        final int eventCount = jsonPath.getInt("eventCount");

        given()
                .port(this.restService.getHost().getPort())
                .when()
                .get("profile/24")
                .then()
                .body("topTenArtist", contains(topTenArtists.toArray()))
                .and()
                .body("topTenAlbums", contains(topTenAlbums.toArray()))
                .and()
                .body("topTenTracks", contains(topTenTracks.toArray()))
                .and()
                .body("firstListeningEvent", equalTo(firstEvent))
                .and()
                .body("lastListeningEvent", equalTo(lastEvent))
                .and()
                .body("eventCount", equalTo(eventCount));

    }

    private void setupStub() {
        this.wireMockServer.stubFor(get(urlMatching("/recommendation/([0-9]+)/(track|artist|album)"))
                .willReturn(aResponse().withHeader("Content-Type", MediaType.APPLICATION_JSON)
                        .withStatus(Status.OK.getStatusCode())
                        .withBody(this.recommendations)));

        this.wireMockServer.stubFor(get(urlMatching("/profile/([0-9]+)"))
                .willReturn(aResponse().withHeader("Content-Type", MediaType.APPLICATION_JSON)
                        .withStatus(Status.OK.getStatusCode())
                        .withBody(this.profile)));

        this.wireMockServer.stubFor(get(urlEqualTo("/applications/all"))
                .willReturn(aResponse().withHeader("Content-Type", MediaType.APPLICATION_JSON)
                        .withStatus(Status.OK.getStatusCode())
                        .withBody(PORT_PATTERN.matcher(this.hosts).replaceAll(
                                String.valueOf(this.wireMockServer.port())))));
    }

}