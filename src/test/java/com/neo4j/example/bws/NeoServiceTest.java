package com.neo4j.example.bws;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class NeoServiceTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static GraphDatabaseService graphDatabaseService;
    private static NeoService service;

    @Before
    public void setUp() {
        graphDatabaseService = new TestGraphDatabaseFactory().newImpermanentDatabase();
        service = new NeoService(graphDatabaseService);
        service.initialize(graphDatabaseService);
        // For testing purposes we have to invalidate the caches otherwise it leaves garbage node ids around.
        service.userCache.invalidateAll();
        service.siteCache.invalidateAll();
    }

    @After
    public void tearDown() throws Exception {
        graphDatabaseService.shutdown();

    }

    @Test
    public void shouldRespondToHelloWorld() throws IOException  {
        Response response = service.helloWorld();
        HashMap actual = objectMapper.readValue((String) response.getEntity(), HashMap.class);
        assertEquals(expected, actual);
    }

    private static final HashMap expected = new HashMap<String, Object>() {{
        put("hello", "world");
    }};


    @Test
    public void shouldWarmUp() {
        assertEquals("Warmed up and ready to go!", service.warmUp(graphDatabaseService));
    }

    @Test
    public void shouldCreateViewed() throws IOException {
        Response response = service.userVisited(objectMapper.writeValueAsString(request), "1234", graphDatabaseService);
        int code = response.getStatus();

        assertEquals(201, code);
    }

    @Test
    public void shouldCreateViewedTwice() throws IOException {
        service.userVisited(objectMapper.writeValueAsString(request), "1234", graphDatabaseService);
        Response response = service.userVisited(objectMapper.writeValueAsString(request), "1234", graphDatabaseService);
        int code = response.getStatus();

        assertEquals(201, code);
    }


    @Test
    public void shouldNotCreateViewedBadInput() throws IOException {
        Response response = service.userVisited("343:asdf3:#43", "1234", graphDatabaseService);
        int code = response.getStatus();
        String actual = objectMapper.readValue((String) response.getEntity(), String.class);

        assertEquals(400, code);
        assertEquals("Error parsing input 343:asdf3:#43", actual);
    }

    @Test
    public void shouldNotCreateViewedNoUrl() throws IOException {
        Response response = service.userVisited("{\"lru\": \"http://neo4j.com\"}", "1234", graphDatabaseService);
        int code = response.getStatus();
        String actual = objectMapper.readValue((String) response.getEntity(), String.class);

        assertEquals(400, code);
        assertEquals("Missing URL Parameter {\"lru\": \"http://neo4j.com\"}", actual);
    }

    @Test
    public void shouldGetUserViewed() throws IOException {
        service.userVisited(objectMapper.writeValueAsString(request), "1234", graphDatabaseService);
        Response response = service.getVisited("1234", 1, graphDatabaseService);
        int code = response.getStatus();
        ArrayList actual = objectMapper.readValue((String) response.getEntity(), ArrayList.class);

        assertEquals(200, code);
        assertEquals(request.get("url"), actual.get(0));
    }


    @Test
    public void shouldCreateViewedAsync() throws IOException, InterruptedException {
        Response response = service.asyncUserVisited(objectMapper.writeValueAsString(request), "1234", graphDatabaseService);
        int code = response.getStatus();

        assertEquals(201, code);
        Random rn = new Random();
        for(int i = 0; i < 50_000; i++){
            String userId = String.valueOf(rn.nextInt(1001));
            String urlHash = "{\"url\": \"http://" + String.valueOf(rn.nextInt(1001)) + ".neo4j.com\"}";
            service.asyncUserVisited(urlHash, userId, graphDatabaseService);
        }
        Thread.sleep(25000);

    }


    public static final HashMap<String, Object> request =
            new HashMap<String, Object>(){{
                put("url", "http://www.neo4j.org");
            }};


}
