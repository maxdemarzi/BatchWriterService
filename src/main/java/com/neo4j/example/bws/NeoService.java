package com.neo4j.example.bws;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang.ArrayUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.tooling.GlobalGraphOperations;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.*;

import static java.lang.Thread.sleep;

@javax.ws.rs.Path("/service")
public class NeoService {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final PathExpander VISITED_EXPANDER = PathExpanders.forTypeAndDirection(Relationships.VISITED, Direction.OUTGOING);
    private static final PathFinder<Path> ONE_HOP_VISITED_PATH = GraphAlgoFactory.shortestPath(VISITED_EXPANDER, 1);
    public static final Cache<String, Long> userCache = CacheBuilder.newBuilder().maximumSize(10_000_000).build();
    public static final Cache<String, Long> siteCache = CacheBuilder.newBuilder().maximumSize(100_000).build();
    private static final BatchWriterService batchWriterService = BatchWriterService.INSTANCE;

    private static final String ACTION = "action";
    private static final String DATA = "data";

    public NeoService(@Context GraphDatabaseService graphdb){
        batchWriterService.SetGraphDatabase(graphdb);
    }

    @GET
    @javax.ws.rs.Path("/helloworld")
    public String helloWorld() {
        return "Hello World!";
    }

    @GET
    @javax.ws.rs.Path("/warmup")
    public String warmUp(@Context GraphDatabaseService db) {
        try ( Transaction tx = db.beginTx() )
        {
            for (Node siteNode : GlobalGraphOperations.at(db).getAllNodesWithLabel(Labels.Site)){
                siteNode.getPropertyKeys();
                siteCache.put((String) siteNode.getProperty("url"), siteNode.getId());
            }

            for (Node userNode : GlobalGraphOperations.at(db).getAllNodesWithLabel(Labels.User)){
                userNode.getPropertyKeys();
                userCache.put((String) userNode.getProperty("userId"), userNode.getId());
            }

            for ( Relationship relationship : GlobalGraphOperations.at(db).getAllRelationships()){
                relationship.getPropertyKeys();
                relationship.getNodes();
            }
        }
        return "Warmed up and ready to go!";
    }

    @GET
    @javax.ws.rs.Path("/initialize")
    public String initialize(@Context GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            Schema schema = db.schema();
            schema.constraintFor( Labels.User )
                    .assertPropertyIsUnique( "userId" )
                    .create();
            schema.constraintFor( Labels.Site )
                    .assertPropertyIsUnique( "url" )
                    .create();
            tx.success();
        }
        return "Initialized!";
    }

    @POST
    @javax.ws.rs.Path("/{userId}/visited")
    public Response userVisited(String body, @PathParam("userId") String userId, @Context GraphDatabaseService db) throws IOException {
        HashMap input;
        try{
            input = objectMapper.readValue( body, HashMap.class);
        } catch (Exception e) {
            return Response.status(Response.Status.BAD_REQUEST).entity(objectMapper.writeValueAsString("Error parsing input " + body)).build();
        }

        if(!input.containsKey("url")){
            return Response.status(Response.Status.BAD_REQUEST).entity(objectMapper.writeValueAsString("Missing URL Parameter " + body)).build();
        }

        String url = (String)input.get("url");

        try (Transaction tx = db.beginTx()) {
            Long userNodeId = userCache.getIfPresent(userId);
            Node userNode;
            if(userNodeId == null){
                // If the node id is not in the cache, let's try to find the node in the index.
                ResourceIterator<Node> results = db.findNodesByLabelAndProperty(Labels.User, "userId", userId).iterator();
                if (results.hasNext()) {
                    userNode = results.next();
                } else {
                    // If the node is not in the index, then it probably doesn't exist, so we'll create one.
                    UniqueFactory.UniqueNodeFactory userFactory = getUniqueUserFactory(db);
                    userNode = userFactory.getOrCreate( "userId", userId );
                }

                // Add it to the cache.
                userCache.put(userId, userNode.getId());
            } else {
                // If the id is in the index, then just get the node.
                userNode = db.getNodeById(userNodeId);
            }

            Long siteNodeId = siteCache.getIfPresent(url);
            Node siteNode;
            if (siteNodeId == null) {
                // If the node id is not in the cache, let's try to find the node in the index.
                ResourceIterator<Node> results = db.findNodesByLabelAndProperty(Labels.Site, "url", url).iterator();
                if (results.hasNext()) {
                    siteNode = results.next();
                } else {
                    // If the node is not in the index, then it probably doesn't exist, so we'll create one.
                    UniqueFactory.UniqueNodeFactory siteFactory = getUniqueContentFactory(db);
                    siteNode = siteFactory.getOrCreate( "url", url );
                }

                // Add it to the cache.
                siteCache.put(url, siteNode.getId());
            } else {
                siteNode = db.getNodeById(siteNodeId);
            }

            CreateVisitedRelationship(userNode, siteNode);
            tx.success();
        }

        return javax.ws.rs.core.Response.status(javax.ws.rs.core.Response.Status.CREATED).build();
    }

    public static void CreateVisitedRelationship(Node userNode, Node siteNode) {
        Relationship visited;
        org.neo4j.graphdb.Path visitedPath = ONE_HOP_VISITED_PATH.findSinglePath(userNode, siteNode);
        if (visitedPath == null) {
            visited = userNode.createRelationshipTo(siteNode, Relationships.VISITED);
        } else {
            visited = visitedPath.lastRelationship();
        }

        MutableDateTime currently = new MutableDateTime(DateTimeZone.UTC);
        currently.setSecondOfMinute(0);
        currently.setMillisOfSecond(0);
        visited.setProperty("lastVisited", currently.getMillis());
        long[] visitedList = (long[])visited.getProperty("visitedList", new long[]{});
        HashSet<Long> visitedSet = new HashSet<>(Arrays.asList(ArrayUtils.toObject(visitedList)));

        visitedSet.add(currently.getMillis());
        visited.setProperty("visitedList", ArrayUtils.toPrimitive(visitedSet.toArray(new Long[visitedSet.size()])));
    }

    @POST
    @javax.ws.rs.Path("/async/{userId}/visited")
    public Response asyncUserVisited(String body, @PathParam("userId") String userId, @Context GraphDatabaseService db) throws IOException, InterruptedException {
        HashMap input;
        try{
            input = objectMapper.readValue( body, HashMap.class);
        } catch (Exception e) {
            return Response.status(Response.Status.BAD_REQUEST).entity(objectMapper.writeValueAsString("Error parsing input " + body)).build();
        }

        if(!input.containsKey("url")){
            return Response.status(Response.Status.BAD_REQUEST).entity(objectMapper.writeValueAsString("Missing URL Parameter " + body)).build();
        }

        String url = (String)input.get("url");

        HashMap<String, Object> write = new HashMap<>();
        HashMap<String, Object> data = new HashMap<>();
        data.put("url", url);
        data.put("userId", userId);

        boolean createUser = true;
        boolean createContent = true;

        try (Transaction tx = db.beginTx()) {
            Long userNodeId = userCache.getIfPresent(userId);
            Node userNode;
            if(userNodeId == null){
                // If the node id is not in the cache, let's try to find the node in the index.
                ResourceIterator<Node> results = db.findNodesByLabelAndProperty(Labels.User, "userId", userId).iterator();
                if (results.hasNext()) {
                    userNode = results.next();
                    data.put("userNodeId", userNode.getId());
                    userCache.put(userId, userNode.getId());
                    createUser = false;
                }

            } else {
                data.put("userNodeId", userNodeId);
                createUser = false;
            }

            Long siteNodeId = siteCache.getIfPresent(url);
            Node siteNode;
            if (siteNodeId == null) {
                // If the node id is not in the cache, let's try to find the node in the index.
                ResourceIterator<Node> results = db.findNodesByLabelAndProperty(Labels.Site, "url", url).iterator();
                if (results.hasNext()) {
                    siteNode = results.next();
                    data.put("siteNodeId", siteNode.getId());
                    siteCache.put(url, siteNode.getId());
                    createContent = false;
                }

            } else {
                data.put("siteNodeId", siteNodeId);
                createContent = false;
            }

            if(!createContent && createUser){
                write.put(ACTION, BatchWriterServiceAction.CREATE_USER);
            }

            if(createContent && !createUser){
                write.put(ACTION, BatchWriterServiceAction.CREATE_SITE);
            }

            if(createContent && createUser){
                write.put(ACTION, BatchWriterServiceAction.CREATE_BOTH);
            }

            if(!createContent && !createUser){
                write.put(ACTION, BatchWriterServiceAction.CREATE_VISITED);
            }
            write.put(DATA, data);

            batchWriterService.queue.put(write);
        }

        return javax.ws.rs.core.Response.status(javax.ws.rs.core.Response.Status.CREATED).build();
    }

    @GET
    @javax.ws.rs.Path("/{userId}/visited")
    public Response getVisited(@PathParam("userId") String userId,
                              @QueryParam("days") @DefaultValue("1") int days,
                              @Context GraphDatabaseService db) throws IOException {
        ArrayList<String> results = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            Long userNodeId = userCache.getIfPresent(userId);
            Node userNode;
            if(userNodeId == null){
                // If the node id is not in the cache, let's try to find the node in the index.
                ResourceIterator<Node> iterator = db.findNodesByLabelAndProperty(Labels.User, "userId", userId).iterator();
                if (iterator.hasNext()) {
                    userNode = iterator.next();
                } else {
                    // If the node is not in the index, then we'll throw an error
                    return Response.status(Response.Status.BAD_REQUEST).entity(objectMapper.writeValueAsString("User not found " + userId)).build();
                }

                // Add it to the cache.
                userCache.put(userId, userNode.getId());
            } else {
                // If the id is in the index, then just get the node.
                userNode = db.getNodeById(userNodeId);
            }

            for(Relationship visited : userNode.getRelationships(Direction.OUTGOING, Relationships.VISITED)){
                DateTime when = new DateTime((long)visited.getProperty("lastVisited"));
                DateTime currently = new DateTime();

                if(when.isAfter(currently.minusDays(days))){
                    results.add((String)visited.getEndNode().getProperty("url"));
                }
            }
        }

        return javax.ws.rs.core.Response.ok(objectMapper.writeValueAsString(results)).build();
    }

    public static UniqueFactory.UniqueNodeFactory getUniqueContentFactory(final GraphDatabaseService db) {
        return new UniqueFactory.UniqueNodeFactory( db, Labels.Site.name() )
        {
            @Override
            protected void initialize( Node created, Map<String, Object> properties )
            {
                created.addLabel( Labels.Site );
                created.setProperty( "url", properties.get( "url" ) );
            }
        };
    }

    public static UniqueFactory.UniqueNodeFactory getUniqueUserFactory(final GraphDatabaseService db) {
        return new UniqueFactory.UniqueNodeFactory( db, Labels.User.name() )
        {
            @Override
            protected void initialize( Node created, Map<String, Object> properties )
            {
                created.addLabel( Labels.User );
                created.setProperty( "userId", properties.get( "userId" ) );
            }
        };
    }

}
