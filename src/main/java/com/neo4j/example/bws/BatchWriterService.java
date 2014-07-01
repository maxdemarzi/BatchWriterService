package com.neo4j.example.bws;

import com.google.common.util.concurrent.AbstractScheduledService;
import org.joda.time.DateTime;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.UniqueFactory;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class BatchWriterService extends AbstractScheduledService {

    private static final Logger logger = Logger.getLogger(BatchWriterService.class.getName());
    private GraphDatabaseService graphDb;
    public LinkedBlockingQueue<HashMap<String, Object>> queue = new LinkedBlockingQueue<>();

    // Optionally set a limit to the size of the queue which will force requests to block until drained.
    // public LinkedBlockingQueue<HashMap<String, Object>> queue = new LinkedBlockingQueue<>(25_000);

    private static final String ACTION = "action";
    private static final String DATA = "data";

    public void SetGraphDatabase(GraphDatabaseService graphDb){
        this.graphDb = graphDb;
    }

    public final static BatchWriterService INSTANCE = new BatchWriterService();
    private BatchWriterService() {
        if (!this.isRunning()){
            logger.info("Starting BatchWriterService");
            this.startAsync();
            this.awaitRunning();
            logger.info("Started BatchWriterService");
        }
    }

    @Override
    protected void runOneIteration() throws Exception {
        long startTime = System.nanoTime();
        long transactionTime = System.nanoTime();
        Collection<HashMap<String, Object>> writes = new ArrayList<>();

        queue.drainTo(writes);

        if(!writes.isEmpty()){
            int i = 0;
            Transaction tx = graphDb.beginTx();
            try {
                for( HashMap write : writes){
                    try {
                        i++;
                        switch ((BatchWriterServiceAction) write.get(ACTION)) {
                            case CREATE_USER:
                                if (write.get(DATA) != null &&
                                        ((HashMap)write.get(DATA)).containsKey("userId") &&
                                        ((HashMap)write.get(DATA)).containsKey("siteNodeId") ) {
                                    String userId = (String)((HashMap)write.get(DATA)).get("userId");
                                    UniqueFactory.UniqueNodeFactory userFactory = NeoService.getUniqueUserFactory(graphDb);
                                    Node userNode = userFactory.getOrCreate( "userId", userId );
                                    Node siteNode = graphDb.getNodeById((Long)((HashMap)write.get(DATA)).get("siteNodeId"));
                                    NeoService.CreateVisitedRelationship(userNode, siteNode);
                                }
                                break;
                            case CREATE_SITE:
                                if (write.get(DATA) != null &&
                                        ((HashMap)write.get(DATA)).containsKey("userNodeId") &&
                                        ((HashMap)write.get(DATA)).containsKey("url") ) {
                                    String url = (String)((HashMap)write.get(DATA)).get("url");
                                    UniqueFactory.UniqueNodeFactory contentFactory = NeoService.getUniqueContentFactory(graphDb);
                                    Node siteNode = contentFactory.getOrCreate( "url", url );
                                    Node userNode = graphDb.getNodeById((Long)((HashMap)write.get(DATA)).get("userNodeId"));
                                    NeoService.CreateVisitedRelationship(userNode, siteNode);
                                }
                                break;
                            case CREATE_BOTH:
                                if (write.get(DATA) != null &&
                                        ((HashMap)write.get(DATA)).containsKey("userId") &&
                                        ((HashMap)write.get(DATA)).containsKey("url") ) {
                                    String userId = (String)((HashMap)write.get(DATA)).get("userId");
                                    UniqueFactory.UniqueNodeFactory userFactory = NeoService.getUniqueUserFactory(graphDb);
                                    Node userNode = userFactory.getOrCreate( "userId", userId );
                                    String url = (String)((HashMap)write.get(DATA)).get("url");
                                    UniqueFactory.UniqueNodeFactory contentFactory = NeoService.getUniqueContentFactory(graphDb);
                                    Node siteNode = contentFactory.getOrCreate( "url", url );
                                    NeoService.CreateVisitedRelationship(userNode, siteNode);
                                }
                                break;
                            case CREATE_VISITED:
                                if (write.get(DATA) != null &&
                                        ((HashMap)write.get(DATA)).containsKey("userNodeId") &&
                                        ((HashMap)write.get(DATA)).containsKey("siteNodeId") ) {
                                    Node userNode = graphDb.getNodeById((Long)((HashMap)write.get(DATA)).get("userNodeId"));
                                    Node siteNode = graphDb.getNodeById((Long)((HashMap)write.get(DATA)).get("siteNodeId"));
                                    NeoService.CreateVisitedRelationship(userNode, siteNode);
                                }
                                break;

                        }
                    } catch (Exception exception) {
                        logger.severe("Error Creating Visited Relationship: " + write);
                    }

                    if(i % 1_000 == 0){
                        tx.success();
                        tx.close();
                        DateTime currently = new DateTime();
                        System.out.printf("Performed a transaction of 1,000 writes in  %d [msec] @ %s \n", (System.nanoTime() - transactionTime) / 1000000, currently.toDateTimeISO());
                        transactionTime = System.nanoTime();
                        tx = graphDb.beginTx();
                    }
                }

                tx.success();
            } finally {
                tx.close();
                DateTime currently = new DateTime();
                System.out.printf("Performed a set of transactions with %d writes in  %d [msec] @ %s \n", writes.size(), (System.nanoTime() - startTime) / 1000000, currently.toDateTimeISO());
            }
        }

    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(0, 1, TimeUnit.SECONDS);
    }

}