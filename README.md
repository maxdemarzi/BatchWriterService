Batch Writer Service
=============++=====

A POC Batch Writer Service for scaling Neo4j Writes

1. Build it:

        mvn clean package

2. Copy target/BatchWriterService-1.0-SNAPSHOT.jar to the plugins/ directory of your Neo4j server.

3. Download and copy additional jars to the plugins/ directory of your Neo4j server.

        wget http://repo1.maven.org/maven2/joda-time/joda-time/2.3/joda-time-2.3.jar
        wget http://repo1.maven.org/maven2/com/google/guava/guava/17.0/guava-17.0.jar

3. Configure Neo4j by adding a line to conf/neo4j-server.properties:

        org.neo4j.server.thirdparty_jaxrs_classes=com.neo4j.example.bws=/v1

4. Start Neo4j server.

5. Check that it is installed correctly over HTTP:

        :GET /v1/service/helloworld

6. Warm up the database (optional, but recommended after a restart):
This call will warm up the Neo4j Object Cache as well as warm up the content and user caches (see Step 9).

        :GET /v1/service/warmup

7. Initialize the Database (Database migration):
This call will create two unique index contraints, one on userId of User and one on url of Site.

        :GET /v1/service/initialize


8. Create a View :POST /v1/service/{userId}/visited 

        {"url" : "http://www.neo4j.com"}
         
9. Get user Views :GET /v1/service/{userId}/visited
         
         ["http://www.neo4j.com"]
                  
10. Performance Testing:
         
To measure the requests per seconds by sending 20k post requests using 4 threads with keep-alive on to the same user/content pair:          
         
         ab -k -n 20000 -c 4 -p params.json -T 'application/json' http://127.0.0.1:7474/v1/service/user123/visited
         
After a few runs, you should see something like:
         
         Requests per second:    5378.89 [#/sec] (mean)

To measure the requests per seconds by sending 20k get requests using 4 threads with keep-alive on to the same user/content pair:

        ab -k -n 20000 -c 4 http://127.0.0.1:7474/v1/service/user123/visited
        
After a few runs, you should see something like:
        
        Requests per second:    25037.21 [#/sec] (mean)
        
We are currently Caching userId to User Node and url to Site Node lookups.  
The performance should reach these levels in a production deployment only after the initial period of loading.
 
 11. Asyncronous Batched Writes:
 
 Working POC, will need to compare against single requests with  load testing tool.

         :POST /v1/service/async/{userId}/visited {"url" : "http://www.neo4j.org"}