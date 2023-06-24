package io.id.stock.analysis.Module;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertOneResult;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBStock {

    String uri;
    ServerApi serverApi;
    MongoClientSettings settings;
    MongoClient mongoClient;
    MongoDatabase mongodb;
    MongoCollection<Document> collection;

    private static final Logger log = LoggerFactory.getLogger(MongoDBStock.class.getSimpleName());

    public MongoDBStock(String connectionString){
        this.uri = connectionString;
    }
    public void createConnection(){
        // Construct a ServerApi instance using the ServerApi.builder() method
        serverApi = ServerApi.builder()
                .version(ServerApiVersion.V1)
                .build();

        settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(uri))
                .serverApi(serverApi)
                .build();

        try (MongoClient mongoClient = MongoClients.create(settings)) {
            this.mongoClient = MongoClients.create(settings);
            log.info("Connection MongoDB Success");
        } catch (Exception e){
            log.info("Connection MongoDB Failed "+e.getMessage());
        }
    }

    public void insertOneDoc(String databaseName, String collectionName, String json){
        try {
            mongodb = mongoClient.getDatabase(databaseName);
            collection = mongodb.getCollection(collectionName);
            InsertOneResult result = collection.insertOne(Document.parse(json));
            log.info("Inserted a document with the following id: " + result.getInsertedId().asObjectId().getValue().toString());
        } catch (MongoException me) {
            log.info(me.getMessage());
        }
    }

    public void insertOneDocWithNoDuplicate(String databaseName, String collectionName, String json){
        try {
            mongodb = mongoClient.getDatabase(databaseName);
            collection = mongodb.getCollection(collectionName);

            // Check for duplicates before inserting
            Document existingDocument = collection.find(Document.parse(json)).first();
            if (existingDocument == null) {
                // Insert the document if no duplicates found
                InsertOneResult result = collection.insertOne(Document.parse(json));
                log.info("Inserted a document with the following id: " + result.getInsertedId().asObjectId().getValue().toString());
            } else {
                log.info("Duplicate document found. Skipping insertion.");
            }
        } catch (MongoException me) {
            log.info(me.getMessage());
        }
    }
}
