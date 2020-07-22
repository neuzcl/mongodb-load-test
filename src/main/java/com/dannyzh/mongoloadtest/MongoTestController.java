package com.dannyzh.mongoloadtest;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;

import com.mongodb.ServerAddress;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import java.util.Arrays;
import java.util.HashMap;

import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;

@RestController
public class MongoTestController {
    private static MongoClient MONGO_CLIENT = MongoClients.create(
            "mongodb://***:***@int3.dannyzh.com:27017,int4.dannyzh.com:27017,int5.dannyzh.com:27017/?replicaSet=cmb&minPoolSize=200&maxPoolSize=5000&readPreference=primaryPreferred");
    private static String DATABASE_NAME = "cmb";
    private static String COLLECTION_NAME = "monthlySummary";

    @GetMapping("/testService")
    public String testService() {
        int randomNum = ThreadLocalRandom.current().nextInt(0, 175000);
        String eacId = "6666940858" + String.format("%06d", randomNum);

        return "Service is running..., using random eacId: " + eacId;
    }

    @GetMapping("/testAggregation")
    public String TestAggregation() {
        MongoDatabase database = MONGO_CLIENT.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

        Map<String, Object> multiIdMap = new HashMap<String, Object>();
        multiIdMap.put("month", "$month");
        multiIdMap.put("datFlg1", "$datFlg1");
        multiIdMap.put("trxCod2", "$trxCod2");
        multiIdMap.put("trxCod3", "$trxCod3");

        Document groupFields = new Document(multiIdMap);

        // 1 card, 1 month
        // AggregateIterable<Document> result = collection.aggregate(Arrays.asList(
        // match(and(eq("eacId", "6666940858000001"), eq("month", 202007))),
        // project(fields(excludeId(),
        // include("month"), computed("summary", eq("$objectToArray",
        // "$monthlyClassifySummary")))),
        // unwind("$summary"),
        // project(fields(include("month"), computed("summary", "$summary.v"),
        // computed("classify", eq("$split", Arrays.asList("$summary.k", "_"))))),
        // project(fields(include("month", "summary"),
        // computed("datFlg1", eq("$arrayElemAt", Arrays.asList("$classify", 0L))),
        // computed("trxCod2", eq("$arrayElemAt", Arrays.asList("$classify", 1L))),
        // computed("trxCod3", eq("$arrayElemAt", Arrays.asList("$classify", 2L))))),
        // group(groupFields, sum("totalCount", "$summary.count"), sum("totalAmount",
        // "$summary.amount"))));

        int randomNum = ThreadLocalRandom.current().nextInt(0, 175000);
        String eacId = "6666940858" + String.format("%06d", randomNum);

        // 1 card, 3 month
        AggregateIterable<Document> result = collection.aggregate(Arrays.asList(
                match(and(eq("eacId", eacId), in("month", Arrays.asList(202001L, 202002L, 202003L)))),
                project(fields(excludeId(), include("month"),
                        computed("summary", eq("$objectToArray", "$monthlyClassifySummary")))),
                unwind("$summary"),
                project(fields(include("month"), computed("summary", "$summary.v"),
                        computed("classify", eq("$split", Arrays.asList("$summary.k", "_"))))),
                project(fields(include("month", "summary"),
                        computed("datFlg1", eq("$arrayElemAt", Arrays.asList("$classify", 0L))),
                        computed("trxCod2", eq("$arrayElemAt", Arrays.asList("$classify", 1L))),
                        computed("trxCod3", eq("$arrayElemAt", Arrays.asList("$classify", 2L))))),
                group(groupFields, sum("totalCount", "$summary.count"), sum("totalAmount", "$summary.amount"))));

        MongoCursor<Document> cursor = result.cursor();
        Document response = null;
        while(cursor.hasNext()){
            response = cursor.next();
        }

        if (response != null) {
            return response.toJson();
        } else {
            return "Not found.";
        }
    }

    @GetMapping("/testAggregation2")
    public String TestAggregation2() {
        MongoDatabase database = MONGO_CLIENT.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

        Map<String, Object> multiIdMap = new HashMap<String, Object>();
        multiIdMap.put("month", "$month");
        multiIdMap.put("datFlg1", "$datFlg1");
        multiIdMap.put("trxCod2", "$trxCod2");
        multiIdMap.put("trxCod3", "$trxCod3");

        Document groupFields = new Document(multiIdMap);

        int randomNum = ThreadLocalRandom.current().nextInt(0, 175000);
        String eacId = "6666940858" + String.format("%06d", randomNum);

        AggregateIterable<Document> result = collection.aggregate(Arrays.asList(
                match(and(eq("eacId", eacId), in("month", Arrays.asList(202001, 202002, 202003)))),
                project(fields(excludeId(), include("month"),
                        computed("summary", eq("$objectToArray", "$monthlyClassifySummary")))),
                unwind("$summary"),
                project(fields(include("month"), computed("summary", "$summary.v"),
                        computed("classify", eq("$split", Arrays.asList("$summary.k", "_"))))),
                project(fields(include("month", "summary"),
                        computed("datFlg1", eq("$arrayElemAt", Arrays.asList("$classify", 0))),
                        computed("trxCod2", eq("$arrayElemAt", Arrays.asList("$classify", 1))),
                        computed("trxCod3", eq("$arrayElemAt", Arrays.asList("$classify", 2))))),
                group(groupFields, sum("totalCount", "$summary.count"), sum("totalAmount", "$summary.amount"))));

        MongoCursor<Document> cursor = result.cursor();
        Document response = null;
        while(cursor.hasNext()){
            response = cursor.next();
        }

        if (response != null) {
            return response.toJson();
        } else {
            return "Not found.";
        }
    }

    @GetMapping("/testAggregation3")
    public String TestAggregation3() {
        MongoDatabase database = MONGO_CLIENT.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

        Map<String, Object> multiIdMap = new HashMap<String, Object>();
        multiIdMap.put("month", "$month");
        multiIdMap.put("datFlg1", "$datFlg1");
        multiIdMap.put("trxCod2", "$trxCod2");
        multiIdMap.put("trxCod3", "$trxCod3");

        Document groupFields = new Document(multiIdMap);

        int randomNum = ThreadLocalRandom.current().nextInt(0, 175000);
        String eacId = "6666940858" + String.format("%06d", randomNum);

        AggregateIterable<Document> result = collection.aggregate(Arrays.asList(
                match(and(eq("eacId", eacId), in("month", Arrays.asList(202001, 202002, 202003)))),
                project(fields(excludeId(), include("month"),
                        computed("summary", eq("$objectToArray", "$monthlyClassifySummary")))),
                unwind("$summary"),
                project(fields(include("month"), computed("summary", "$summary.v"),
                        computed("classify", eq("$split", Arrays.asList("$summary.k", "_"))))),
                project(fields(include("month", "summary"),
                        computed("datFlg1", eq("$arrayElemAt", Arrays.asList("$classify", 0))),
                        computed("trxCod2", eq("$arrayElemAt", Arrays.asList("$classify", 1))),
                        computed("trxCod3", eq("$arrayElemAt", Arrays.asList("$classify", 2)))))));

        MongoCursor<Document> cursor = result.cursor();
        Document response = null;
        while(cursor.hasNext()){
            response = cursor.next();
        }

        if (response != null) {
            return response.toJson();
        } else {
            return "Not found.";
        }
    }

    @GetMapping("/testAggregation4")
    public String TestAggregation4() {
        MongoDatabase database = MONGO_CLIENT.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

        Map<String, Object> multiIdMap = new HashMap<String, Object>();
        multiIdMap.put("month", "$month");
        multiIdMap.put("datFlg1", "$datFlg1");
        multiIdMap.put("trxCod2", "$trxCod2");
        multiIdMap.put("trxCod3", "$trxCod3");

        Document groupFields = new Document(multiIdMap);

        int randomNum = ThreadLocalRandom.current().nextInt(0, 175000);
        String eacId = "6666940858" + String.format("%06d", randomNum);

        AggregateIterable<Document> result = collection.aggregate(Arrays.asList(
                match(and(eq("eacId", eacId), in("month", Arrays.asList(202001, 202002, 202003)))),
                project(fields(excludeId(), include("month"),
                        computed("summary", eq("$objectToArray", "$monthlyClassifySummary")))),
                unwind("$summary"),
                project(fields(include("month"), computed("summary", "$summary.v"),
                        computed("classify", eq("$split", Arrays.asList("$summary.k", "_")))))));

        MongoCursor<Document> cursor = result.cursor();
        Document response = null;
        while(cursor.hasNext()){
            response = cursor.next();
        }

        if (response != null) {
            return response.toJson();
        } else {
            return "Not found.";
        }
    }

    @GetMapping("/testAggregation5")
    public String TestAggregation5() {
        MongoDatabase database = MONGO_CLIENT.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

        Map<String, Object> multiIdMap = new HashMap<String, Object>();
        multiIdMap.put("month", "$month");
        multiIdMap.put("datFlg1", "$datFlg1");
        multiIdMap.put("trxCod2", "$trxCod2");
        multiIdMap.put("trxCod3", "$trxCod3");

        Document groupFields = new Document(multiIdMap);

        int randomNum = ThreadLocalRandom.current().nextInt(0, 175000);
        String eacId = "6666940858" + String.format("%06d", randomNum);

        AggregateIterable<Document> result = collection.aggregate(Arrays.asList(
                match(and(eq("eacId", eacId), in("month", Arrays.asList(202001, 202002, 202003)))),
                project(fields(excludeId(), include("month"),
                        computed("summary", eq("$objectToArray", "$monthlyClassifySummary")))),
                unwind("$summary")));

        MongoCursor<Document> cursor = result.cursor();
        Document response = null;
        while(cursor.hasNext()){
            response = cursor.next();
        }

        if (response != null) {
            return response.toJson();
        } else {
            return "Not found.";
        }
    }

    @GetMapping("/testAggregation6")
    public String TestAggregation6() {
        MongoDatabase database = MONGO_CLIENT.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

        Map<String, Object> multiIdMap = new HashMap<String, Object>();
        multiIdMap.put("month", "$month");
        multiIdMap.put("datFlg1", "$datFlg1");
        multiIdMap.put("trxCod2", "$trxCod2");
        multiIdMap.put("trxCod3", "$trxCod3");

        Document groupFields = new Document(multiIdMap);

        int randomNum = ThreadLocalRandom.current().nextInt(0, 175000);
        String eacId = "6666940858" + String.format("%06d", randomNum);

        AggregateIterable<Document> result = collection.aggregate(Arrays.asList(
                match(and(eq("eacId", eacId), in("month", Arrays.asList(202001, 202002, 202003)))),
                project(fields(excludeId(), include("month"),
                        computed("summary", eq("$objectToArray", "$monthlyClassifySummary"))))));

        MongoCursor<Document> cursor = result.cursor();
        Document response = null;
        while(cursor.hasNext()){
            response = cursor.next();
        }

        if (response != null) {
            return response.toJson();
        } else {
            return "Not found.";
        }
    }

    @GetMapping("/testAggregation7")
    public String TestAggregation7() {
        MongoDatabase database = MONGO_CLIENT.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

        Map<String, Object> multiIdMap = new HashMap<String, Object>();
        multiIdMap.put("month", "$month");
        multiIdMap.put("datFlg1", "$datFlg1");
        multiIdMap.put("trxCod2", "$trxCod2");
        multiIdMap.put("trxCod3", "$trxCod3");

        Document groupFields = new Document(multiIdMap);

        int randomNum = ThreadLocalRandom.current().nextInt(0, 175000);
        String eacId = "6666940858" + String.format("%06d", randomNum);

        AggregateIterable<Document> result = collection.aggregate(Arrays.asList(
                match(and(eq("eacId", eacId), in("month", Arrays.asList(202001, 202002, 202003))))));

        MongoCursor<Document> cursor = result.cursor();
        Document response = null;
        while(cursor.hasNext()){
            response = cursor.next();
        }

        if (response != null) {
            return response.toJson();
        } else {
            return "Not found.";
        }
    }

    @GetMapping("/testAggregation11")
    public String TestAggregation11() {
        MongoDatabase database = MONGO_CLIENT.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

        Map<String, Object> multiIdMap = new HashMap<String, Object>();
        multiIdMap.put("month", "$month");
        multiIdMap.put("datFlg1", eq("$arrayElemAt", Arrays.asList("$classify", 0)));
        multiIdMap.put("trxCod2", eq("$arrayElemAt", Arrays.asList("$classify", 1)));
        multiIdMap.put("trxCod3", eq("$arrayElemAt", Arrays.asList("$classify", 2)));

        Document groupFields = new Document(multiIdMap);

        int randomNum = ThreadLocalRandom.current().nextInt(0, 175000);
        String eacId = "6666940858" + String.format("%06d", randomNum);

        AggregateIterable<Document> result = collection.aggregate(Arrays.asList(
                match(and(eq("eacId", eacId), in("month", Arrays.asList(202001, 202002, 202003)))),
                project(fields(excludeId(), include("month"),
                        computed("summary", eq("$objectToArray", "$monthlyClassifySummary")))),
                unwind("$summary"),
                project(fields(include("month"), computed("summary", "$summary.v"),
                        computed("classify", eq("$split", Arrays.asList("$summary.k", "_"))))),
                group(groupFields, sum("totalCount", "$summary.count"), sum("totalAmount", "$summary.amount"))));

        MongoCursor<Document> cursor = result.cursor();
        Document response = null;
        while(cursor.hasNext()){
            response = cursor.next();
        }

        if (response != null) {
            return response.toJson();
        } else {
            return "Not found.";
        }
    }

    @GetMapping("/testAggregation12")
    public String TestAggregation12() {
        MongoDatabase database = MONGO_CLIENT.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

        Map<String, Object> multiIdMap = new HashMap<String, Object>();
        multiIdMap.put("month", "$month");
        multiIdMap.put("classify", "$summary.k");

        Document groupFields = new Document(multiIdMap);

        int randomNum = ThreadLocalRandom.current().nextInt(0, 175000);
        String eacId = "6666940858" + String.format("%06d", randomNum);

        AggregateIterable<Document> result = collection.aggregate(Arrays.asList(
                match(and(eq("eacId", eacId), in("month", Arrays.asList(202001, 202002, 202003)))),
                project(fields(excludeId(), include("month"),
                        computed("summary", eq("$objectToArray", "$monthlyClassifySummary")))),
                unwind("$summary"),
                group(groupFields, sum("totalCount", "$summary.v.count"), sum("totalAmount", "$summary.v.amount"))));

        MongoCursor<Document> cursor = result.cursor();
        Document response = null;
        while(cursor.hasNext()){
            response = cursor.next();
        }

        if (response != null) {
            return response.toJson();
        } else {
            return "Not found.";
        }
    }
}