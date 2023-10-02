package com.brijesh.vertx.vertx_mongodb;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.BulkOperation;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.mongo.UpdateOptions;

import java.util.ArrayList;
import java.util.List;


public class MongoVerticle extends AbstractVerticle {

   private JsonObject mongoConfig(){
     return new JsonObject().put("db_name", "reactivedb").put("connection_string", "mongodb://localhost:27017");
   }

   @Override
   public void start(){
      save();
     insert();
     updateCollection();
     updateCollectionWithOptions();
     replaceDocuments();
     bulkWrite();
     findAll();
   }

  public List<JsonObject> findAll(){
    System.out.println("------------------------findAll()----------------------------");
    MongoClient mongoClient = MongoClient.createShared(vertx, mongoConfig());
    JsonObject query = new JsonObject();
    List<JsonObject> lst = new ArrayList<>();
    Handler<AsyncResult<List<JsonObject>>> handler = res -> {
      if (res.succeeded()) {
        for (JsonObject json : res.result()) {
          System.out.println(json.encodePrettily());
          lst.add(json);
        }
      } else {
        res.cause().printStackTrace();
      }
    };
    mongoClient.find("books", query, handler);
    return lst;
  }

  public void save(){
    System.out.println("------------------------save()----------------------------");
    MongoClient mongoClient = MongoClient.createShared(vertx, mongoConfig());
    JsonObject document = new JsonObject()
      .put("title", "The Hobbit");
    mongoClient.save("books", document, res -> {
      if (res.succeeded()) {
        String id = res.result();
        System.out.println("Saved book with id " + id);
      } else {
        res.cause().printStackTrace();
      }
    });
  }

  public void insert(){
    System.out.println("------------------------insert()------------------------");
    MongoClient mongoClient = MongoClient.createShared(vertx, mongoConfig());
    JsonObject document = new JsonObject()
      .put("title", "The Hobbit Are Sinful");
    mongoClient.insert("books", document, res -> {
      if (res.succeeded()) {
        String id = res.result();
        System.out.println("Inserted book with id " + id);
      } else {
        res.cause().printStackTrace();
      }
    });
  }

  public void updateCollection(){
    System.out.println("--------------updateCollection()-------------------");
    MongoClient mongoClient = MongoClient.createShared(vertx, mongoConfig());
    JsonObject query = new JsonObject()
      .put("title", "The Hobbit");
    // Set the author field
    JsonObject update = new JsonObject().put("$set", new JsonObject()
      .put("author", "J. R. R. Tolkien"));
    mongoClient.updateCollection("books", query, update, res -> {
      if (res.succeeded()) {
        System.out.println("Book updated !");
      } else {
        res.cause().printStackTrace();
      }
    });
  }

  public void updateCollectionWithOptions(){
    System.out.println("-------------------updateCollectionWithOptions()----------------");
    MongoClient mongoClient = MongoClient.createShared(vertx, mongoConfig());
    JsonObject query = new JsonObject()
      .put("title", "The Hobbit");
// Set the author field
    JsonObject update = new JsonObject().put("$set", new JsonObject()
      .put("author", "J. R. R. Tolkien"));
    UpdateOptions options = new UpdateOptions().setMulti(true);
    mongoClient.updateCollectionWithOptions("books", query, update, options, res -> {
      if (res.succeeded()) {
        System.out.println("Book updated !");
      } else {
        res.cause().printStackTrace();
      }
    });
  }

  public void replaceDocuments(){
    System.out.println("-------------------replaceDocuments()---------------- ");
    MongoClient mongoClient = MongoClient.createShared(vertx, mongoConfig());
    JsonObject query = new JsonObject()
      .put("title", "The Hobbit");
    JsonObject replace = new JsonObject()
      .put("title", "The Lord of the Rings")
      .put("author", "J. R. R. Tolkien");
    mongoClient.replaceDocuments("books", query, replace, res -> {
      if (res.succeeded()) {
        System.out.println("Book replaced !");
      } else {
        res.cause().printStackTrace();
      }
    });
  }

  public void bulkWrite(){
    MongoClient mongoClient = MongoClient.createShared(vertx, mongoConfig());
    JsonObject insert = new JsonObject()
      .put("title", "The Hobbit Are Most Sinful");
    JsonObject update = new JsonObject().put("$set", new JsonObject()
      .put("author", "J. R. R. Tolkien"));
    JsonObject query = new JsonObject()
      .put("title", "The Hobbit");
    List<BulkOperation> bulkOperationLst = new ArrayList<>();
    bulkOperationLst.add(BulkOperation.createInsert(insert));
    bulkOperationLst.add(BulkOperation.createUpdate(query,update,true,true));
    mongoClient.bulkWrite("books",bulkOperationLst, res -> {
        if(res.succeeded()){
          System.out.println("Bulk Operation success !");
        }else{
          res.cause().printStackTrace();
        }
    });
  }


}
