package com.brijesh.vertx.vertx_mongodb;

import io.vertx.core.Vertx;

public class MainClass {

  public static void main(String[] args){
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(MongoVerticle.class.getName());
  }
}
