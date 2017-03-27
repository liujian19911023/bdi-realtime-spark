package com.bfd.spark.pool;


import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class MongodbPool {
	private static MongoClient mongoClient = null;

	private MongodbPool() {

	}

	public synchronized static MongoClient getMongoClient(String url) {
		if (mongoClient == null) {
			mongoClient = new MongoClient(new MongoClientURI(url));
			Thread thread = new Thread() {
				public void run() {
					mongoClient.close();
					mongoClient = null;
				}
			};
			Runtime.getRuntime().addShutdownHook(thread);
		}
		return mongoClient;
	}

}
