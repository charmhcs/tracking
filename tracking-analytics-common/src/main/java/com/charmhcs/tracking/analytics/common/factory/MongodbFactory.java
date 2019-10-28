package com.charmhcs.tracking.analytics.common.factory;

import org.bson.Document;

import com.charmhcs.tracking.analytics.common.map.CodeMap;
import com.charmhcs.tracking.analytics.common.util.LogStack;
import com.charmhcs.tracking.analytics.common.util.PropertyLoader;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Indexes;

/**  
*************************************************************************************************
*
*   날  짜      성 명                     수정  내용
* ----------  ---------  ------------------------------------------------------------------------
* 2018. 10. 18.   cshwang   Initial Release
*
*
************************************************************************************************/
public class MongodbFactory {

	private final static String TRACKING_MONGODB_URL = PropertyLoader.getProperty("tracking.mongodb.uri");
	private final static String DEFAULT_SHARD_KEY = "_id";
	private final static String DEFAULT_SHARD_TYPE = "hashed";

	public static boolean dropAndCreateAndShardingCollection(String databaseName, String collectionName) {
		dropCollection(databaseName, collectionName);
		createCollection(databaseName, collectionName);
		shardCollection(databaseName, collectionName);
		return true;

	}

	/**
	 * Collection 이 존재하는지 확인
	 * @param databaseName
	 * @param collectionName
	 * @return
	 */
	public static boolean collectionExists(String databaseName, String collectionName) {
		MongoClient mongoClient = null;
		MongoDatabase database = null;
		try {
			mongoClient = MongoClients.create(TRACKING_MONGODB_URL);
			database = mongoClient.getDatabase(databaseName);

			for (String name : database.listCollectionNames()) {
				if (name.equalsIgnoreCase(collectionName)) {
		            return true;
		        }
			}
		}catch(Exception e) {
			LogStack.factory.error(e);
			return false;
		}finally {
			mongoClient.close();
		}
		return false;
	}

	//2019-06-19 hjhwang, 트래킹 외 db 연결용
	public static boolean collectionExists(String mongodbUrl, String databaseName, String collectionName) {
		MongoClient mongoClient = null;
		MongoDatabase database = null;
		try {
			mongoClient = MongoClients.create(mongodbUrl);
			database = mongoClient.getDatabase(databaseName);

			for (String name : database.listCollectionNames()) {
				if (name.equalsIgnoreCase(collectionName)) {
		            return true;
		        }
			}
		}catch(Exception e) {
			LogStack.factory.error(e);
			return false;
		}finally {
			mongoClient.close();
		}
		return false;
	}

	/**
	 * @param databaseName
	 * @param collectionName
	 * @return
	 */
	public static boolean createCollection(String databaseName, String collectionName) {
		MongoClient mongoClient = null;
		MongoDatabase database = null;
		try {
			mongoClient = MongoClients.create(TRACKING_MONGODB_URL);
			database = mongoClient.getDatabase(databaseName);
			database.createCollection(collectionName);


		}catch(Exception e) {
			LogStack.factory.error(e);
			return false;
		}finally {
			mongoClient.close();
		}
		return true;
	}

	/**
	 * @param databaseName
	 * @param collectionName
	 * @return
	 */
	public static boolean dropCollection(String databaseName, String collectionName) {
		MongoClient mongoClient = null;
		MongoDatabase database = null;

		try {
			mongoClient = MongoClients.create(TRACKING_MONGODB_URL);
			database = mongoClient.getDatabase(databaseName);

			MongoCollection<Document> collection = database.getCollection(collectionName);
			collection.drop();

		}catch(Exception e) {
			LogStack.factory.error(e);
			return false;
		}finally {
			mongoClient.close();
		}
		return true;
	}

	public static boolean shardCollection(String databaseName, String collectionName) {
		return shardCollection(databaseName, collectionName, DEFAULT_SHARD_KEY, DEFAULT_SHARD_TYPE);
	}

	/**
	 * 인덱스를 생성한다.
	 *
	 * @param databaseName
	 * @param collectionName
	 * @param indexKey
	 * @return
	 */
	public static boolean createCollectionIndex(String databaseName, String collectionName, String indexKey) {
		MongoClient mongoClient = null;
		MongoDatabase database = null;

		try {
			mongoClient = MongoClients.create(TRACKING_MONGODB_URL);
			database = mongoClient.getDatabase(databaseName);

			MongoCollection<Document> collection = database.getCollection(collectionName);
			collection.createIndex(Indexes.ascending(indexKey));

		}catch(Exception e) {
			LogStack.factory.error(e);
			return false;
		}finally {
			mongoClient.close();
		}
		return true;
	}

	public static boolean createCollectionCompoundIndex(String databaseName, String collectionName, String[] indexKey) {
		MongoClient mongoClient = null;
		MongoDatabase database = null;

		try {
			mongoClient = MongoClients.create(TRACKING_MONGODB_URL);
			database = mongoClient.getDatabase(databaseName);
			MongoCollection<Document> collection = database.getCollection(collectionName);

			Document indexs = new Document();
			for(String key : indexKey) {
				indexs.append(key, 1);
			}

			collection.createIndex(Indexes.compoundIndex(indexs));

		}catch(Exception e) {
			LogStack.factory.error(e);
			return false;
		}finally {
			mongoClient.close();
		}
		return true;
	}

	public static boolean createCollectionCompoundIndex(String databaseName, String collectionName, Document indexKey) {
		MongoClient mongoClient = null;
		MongoDatabase database = null;

		try {
			mongoClient = MongoClients.create(TRACKING_MONGODB_URL);
			database = mongoClient.getDatabase(databaseName);
			MongoCollection<Document> collection = database.getCollection(collectionName);


			collection.createIndex(indexKey);

		}catch(Exception e) {
			LogStack.factory.error(e);
			return false;
		}finally {
			mongoClient.close();
		}
		return true;
	}

//	MongodbFactory.getMongoCollection(MongodbMap.DATABASE_PROCESSED_USER, getProcessDate() + MongodbMap.COLLECTION_PIXEL_USER)
//	.createIndex(Indexes.compoundIndex(Indexes.ascending("px"), Indexes.ascending("ct"), Indexes.ascending("pc")));



	public static boolean shardCollection(String databaseName, String collectionName, String shardKey, String shardType) {
		MongoClient mongoClient = null;
		MongoDatabase adminDatabase = null;
		try {
			mongoClient = MongoClients.create(TRACKING_MONGODB_URL);
			adminDatabase = mongoClient.getDatabase("admin");
			adminDatabase.runCommand(new Document("shardCollection", databaseName + "." + collectionName).append("key",new Document(shardKey, CodeMap.ONE)));

		}catch(Exception e) {
			LogStack.factory.error(e);
			return false;
		}finally {
			mongoClient.close();
		}
		return true;
	}

	/**
	 * @param databaseName
	 * @param collectionName
	 * @return
	 */
	public static MongoCollection<Document> getMongoCollection(String databaseName, String collectionName){
		MongoClient mongoClient = null;
		MongoDatabase database = null;

		try {
			mongoClient = MongoClients.create(TRACKING_MONGODB_URL);
			database = mongoClient.getDatabase(databaseName);
		}catch(Exception e) {
			LogStack.factory.error(e);
			mongoClient.close();
			return null;
		}
		return database.getCollection(collectionName);
	}

	/**
	 *
	 * @param mongoDBUrl
	 * @param databaseName
	 * @return
	 *
	 * 20190620 hjhwang, 모든 collection 명 조회
	 */
	public static MongoIterable<String> getMongoDBCollectionList(String mongoDBUrl, String databaseName){
		MongoClient mongoClient = null;
		MongoIterable<String> result = null;

		try {
			mongoClient = MongoClients.create(mongoDBUrl);
			MongoDatabase database = mongoClient.getDatabase(databaseName);

			result = database.listCollectionNames();
		} catch(Exception e) {
			LogStack.factory.error(e);
			mongoClient.close();
			return null;
		}

		return result;
	}

}
