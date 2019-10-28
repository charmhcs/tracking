package com.charmhcs.tracking.analytics.common.factory;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.charmhcs.tracking.analytics.common.util.PropertyLoader;

/**  
*
*   날  짜      성 명                     수정  내용
* ----------  ---------  ------------------------------------------------------------------------
* 2018. 10. 30.   cshwang   Initial Release
*
*
************************************************************************************************/
public class SparkMongodbConnectionFactory {

	public final static String APP_NAME = PropertyLoader.getProperty("spark.appname"); // Tracking Analysis default App
	public final static String SPARK_MASTER = PropertyLoader.getProperty("spark.master");
	public final static String SPARK_MONGODB_INPUT_URI = PropertyLoader.getProperty("tracking.spark.mongodb.input.uri");
	public final static String SPARK_MONGODB_OUTPPUT_URI = PropertyLoader.getProperty("tracking.spark.mongodb.output.uri");
	public final static String SPARK_MONGODB_3RDPARTY_INPUT_URI = PropertyLoader.getProperty("3rdparty.spark.mongodb.input.uri");


	/**
	 * Spark Context 생성
	 *
	 * @param inputUri
	 * @param outputUri
	 * @param appName
	 * @return
	 */
	public static JavaSparkContext createJavaSparkContext(String inputUri, String outputUri, String appName) {
		SparkConf conf = new SparkConf().setMaster(SPARK_MASTER).setAppName(appName)
				.set("spark.mongodb.input.uri", inputUri)
				.set("spark.mongodb.output.uri", outputUri);
		return new JavaSparkContext(conf);
	}

	/**
	 * Spark Context 생성
	 *
	 * @param inputUri
	 * @param outputUri
	 *
	 * @return
	 */
	public static JavaSparkContext createJavaSparkContext(String inputUri, String outputUri) {
		return createJavaSparkContext(inputUri, outputUri, APP_NAME);
	}

	/**
 	 * Spark Context 생성
 	 *
	 * @return
	 */
	public static JavaSparkContext createJavaSparkContext() {
		SparkConf conf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APP_NAME)
				.set("spark.mongodb.input.uri", SPARK_MONGODB_INPUT_URI)
				.set("spark.mongodb.output.uri", SPARK_MONGODB_OUTPPUT_URI);
		return new JavaSparkContext(conf);
	}


	/**
	 * Spark Session 생성 (SQL 사용시)
	 *
	 * @param inputUri
	 * @param outputUri
	 * @param appName
	 * @return
	 */
	public static SparkSession createSparkSession(String inputUri, String outputUri,String appName) {
		return SparkSession.builder().master(SPARK_MASTER).appName(appName)
				.config("spark.mongodb.input.uri", inputUri)
				.config("spark.mongodb.output.uri", outputUri).getOrCreate();
	}

	/**
	 * Spark Session 생성 (SQL 사용시)
	 *
	 * @param inputUri
	 * @param outputUri
	 * @return
	 */
	public static SparkSession createSparkSession(String inputUri, String outputUri) {
		return createSparkSession(inputUri, outputUri, APP_NAME);
	}


	/**
	 * Spark Session 생성 (SQL 사용시)
	 *
	 * @return
	 */
	public static SparkSession createSparkSession() {
		return SparkSession.builder().master(SPARK_MASTER).appName(APP_NAME)
				.config("spark.mongodb.input.uri", SPARK_MONGODB_INPUT_URI)
				.config("spark.mongodb.output.uri", SPARK_MONGODB_OUTPPUT_URI).getOrCreate();
	}
}
