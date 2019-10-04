package com.dmcmedia.tracking.analytics.core.etc;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import com.dmcmedia.tracking.analytics.common.factory.SparkJdbcConnectionFactory;
import com.dmcmedia.tracking.analytics.common.test.CommonJunitTest;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;


/**                                                                                         	*
*                        M a r k e t i n g   T e c h n o l o g y   L a b					    *
*                                                                                               *
*                                       DMC Media Co., Ltd.                                     *
*                                                                                               *
*                  All rights reserved.  No part of this publication may be                     *
*                  reproduced,  stored in a retrieval system  or transmitted                    *
*                  in any form or by any means.                                                 *
*                                                                                               *
*************************************************************************************************     
*
*   날  짜      성 명                     수정  내용
* ----------  ---------  ------------------------------------------------------------------------
* 2017. 5. 16.   cshwang   Initial Release
*
*
************************************************************************************************/
public class F1JdbcRead extends CommonJunitTest implements Serializable {	

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Before
	public void setUp() {
		super.atomicInteger = new AtomicInteger(0);
	}

	@Test
	public void testTrackerMongodbRead() throws Exception {

		System.setProperty("hadoop.home.dir", "C:/develop/tracking/winutils");
		SparkSession sparkSession = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri",	"mongodb://175.125.132.216:30000/tracking_m05.f1.product.373556279687015")
				.config("spark.mongodb.output.uri", "mongodb://175.125.132.216:30000/tracking.fos1.porcess")
				.getOrCreate();
	

        
    	Map<String, String> jdbcOption = new HashMap<String, String>();
		jdbcOption.put("url", "jdbc:mysql://175.125.132.212:3306/tracking?characterEncode=utf8&serverTimezone=UTC");
		jdbcOption.put("user", "dmcmedia");
		jdbcOption.put("password", "dk#007Gek");
		jdbcOption.put("driver", "com.mysql.cj.jdbc.Driver");  
        
		String jdbcDbTable = "(SELECT CATALOG_ID AS catalogId, CATALOG_NAME AS catalogName, CATALOG_SITE_ADDRESS AS catalogSiteAddress  FROM tracking.T_TRK_F1_FEED_CATALOG WHERE USE_YN = 'Y') AS t";        
        Dataset<Row> catalogDF = SparkJdbcConnectionFactory.getSelectJdbcDfLoad(sparkSession, jdbcOption, jdbcDbTable);	            
        
        catalogDF.schema();
        catalogDF.show();
//        List<Catalog> catalogList =  catalogDF.as(Encoders.bean(Catalog.class)).collectAsList();
        
        //String jdbcDbTable = "(SELECT CATALOG_ID AS catalogId, CATALOG_NAME AS catalogName, CATALOG_SITE_ADDRESS AS catalogSiteAddress  FROM T_TRK_F1_FEED_CATALOG WHERE USE_YN = 'Y') AS t";        
        //Dataset<Row> catalogDF = SparkCreateFactory.getSelectJdbcDfLoad(sparkSession, jdbcOption, jdbcDbTable);	        
		
		//Dataset<Row> catalogDF = sparkSession.sql("SELECT DISTINCT(CATALOG_ID) AS catalogId  FROM  V_F1_PRODUCT");	
		//catalogDF.printSchema();
		//catalogDF.show();
		
		//List<FeedCatalog> catalogList =  catalogDF.as(Encoders.bean(FeedCatalog.class)).collectAsList();
		//for(FeedCatalog feedCatalog : catalogList){
        //	String catalogId = feedCatalog.getCatalogId(); 
        //	LogStack.process.info("Process Catelog Id : "+  catalogId);	
        //	MongoSpark.write(resultDF.where("catalog_id = "+ catalogId)).option("database", MongodbMap.DATABASE_FEED)
		//						.option("collection", MongodbMap.COLLECTION_FREFIX_FEED_HOURLY + catalogId)
		//						.mode("append").save();
		//}
        
        
       // for(Catalog feedCatalog : catalogList){
        //	LogStack.junitTest.info(feedCatalog.getCatalogId());
        //	LogStack.junitTest.info(feedCatalog.getCatalogName());
        //	LogStack.junitTest.info(feedCatalog.getCatalogSiteAddress());
        //}
       // df.foreach((ForeachFunction<Row>) 
        //		person -> System.out.println(person));

		// Create a JavaSparkContext using the SparkSession's SparkContext
		// object
		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

		Map<String, String> readOverrides = new HashMap<String, String>();
        
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);		
		Dataset<Row> implicitDS = MongoSpark.load(jsc, readConfig).toDF();

		// Create the temp view and execute the query
		implicitDS.createOrReplaceTempView("T_F1_PRODUCT");

		Dataset<Row> centenarians = sparkSession.sql("  SELECT 	 trackingObject.content_name		AS CONTENT_NAME"
				+ "			,trackingObject.content_ids			AS CONTENT_IDS"
				+ "			,trackingObject.content_category	AS CONTENT_CATEGORY"
				+ "			,trackingObject.content_type		AS CONTENT_TYPE"
				+ "			,trackingObject.link				AS LINK"
				+ "			,trackingObject.image_link			AS IMAGE_LINK"		
				+ "			,trackingObject.description			AS DESCRIPTION"
				+ "			,trackingObject.product_catalog_id	AS PRODUCT_CATALOG_ID"
				+ "			,trackingObject.availability		AS AVAILABILITY"
				+ "			,trackingObject.brand				AS BRAND"
				+ "			,trackingObject.product_type		AS PRODUCT_TYPE"
				+ "			,trackingObject.value				AS VALUE"
				+ "			,trackingObject.currency			AS CURRENCY"
				+ "			,logDateTime						AS LOG_DATE_TIME"
				+ " FROM 	T_F1_PRODUCT"
				+ " WHERE 	logDateTime LIKE '2017-05-02 17%' ");

		
		centenarians.printSchema();
		centenarians.show();
		
	
		jsc.close();
	}
}
