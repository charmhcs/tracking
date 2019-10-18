package com.dmcmedia.tracking.analytics.core.etc;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import com.dmcmedia.tracking.analytics.common.factory.SparkMongodbConnectionFactory;
import com.dmcmedia.tracking.analytics.common.map.MongodbMap;
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
* 2017. 7. 23.   cshwang   Initial Release
*
*
************************************************************************************************/
public class AddToCartAmountCategories  extends CommonJunitTest{
	
	@Before
	public void setUp() {
		super.atomicInteger = new AtomicInteger(0);
	}
	
	@Test
	public void createAddToCartAmountByCategories(){
		System.setProperty("hadoop.home.dir", "C:/develop/tracking/winutils");
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();
		String catalogId = "1921855474765164";
		String month = "08";
		String logDate = "2017-08-21";
		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());		
		
		try{
			Dataset<Row> feedListDs = MongoSpark.load(jsc, ReadConfig.create(sparkSession)
					.withOption("database", MongodbMap.DATABASE_PROCESSED_FEED)
					.withOption("collection",  MongodbMap.COLLECTION_PIXEL_FEED + MongodbMap.COLLECTION_DOT  + catalogId)).toDF();	
			Dataset<Row> addToCartDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession)
					.withOption("database", MongodbMap.DATABASE_PROCESSED_MONTH + month)
					.withOption("collection",  logDate	+ MongodbMap.COLLECTION_PIXEL_PURCHASED)).toDF();	
			
			feedListDs.createOrReplaceTempView("T_FEED");
		    addToCartDS.createOrReplaceTempView("T_ADD_TO_CART");			
			
			Dataset<Row> feedDF = sparkSession.sql("SELECT 	 	content_name		AS CONTENT_NAME"
													+ "			,content_ids		AS CONTENT_IDS"
													+ "			,content_category	AS CONTENT_CATEGORY"
													+ "			,catalog_id			AS CATALOG_ID"
													+ "			,value				AS VALUE"
													+ "			,currency			AS CURRENCY"
													+ " FROM 	T_FEED");
			feedDF.createOrReplaceTempView("V_FEED");
			feedDF.printSchema();
			feedDF.show();
			
			Dataset<Row> addToCartbyCatalogDF = sparkSession.sql("SELECT	 content_ids		AS CONTENT_IDS"
																+ "			,catalog_id			AS CATALOG_ID"
																+ "			,value				AS VALUE"
																+ "			,log_date_time				AS LOG_DATE_TIME"
																+ "			,CAST(DATE(log_date_time)	AS STRING)	AS LOG_DATE"
																+ "			,CAST(HOUR(log_date_time) AS STRING)	AS LOG_HOUR"
																+ " FROM 	T_ADD_TO_CART ");
			addToCartbyCatalogDF.createOrReplaceTempView("V_ADD_TO_CART");
			addToCartbyCatalogDF.printSchema();
			addToCartbyCatalogDF.show();
			addToCartbyCatalogDF.count();		
			
			//Map<String, String> connectionProperties = new HashMap<String, String>();
			
			Properties connectionProperties = new Properties();
			connectionProperties.put("user", "dmcmedia");
			connectionProperties.put("password", "dk#007Gek");
			connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver");	
			connectionProperties.put("lowerBound", "1");
			connectionProperties.put("upperBound", "10000");			
			
//			Dataset<Row> ds = spark.read()
//					  .format("jdbc")
//					  .option("url", url)
//					  .option("driver", "com.mysql.jdbc.Driver")
//					  .option("dbtable", "bigdatatable")
//					  .option("user", "root")
//					  .option("password", "foobar")
//					  .option("partitionColumn", "NUMERIC_COL")
//					  .option("lowerBound", "1")
//					  .option("upperBound", "10000")
//					  .option("numPartitions", "64")
//					  .load();
			
			
			Dataset<Row> resultDF;
			resultDF = sparkSession.sql(
					  " SELECT 	 T.CATALOG_ID 							AS CATALOG_ID"
					+ "			,T.LOG_DATE								AS LOG_DATE"
					+ "			,T.CONTENT_CATEGORY						AS CONTENT_CATEGORY"
					+ "			,T.TOTAL_AMOUNT							AS TOTAL_AMOUNT"
					+ "			,T.TOTAL_COUNT							AS TOTAL_COUNT"
					+ "			,ROUND(T.TOTAL_AMOUNT / T.TOTAL_COUNT) 	AS AVERAGE_AMOUNT"
					+ " FROM (  SELECT   B.CATALOG_ID"
					+ "					,B.LOG_DATE"
					+ "					,A.CONTENT_CATEGORY"
					+ "					,SUM(A.VALUE)				AS TOTAL_AMOUNT"
					+ "					,COUNT(A.CONTENT_IDS)		AS TOTAL_COUNT"
					+ " 		FROM 	V_FEED A INNER JOIN V_ADD_TO_CART B "
					+ " 		ON		A.CONTENT_IDS = B.CONTENT_IDS"
					+ " 		GROUP BY B.CATALOG_ID, A.CONTENT_CATEGORY, B.LOG_DATE) T "
					+ " ORDER BY T.TOTAL_AMOUNT DESC");
			resultDF.printSchema();
			resultDF.show();			
//			resultDF.write().jdbc("jdbc:mysql://175.125.132.212:3306/tracking?characterEncode=utf8&serverTimezone=UTC", "tablename", connectionProperties);
			
			resultDF.coalesce(1).write().option("header", "true").csv("D:/download/"+logDate+"addtocart_amount_category.csv");
//			
//			Dataset<Row> resultDF3;
//			resultDF3 = sparkSession.sql(
//					  " SELECT 	 T.CATALOG_ID 												AS CATALOG_ID"					
//					+ "			,T.CONTENT_CATEGORY											AS CONTENT_CATEGORY"
//					+ "			,T.LOG_DATE													AS LOG_DATE"
//					+ "			,SUM(T.VALUE)												AS TOTAL_AMOUNT"
//					+ "			,COUNT(T.CONTENT_IDS)										AS TOTAL_COUNT"
//					+ "			,ROUND(SUM(T.VALUE) / COUNT(T.CONTENT_IDS))					AS AVERAGE_AMOUNT"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 0 THEN T.CONTENT_IDS END) 	AS 00H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 1 THEN  T.CONTENT_IDS END) 	AS 01H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 2 THEN  T.CONTENT_IDS END) 	AS 02H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 3 THEN  T.CONTENT_IDS END) 	AS 03H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 4 THEN  T.CONTENT_IDS END) 	AS 04H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 5 THEN  T.CONTENT_IDS END) 	AS 05H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 6 THEN  T.CONTENT_IDS END) 	AS 06H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 7 THEN  T.CONTENT_IDS END) 	AS 07H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 8 THEN  T.CONTENT_IDS END) 	AS 08H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 9 THEN  T.CONTENT_IDS END) 	AS 09H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 10 THEN  T.CONTENT_IDS END) 	AS 10H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 11 THEN  T.CONTENT_IDS END) 	AS 11H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 12 THEN  T.CONTENT_IDS END) 	AS 12H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 13 THEN  T.CONTENT_IDS END) 	AS 13H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 14 THEN  T.CONTENT_IDS END) 	AS 14H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 15 THEN  T.CONTENT_IDS END) 	AS 15H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 16 THEN  T.CONTENT_IDS END) 	AS 16H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 17 THEN  T.CONTENT_IDS END) 	AS 17H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 18 THEN  T.CONTENT_IDS END) 	AS 18H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 19 THEN  T.CONTENT_IDS END) 	AS 19H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 20 THEN  T.CONTENT_IDS END) 	AS 20H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 21 THEN  T.CONTENT_IDS END) 	AS 21H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 22 THEN  T.CONTENT_IDS END) 	AS 22H"
//					+ "			,COUNT(CASE WHEN T.LOG_HOUR = 23 THEN  T.CONTENT_IDS END) 	AS 23H"
//					+ " FROM (  SELECT   B.CATALOG_ID"
//					+ "					,A.CONTENT_CATEGORY"
//					+ "					,B.LOG_DATE"					
//					+ "					,B.LOG_HOUR"
//					+ "					,A.CONTENT_IDS"
//					+ "					,A.VALUE"										
//					+ " 		FROM 	V_FEED A INNER JOIN V_ADD_TO_CART B "
//					+ " 		ON		A.CONTENT_IDS = B.CONTENT_IDS ) T "
//					+ " GROUP BY T.CATALOG_ID, T.CONTENT_CATEGORY, T.LOG_DATE"
//					);
//
//			resultDF3.printSchema();
//			resultDF3.show(50);			
//			resultDF3.coalesce(1).write().option("header", "true").csv("D:/download/"+logDate+"addtocart_amount_category_hourly.csv");
		
		}catch(Exception e){
			new Exception(e);
		}finally{
			if(jsc != null){
				jsc.close();
			}
		}		
	}
}