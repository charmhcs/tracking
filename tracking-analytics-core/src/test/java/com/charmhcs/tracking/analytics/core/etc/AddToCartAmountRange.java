package com.dmcmedia.tracking.analytics.core.etc;

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
public class AddToCartAmountRange extends CommonJunitTest{
	
	@Before
	public void setUp() {
		super.atomicInteger = new AtomicInteger(0);
	}
	
	@Test
	public void createAddToCartAmountOfMoney(){
		
		System.setProperty("hadoop.home.dir", "C:/develop/tracking/winutils");		
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();
//		String catalogId = "1921855474765164";
		String month = "08";
		String logDate = "2017-08-21";
		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());		
		
		try{
			/*add to cart에 undefined가 많아서 Test로.. 처리 */
//		    Dataset<Row> feedListDs = MongoSpark.load(jsc, ReadConfig.create(sparkSession)
//					.withOption("database", MongodbMap.DATABASE_PROCESSED_FEED)
//					.withOption("collection",  MongodbMap.COLLECTION_FEED + MongodbMap.COLLECTION_DOT  + catalogId)).toDF();
					
			Dataset<Row> addToCartDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession)
					.withOption("database", MongodbMap.DATABASE_PROCESSED_MONTH + month)
					.withOption("collection",  logDate	+ MongodbMap.COLLECTION_PIXEL_PURCHASED)).toDF();	
			
//		    feedListDs.createOrReplaceTempView("T_FEED");
		    addToCartDS.createOrReplaceTempView("T_ADD_TO_CART");
		    
//		    Dataset<Row> feedDF = sparkSession.sql(
//		    		 "SELECT    content_ids			AS CONTENT_IDS"
//					+ "			,content_category	AS CONTENT_CATEGORY"
//					+ "			,value				AS VALUE"
//					+ "			,currency			AS CURRENCY"
//					+ " FROM 	T_FEED");
//			feedDF.createOrReplaceTempView("V_FEED");		
			
			Dataset<Row> addToCartbyCatalogDF = sparkSession.sql(
					  "SELECT	 content_ids		AS CONTENT_IDS"
					+ "			,catalog_id			AS CATALOG_ID"
					+ "			,value				AS VALUE"
					+ "			,CAST(DATE(log_date_time)	AS STRING)	AS LOG_DATE"
					+ " FROM 	T_ADD_TO_CART ");
			addToCartbyCatalogDF.createOrReplaceTempView("V_ADD_TO_CART");
			addToCartbyCatalogDF.printSchema();
			addToCartbyCatalogDF.show();
			
			Dataset<Row> resultDF;
			resultDF = sparkSession.sql(
					  " SELECT   T.CATALOG_ID				AS catalogId"
					+ "			,T.LOG_DATE  				AS logDate"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 0 AND 9999 THEN T.CONTENT_IDS END) AS FAM001"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 10000 AND 19999 THEN T.CONTENT_IDS END) AS FAM002"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 20000 AND 29999 THEN T.CONTENT_IDS END) AS FAM003"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 30000 AND 39999 THEN T.CONTENT_IDS END) AS FAM004"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 40000 AND 49999 THEN T.CONTENT_IDS END) AS FAM005"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 50000 AND 59999 THEN T.CONTENT_IDS END) AS FAM006"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 60000 AND 69999 THEN T.CONTENT_IDS END) AS FAM007"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 70000 AND 79999 THEN T.CONTENT_IDS END) AS FAM008"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 80000 AND 89999 THEN T.CONTENT_IDS END) AS FAM009"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 90000 AND 99999 THEN T.CONTENT_IDS END) AS FAM010"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 100000 AND 149999 THEN T.CONTENT_IDS END) AS FAM011"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 150000 AND 199999 THEN T.CONTENT_IDS END) AS FAM012"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 200000 AND 299999 THEN T.CONTENT_IDS END) AS FAM013"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 300000 AND 399999 THEN T.CONTENT_IDS END) AS FAM014"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 400000 AND 499999 THEN T.CONTENT_IDS END) AS FAM015"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 500000 AND 699999 THEN T.CONTENT_IDS END) AS FAM016"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 700000 AND 999999 THEN T.CONTENT_IDS END) AS FAM017"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 1000000 AND 1499999 THEN T.CONTENT_IDS END) AS FAM018"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 1500000 AND 1999999 THEN T.CONTENT_IDS END) AS FAM019"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 2000000 AND 2999999 THEN T.CONTENT_IDS END) AS FAM020"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 3000000 AND 4999999 THEN T.CONTENT_IDS END) AS FAM021"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 5000000 AND 6999999 THEN T.CONTENT_IDS END) AS FAM022"
					+ "			,COUNT(CASE WHEN VALUE BETWEEN 7000000 AND 9999999 THEN T.CONTENT_IDS END) AS FAM023"
					+ "			,COUNT(CASE WHEN VALUE >= 10000000  THEN T.CONTENT_IDS END) AS FAM024"
					+ " FROM 	(SELECT  CATALOG_ID"
					+ "					,LOG_DATE"
					+ "					,VALUE"
					+ "					,CONTENT_IDS"
					+ " 		FROM 	V_ADD_TO_CART ) T "
					+ " GROUP BY T.CATALOG_ID, T.LOG_DATE");
			resultDF.printSchema();
			resultDF.show();		
			resultDF.coalesce(1).write().option("header", "true").csv("D:/download/"+logDate+"addtocart_amount.csv");
		
		}catch(Exception e){
			new Exception(e);
		}finally{
			if(jsc != null){
				jsc.close();
			}
		}
	}
}
