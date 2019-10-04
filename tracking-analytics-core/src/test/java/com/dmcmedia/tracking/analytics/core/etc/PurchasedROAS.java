package com.dmcmedia.tracking.analytics.core.etc;

import java.util.Properties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import com.dmcmedia.tracking.analytics.common.factory.SparkMongodbConnectionFactory;
import com.dmcmedia.tracking.analytics.common.map.MongodbMap;
import com.dmcmedia.tracking.analytics.common.test.CommonJunitTest;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

public class PurchasedROAS extends CommonJunitTest{
	
	@Test
	public void createPurchasedROAS(){
		System.setProperty("hadoop.home.dir", "C:/develop/tracking/winutils");
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();
		//String catalogId = "1921855474765164";
		String month = "08";
		String logDate = "2017-08-21";
		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
		
		try{
			Dataset<Row> viewContentDs = MongoSpark.load(jsc, ReadConfig.create(sparkSession)
						.withOption("database", MongodbMap.DATABASE_RAW_MONTH+ month)
						.withOption("collection",  logDate	+ MongodbMap.COLLECTION_PIXEL_VIEW_CONTENT)).toDF();
			
			Dataset<Row> purchasedDs = MongoSpark.load(jsc, ReadConfig.create(sparkSession)
						.withOption("database", MongodbMap.DATABASE_PROCESSED_MONTH + month)
						.withOption("collection",  logDate	+ MongodbMap.COLLECTION_PIXEL_PURCHASED)).toDF();
			
			viewContentDs.createOrReplaceTempView("T_VIEW_CONTENT");
			purchasedDs.createOrReplaceTempView("T_PURCHASED");
			
//			viewContentDs.printSchema();
//			viewContentDs.show();
//			
//			purchasedDs.printSchema();
//			purchasedDs.show();
			
			
			Dataset<Row> viewContentDF = sparkSession.sql("SELECT 	 	 viewContent.content_ids	AS CONTENT_IDS "
															+ "			,viewContent.p_cid			AS P_CID "
															+ "			,viewContent.catalog_id		AS CATALOG_ID "
															+ "			,viewContent.catalog_id		AS CATALOG_ID "
															+ "			,logDateTime				AS LOGDATETIME"
															+ " FROM 	T_VIEW_CONTENT "
															+ " WHERE	viewContent.referrer LIKE '%media=LLK%' ");
			
			viewContentDF.printSchema();
			viewContentDF.show();
			viewContentDF.createOrReplaceTempView("V_VIEW_CONTENT");
			
			Dataset<Row> purchasedDF = sparkSession.sql("SELECT	 contentIds	AS CONTENT_IDS"
													+ "			,value			AS VALUE"
													+ "			,pCid			AS P_CID"
//													+ "			,"
													+ " FROM 	T_PURCHASED");

			purchasedDF.createOrReplaceTempView("V_PURCHASED");
//			purchasedDF.printSchema();
//			purchasedDF.show();
			
			Dataset<Row> resultDF;
			resultDF = sparkSession.sql(
					  " SELECT 	 A.P_CID "
					  + "		,A.CONTENT_IDS "
					  + "		,CAST(B.VALUE AS LONG) AS VALUE "
					  + " FROM	V_VIEW_CONTENT A INNER JOIN V_PURCHASED  B "
					  + " ON 	A.P_CID = B.P_CID "
					  );
			
			Properties connectionProperties = new Properties();
			connectionProperties.put("user", "dmcmedia");
			connectionProperties.put("password", "dk#007Gek");
			connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver");	
			connectionProperties.put("lowerBound", "1000");
			connectionProperties.put("upperBound", "1000");
			
			resultDF.printSchema();
			resultDF.show();			
			resultDF.write().jdbc("jdbc:mysql://10.94.1.212:3306/tracking?characterEncode=utf8&serverTimezone=UTC", "TMP_TRK_PURCHASED_ROAS", connectionProperties);
			
		}catch(Exception e){
			new Exception(e);
		}finally{
			if(jsc != null){
				jsc.close();
			}
		}		
	}
}