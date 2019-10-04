package com.dmcmedia.tracking.analytics.core.process.feed;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.map.MongodbMap;
import com.dmcmedia.tracking.analytics.common.util.HttpUtil;
import com.dmcmedia.tracking.analytics.common.util.PropertyLoader;
import com.mongodb.spark.MongoSpark;

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
* 2017. 7. 18.   cshwang   Initial Release
*
*
************************************************************************************************/
public class FeedFromText {

	/**
	 * Main
	 * @param args
	 * @throws InterruptedException
	 * @throws IOException 
	 */
	public boolean createNewFileFeedForWebDownload (SparkSession sparkSession, String httpLoaction, String catalogId) throws InterruptedException, IOException {
		
		String fileDownloadPath = PropertyLoader.getProperty("tracking.spark.file.temp.dir");
		HttpUtil.downloadFile("http://ipp.interpark.com/partner/bestBuyer/all_goods.txt", fileDownloadPath);
		Dataset<Row> df = sparkSession.read().format("com.databricks.spark.xml")
								                .option("rowTag", "product>>")		
								                .option("charset", "euc-kr")
								                .load("D:/download/all_goods.txt");
		df.show();
		df.createOrReplaceTempView("T_PRODUCT_RAW");
		
		Dataset<Row> feedDF = sparkSession.sql(""
				+ "SELECT 	 SUBSTRING_INDEX(SUBSTRING_INDEX(T.RECORD, ',', 2), ',', -1) 	AS content_id "
				+ "			,SUBSTRING_INDEX(SUBSTRING_INDEX(T.RECORD, ',', 2), ',', -1) 	AS content_ids "
				+ "			,SUBSTRING_INDEX(SUBSTRING_INDEX(T.RECORD, ',', 3), ',', -1) 	AS content_name "
				+ "			,SUBSTRING_INDEX(SUBSTRING_INDEX(T.RECORD, ',', 11), ',', -1) 	AS image_link "
				+ "			,SUBSTRING_INDEX(SUBSTRING_INDEX(T.RECORD, ',', 5), ',', -1) 	AS content_category "
				+ "			,SUBSTRING_INDEX(SUBSTRING_INDEX(T.RECORD, ',', 10), ',', -1) 	AS link "				
				+ "			,SUBSTRING_INDEX(SUBSTRING_INDEX(T.RECORD, ',', 6), ',', -1) 	AS description "
				+ "			,'in stock' 		AS availability "
				+ "			,'"+catalogId+"' 	AS catalog_id "
				+ "			,'product' 			AS content_type "
				+ "			,'KRW' 				AS currency "
				+ "			,CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(T.RECORD, ',', 13), ',', -1) AS LONG) AS value "
				+ "			,SUBSTRING_INDEX(SUBSTRING_INDEX(T.RECORD, ',', 8), ',', -1) AS brand "
				+ " FROM (SELECT REGEXP_REPLACE(_corrupt_record, '\n<<<.*[가-힣|A-Z|a-z]+.*>>>', ',') AS RECORD FROM T_PRODUCT_RAW) T");
		feedDF.printSchema();
		feedDF.show();
		
		MongoSpark.write(feedDF).option("database", MongodbMap.DATABASE_PROCESSED_FEED)
							.option("collection", MongodbMap.COLLECTION_PIXEL_FEED + MongodbMap.COLLECTION_DOT + catalogId)							
							.mode(SaveMode.Overwrite).save();
		sparkSession.stop();
		return true;		
	}
}
