package com.dmcmedia.tracking.analytics.core.process.feed;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.AbstractDataProcess;
import com.dmcmedia.tracking.analytics.common.DataProcess;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.map.MongodbMap;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.dataset.feed.ProductsFeed;
import com.dmcmedia.tracking.analytics.core.dataset.feed.Purchased;
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
* 2017. 8. 14.   cshwang   Initial Release
*
* 각 Feed별 카테고리 생성
*
************************************************************************************************/
public class CategoriesPixel extends AbstractDataProcess implements DataProcess {
	
	/**
	 * 생성자
	 */
	public CategoriesPixel(){
		super();
	}

	/**
	 *
	 * @param args
	 * @throws InterruptedException
	 */
	public boolean processDataSet(SparkSession sparkSession){		

		JavaSparkContext jsc = null;	
		String processedDataBase = getProcessedDataBase();	    
	    Dataset<Row> pixelPurchasedDS = null;
	    Dataset<Row> pixelProdictsFeedDS = null;
	    
		if(super.isValid() == false){
			return false;
		}
		
		try{
		    jsc = new JavaSparkContext(sparkSession.sparkContext());
 
		    pixelPurchasedDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database", processedDataBase).withOption("collection",  this.getProcessDate() + MongodbMap.COLLECTION_PIXEL_PURCHASED)).toDF(Purchased.class);	
		    pixelProdictsFeedDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database", processedDataBase).withOption("collection", this.getProcessDate() + MongodbMap.COLLECTION_PIXEL_FEED_DAILY)).toDF(ProductsFeed.class);
		    pixelPurchasedDS.createOrReplaceTempView("T_PIXEL_PURCHASED");
		    pixelProdictsFeedDS.createOrReplaceTempView("T_PIXEL_FEED");	
		   		    
			Dataset<Row> contentCategoryLv1DS = sparkSession.sql("SELECT      	 A.catalogId		                										AS catalogId"
																	+ "			,SUBSTRING_INDEX(A.contentCategory, '~', 1)	                	    		AS contentCategoryLv1"
																	+ "			,CAST(NVL(SUM(B.value), 0) AS BIGINT)										AS totalValue"
																	+ "			,NVL(COUNT(B.catalogId), 0)	                								AS totalCount"
																	+ "			,CAST(NVL(ROUND(SUM(B.value) /COUNT(B.catalogId)), 0) AS BIGINT) 			AS averageValue "
																	+ "			,RANK() OVER (PARTITION BY A.catalogId ORDER BY SUM(B.value) DESC)			AS vRankLv1 "
																	+ "			,RANK() OVER (PARTITION BY A.catalogId ORDER BY COUNT(A.contentIds) DESC)	AS cRankLv1 "
																	+ " FROM 	T_PIXEL_FEED A INNER JOIN T_PIXEL_PURCHASED B "
																	+ "			ON 	A.catalogId	= B.catalogId"
																	+ "			AND	A.contentIds = B.contentIds"
																	+ "	WHERE   TRIM(A.catalogId) != '' "
																	+ " AND     TRIM(A.contentCategory) != ''"
																	+ " AND     TRIM(B.value) != ''"
																	+ " GROUP BY A.catalogId, SUBSTRING_INDEX(A.contentCategory, '~', 1)");

			MongoSpark.write(contentCategoryLv1DS)
					.option("database", getProcessedDataBase())
					.option("collection", getProcessDate() + MongodbMap.COLLECTION_PIXEL_CATEGORY_LV1).mode(DEFAULT_SAVEMODE).save();

			contentCategoryLv1DS.createOrReplaceTempView("V_CATALOG_LV1");

			Dataset<Row> contentCategoryLv2DS = sparkSession.sql("SELECT      	 A.catalogId			                									AS catalogId"
																	+ "			,SUBSTRING_INDEX(A.contentCategory, '~', 1)	                	    		AS contentCategoryLv1"
																	+ "			,SUBSTRING_INDEX(SUBSTRING_INDEX(A.contentCategory, '~', 2), '~', -1) 		AS contentCategoryLv2"
																	+ "			,CAST(NVL(SUM(B.value), 0) AS BIGINT)										AS totalValue"
																	+ "			,NVL(COUNT(B.catalogId), 0)	                								AS totalCount"
																	+ "			,CAST(NVL(ROUND(SUM(B.value) /COUNT(B.catalogId)), 0) AS BIGINT) 			AS averageValue "
																	+ "         ,MAX(C.vRankLv1) 															AS vRankLv1"
																	+ "         ,MAX(C.cRankLv1)															AS cRankLv1"	
																	+ "			,RANK() OVER (PARTITION BY A.catalogId ORDER BY SUM(B.value) DESC)			AS vRankLv2"
																	+ "			,RANK() OVER (PARTITION BY A.catalogId ORDER BY COUNT(A.contentIds) DESC)	AS cRankLv2"
																	+ " FROM 	T_PIXEL_FEED A INNER JOIN T_PIXEL_PURCHASED B "
																	+ "			ON 	A.catalogId	= B.catalogId"
																	+ "			AND	A.contentIds 	= B.contentIds"
																	+ "         INNER JOIN V_CATALOG_LV1 C"
																	+ "         ON  A.catalogId = C.catalogId"
																	+ "         AND SUBSTRING_INDEX(A.contentCategory, '~', 1) = C.contentCategoryLv1"
																	+ "	WHERE   A.catalogId != '' "
																	+ " AND     A.contentCategory != '' "
																	+ " GROUP BY A.catalogId, SUBSTRING_INDEX(A.contentCategory, '~', 1), SUBSTRING_INDEX(SUBSTRING_INDEX(A.contentCategory, '~', 2), '~', -1)");
			MongoSpark.write(contentCategoryLv2DS)
					.option("database", getProcessedDataBase())
					.option("collection", getProcessDate() + MongodbMap.COLLECTION_PIXEL_CATEGORY_LV2).mode(DEFAULT_SAVEMODE).save();

			contentCategoryLv2DS.createOrReplaceTempView("V_CATALOG_LV2");
			Dataset<Row> resultDS = sparkSession.sql("SELECT     A.catalogId			                									AS catalogId"
													+ "			,A.contentCategory	                										AS contentCategory"
													+ "			,SUBSTRING_INDEX(A.contentCategory, '~', 1)	                	    		AS contentCategoryLv1"
													+ "			,SUBSTRING_INDEX(SUBSTRING_INDEX(A.contentCategory, '~', 2), '~', -1) 		AS contentCategoryLv2"
													+ "			,SUBSTRING_INDEX(SUBSTRING_INDEX(A.contentCategory, '~', 3), '~', -1) 		AS contentCategoryLv3"
													+ "			,CAST(NVL(SUM(B.value), 0) AS BIGINT)										AS totalValue"
													+ "			,NVL(COUNT(B.catalogId), 0)	                								AS totalCount"
													+ "			,CAST(NVL(ROUND(SUM(B.value) /COUNT(B.catalogId)), 0) AS BIGINT) 			AS averageValue "
													+ "         ,MAX(C.vRankLv1) 															AS vRankLv1"
													+ "         ,MAX(C.cRankLv1) 															AS cRankLv1"
													+ "         ,MAX(C.vRankLv2) 															AS vRankLv2"
													+ "         ,MAX(C.cRankLv2) 															AS cRankLv2"
													+ "			,RANK() OVER (PARTITION BY A.catalogId ORDER BY SUM(B.value) DESC)			AS vRankLv3"
													+ "			,RANK() OVER (PARTITION BY A.catalogId ORDER BY COUNT(A.contentIds) DESC)	AS cRankLv3"
													+ " FROM 	T_PIXEL_FEED A INNER JOIN T_PIXEL_PURCHASED B "
													+ "			ON 	A.catalogId	= B.catalogId"
													+ "			AND	A.contentIds 	= B.contentIds"
													+ "         INNER JOIN V_CATALOG_LV2 C"
													+ "         ON  A.catalogId = C.catalogId"
													+ "         AND SUBSTRING_INDEX(A.contentCategory, '~', 1) = C.contentCategoryLv1"
													+ "         AND SUBSTRING_INDEX(SUBSTRING_INDEX(A.contentCategory, '~', 2), '~', -1) = C.contentCategoryLv2"
													+ "	WHERE   A.catalogId != '' "
													+ " AND     A.contentCategory != '' "
													+ " GROUP BY A.catalogId, A.contentCategory");

			MongoSpark.write(resultDS).option("database", getProcessedDataBase())
					.option("collection", getProcessDate() + MongodbMap.COLLECTION_PIXEL_CATEGORY).mode(DEFAULT_SAVEMODE)
					.save();
		}catch(Exception e){
			LogStack.process.error(ExceptionMap.PROCESS_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.process.error(e);
		}		
		return true;
	}
}