package com.dmcmedia.tracking.analytics.core.process.event;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import com.dmcmedia.tracking.analytics.common.AbstractDataProcess;
import com.dmcmedia.tracking.analytics.common.factory.MongodbFactory;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.map.MongodbMap;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.config.RawDataSQLTempView;
import com.dmcmedia.tracking.analytics.core.dataset.feed.ProductsFeed;
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
* 2017. 10. 10.   cshwang   Initial Release
*
*
************************************************************************************************/
public class EventEcomm extends AbstractDataProcess{
	/**
	 * 생성자
	 */
	public EventEcomm(){
		super();
	}
	
	/**
	 * 생성자 
	 * @param runType
	 */
	public EventEcomm(String runType){
		super(runType);
	}

	/* (non-Javadoc)
	 * @see com.dmcmedia.tracking.analysis.common.AbstractSparkSessionAnalysis#analysisDataSet(org.apache.spark.sql.SparkSession)
	 */
	public boolean processDataSet(SparkSession sparkSession) {

		JavaSparkContext jsc = null;
		Dataset<ProductsFeed> pixelProductsFeedDS = null;
		Dataset<Row> addtocartDS = null;
		Dataset<Row> viewcontentDS = null;
		
		if(super.isValid() == false){
			return false;
		}
		
		try{
			
			if(EACH.equals(this.getRunType().name())){
		    	RawDataSQLTempView rawDataSQLTempView = new RawDataSQLTempView(getProcessDate());
		    	rawDataSQLTempView.createPixelRawDataDailyTempView(sparkSession);
			}
			jsc = new JavaSparkContext(sparkSession.sparkContext());
			
			pixelProductsFeedDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database", getProcessedDataBase()).withOption("collection", getProcessDate() + MongodbMap.COLLECTION_PIXEL_FEED_DAILY)).toDS(ProductsFeed.class);
			pixelProductsFeedDS.persist(StorageLevel.MEMORY_AND_DISK_SER()).createOrReplaceTempView("T_PIXEL_FEED");

			addtocartDS = sparkSession.sql("SELECT	'ADD_TO_CART'					AS	eventType"
									+ "				,addToCart.pixel_id				AS	pixelId"
									+ "				,addToCart.catalog_id			AS	catalogId"
									+ "				,addToCart.content_ids			AS	contentIds"
									+ "				,addToCart.p_cid				AS	pCid"
									+ "				,addToCart.value		   		AS  value"
									+ "				,logDateTime					AS	logDateTime"
									+ "				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logDateTime, 'yyyy-mm-dd HH') AS TIMESTAMP), 'HH')	AS hour"
									+ "	FROM		T_PIXEL_ADD_TO_CART").persist(StorageLevel.MEMORY_AND_DISK_SER());

			viewcontentDS = sparkSession.sql("SELECT	'VIEW_CONTENT'			   	AS  eventType"
									+ "			,viewContent.catalog_id	   	AS  catalogId"
									+ "			,viewContent.pixel_id	   	AS  pixelId"
									+ "			,viewContent.content_ids   	AS  contentIds"
									+ "			,viewContent.p_cid		   	AS  pCid"
									+ "			,viewContent.value		   	AS  value"
									+ "			,logDateTime				AS	logDateTime"
									+ "			,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logDateTime, 'yyyy-mm-dd HH') AS TIMESTAMP), 'HH')	AS hour"
									+ "	FROM	T_PIXEL_VIEW_CONTENT").persist(StorageLevel.MEMORY_AND_DISK_SER());

			
			viewcontentDS.createOrReplaceTempView("V_PIXEL_VIEW_CONTENT");
			addtocartDS.createOrReplaceTempView("V_PIXEL_ADD_TO_CART");

			this.event(sparkSession);
			this.amountEvent(sparkSession);
			this.productsEvent(sparkSession);

		}catch(Exception e){
			LogStack.process.error(ExceptionMap.PROCESS_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.process.error(e);
		}finally {
			if(pixelProductsFeedDS != null) {
				pixelProductsFeedDS.unpersist();
			}
			if(addtocartDS != null) {
				addtocartDS.unpersist();	
			}
			if(viewcontentDS != null) {
				viewcontentDS.unpersist();
			}
		}
		return true;
	}

	/**
	 * @param sparkSession
	 * @param platformCode
	 */
	private void event(SparkSession sparkSession){
		LogStack.process.info("ProcessEvent : Event Start");
		Dataset<Row> eventResultDS = sparkSession.sql("   SELECT	 A.pixelId"
														+ "			,A.catalogId"
														+ "			,B.contentCategory"
														+ "			,SUBSTRING_INDEX(B.contentCategory, '~', 1)	                	    	AS 	contentCategoryLv1"
														+ "			,SUBSTRING_INDEX(SUBSTRING_INDEX(B.contentCategory, '~', 2), '~', -1) 	AS 	contentCategoryLv2"
														+ "			,SUBSTRING_INDEX(SUBSTRING_INDEX(B.contentCategory, '~', 3), '~', -1) 	AS 	contentCategoryLv3"
														+ "			,A.contentIds"
														+ "			,B.contentName"
														+ "			,COUNT(CASE WHEN A.eventType = 'VIEW_CONTENT' THEN A.catalogId END)		AS viewcontent"
														+ "			,COUNT(CASE WHEN A.eventType = 'ADD_TO_CART'   THEN A.catalogId END)	AS addtocart"
														+ "			,COUNT(CASE WHEN A.eventType = 'PURCHASED'   THEN A.catalogId END)		AS purchased"
														+ "			,RANK() OVER (PARTITION BY A.catalogId ORDER BY COUNT(CASE WHEN A.eventType = 'VIEW_CONTENT' THEN A.contentIds END) DESC)	AS viewcontentRank "
														+ "			,RANK() OVER (PARTITION BY A.catalogId ORDER BY COUNT(CASE WHEN A.eventType = 'ADD_TO_CART' THEN A.contentIds END) DESC)	AS addtocartRank "
														+ "			,RANK() OVER (PARTITION BY A.catalogId ORDER BY COUNT(CASE WHEN A.eventType = 'PURCHASED' THEN A.contentIds END) DESC)		AS purchasedRank "
														+ "			,'"+getProcessDate()+"'								AS 	processDate"
														+ " FROM (  SELECT 	 eventType"
														+ "	        		,pixelId"
														+ "	        		,catalogId"
														+ "			       	,contentIds"
														+ "     	FROM    V_PIXEL_VIEW_CONTENT"
														+ "	 "
														+ "     	UNION ALL "
														+ "	 "
														+ "         SELECT 	 eventType"
														+ "     			,pixelId"
														+ "     			,catalogId"
														+ "			        ,contentIds"
														+ "	        FROM    V_PIXEL_ADD_TO_CART "
														+ "	 "
														+ "     	UNION ALL "
														+ "	 "
														+ "         SELECT 	'PURCHASED'				AS 	eventType"
														+ "     			,pixelId"
														+ "     			,catalogId"
														+ "			        ,contentIds"
														+ "     	FROM    T_PIXEL_PURCHASED "
														+ "     	) A INNER JOIN T_PIXEL_FEED B ON A.pixelId = B.pixelId AND  A.catalogId = B.catalogId AND A.contentIds = B.contentIds"
														+ "	WHERE   A.pixelId != '' "
														+ " AND     B.contentCategory != '' "
														+ " GROUP BY A.pixelId, A.catalogId, B.contentCategory, A.contentIds, B.contentName");
		
		MongodbFactory.dropAndCreateAndShardingCollection(getProcessedDataBase(), getProcessDate() + MongodbMap.COLLECTION_PIXEL_EVENT );
		MongoSpark.write(eventResultDS).option("database", getProcessedDataBase())
					.option("collection",getProcessDate() + MongodbMap.COLLECTION_PIXEL_EVENT )
					.mode(DEFAULT_SAVEMODE)
					.save();
		LogStack.process.info("ProcessEvent : Event End");
	}

	/**
	 * @param sparkSession
	 * @param platformCode
	 */
	private void amountEvent(SparkSession sparkSession){
		LogStack.process.info("ProcessEvent :  Amount Start");
		Dataset<Row> amountResultDS =  sparkSession.sql("SELECT 	 B.eventType    							AS	eventType"
														+ "			,A.pixelId									AS	pixelId"
														+ "			,A.catalogId								AS	catalogId"
														+ "			,A.contentCategory							AS	contentCategory"
														+ "			,SUBSTRING_INDEX(A.contentCategory, '~', 1)	                	    AS 	contentCategoryLv1"
														+ "			,SUBSTRING_INDEX(SUBSTRING_INDEX(A.contentCategory, '~', 2), '~', -1) 	AS 	contentCategoryLv2"
														+ "			,SUBSTRING_INDEX(SUBSTRING_INDEX(A.contentCategory, '~', 3), '~', -1) 	AS 	contentCategoryLv3"
														+ "			,A.currency								    AS	currency"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 0 AND 9999 THEN B.contentIds END) AS FCN001"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 10000 AND 19999 THEN B.contentIds END) AS FCN002"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 20000 AND 29999 THEN B.contentIds END) AS FCN003"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 30000 AND 39999 THEN B.contentIds END) AS FCN004"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 40000 AND 49999 THEN B.contentIds END) AS FCN005"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 50000 AND 59999 THEN B.contentIds END) AS FCN006"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 60000 AND 69999 THEN B.contentIds END) AS FCN007"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 70000 AND 79999 THEN B.contentIds END) AS FCN008"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 80000 AND 99999 THEN B.contentIds END) AS FCN009"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 100000 AND 149999 THEN B.contentIds END) AS FCN010"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 150000 AND 199999 THEN B.contentIds END) AS FCN011"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 200000 AND 299999 THEN B.contentIds END) AS FCN012"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 300000 AND 399999 THEN B.contentIds END) AS FCN013"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 400000 AND 499999 THEN B.contentIds END) AS FCN014"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 500000 AND 699999 THEN B.contentIds END) AS FCN015"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 700000 AND 999999 THEN B.contentIds END) AS FCN016"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 1000000 AND 1499999 THEN B.contentIds END) AS FCN017"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 1500000 AND 1999999 THEN B.contentIds END) AS FCN018"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 2000000 AND 2999999 THEN B.contentIds END) AS FCN019"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 3000000 AND 4999999 THEN B.contentIds END) AS FCN020"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 5000000 AND 6999999 THEN B.contentIds END) AS FCN021"
														+ "			,COUNT(CASE WHEN B.value BETWEEN 7000000 AND 9999999 THEN B.contentIds END) AS FCN022"
														+ "			,COUNT(CASE WHEN B.value >= 10000000  THEN B.contentIds END) AS FCN099"
														+ "			,SUM(CASE WHEN B.value BETWEEN 0 AND 9999 THEN B.value END) AS FAM001"
														+ "			,SUM(CASE WHEN B.value BETWEEN 10000 AND 19999 THEN B.value ELSE 0 END) AS FAM002"
														+ "			,SUM(CASE WHEN B.value BETWEEN 20000 AND 29999 THEN B.value ELSE 0 END) AS FAM003"
														+ "			,SUM(CASE WHEN B.value BETWEEN 30000 AND 39999 THEN B.value ELSE 0 END) AS FAM004"
														+ "			,SUM(CASE WHEN B.value BETWEEN 40000 AND 49999 THEN B.value ELSE 0 END) AS FAM005"
														+ "			,SUM(CASE WHEN B.value BETWEEN 50000 AND 59999 THEN B.value ELSE 0 END) AS FAM006"
														+ "			,SUM(CASE WHEN B.value BETWEEN 60000 AND 69999 THEN B.value ELSE 0 END) AS FAM007"
														+ "			,SUM(CASE WHEN B.value BETWEEN 70000 AND 79999 THEN B.value ELSE 0 END) AS FAM008"
														+ "			,SUM(CASE WHEN B.value BETWEEN 80000 AND 99999 THEN B.value ELSE 0 END) AS FAM009"
														+ "			,SUM(CASE WHEN B.value BETWEEN 100000 AND 149999 THEN B.value ELSE 0 END) AS FAM010"
														+ "			,SUM(CASE WHEN B.value BETWEEN 150000 AND 199999 THEN B.value ELSE 0 END) AS FAM011"
														+ "			,SUM(CASE WHEN B.value BETWEEN 200000 AND 299999 THEN B.value ELSE 0 END) AS FAM012"
														+ "			,SUM(CASE WHEN B.value BETWEEN 300000 AND 399999 THEN B.value ELSE 0 END) AS FAM013"
														+ "			,SUM(CASE WHEN B.value BETWEEN 400000 AND 499999 THEN B.value ELSE 0 END) AS FAM014"
														+ "			,SUM(CASE WHEN B.value BETWEEN 500000 AND 699999 THEN B.value ELSE 0 END) AS FAM015"
														+ "			,SUM(CASE WHEN B.value BETWEEN 700000 AND 999999 THEN B.value ELSE 0 END) AS FAM016"
														+ "			,SUM(CASE WHEN B.value BETWEEN 1000000 AND 1499999 THEN B.value ELSE 0 END) AS FAM017"
														+ "			,SUM(CASE WHEN B.value BETWEEN 1500000 AND 1999999 THEN B.value ELSE 0 END) AS FAM018"
														+ "			,SUM(CASE WHEN B.value BETWEEN 2000000 AND 2999999 THEN B.value ELSE 0 END) AS FAM019"
														+ "			,SUM(CASE WHEN B.value BETWEEN 3000000 AND 4999999 THEN B.value ELSE 0 END) AS FAM020"
														+ "			,SUM(CASE WHEN B.value BETWEEN 5000000 AND 6999999 THEN B.value ELSE 0 END) AS FAM021"
														+ "			,SUM(CASE WHEN B.value BETWEEN 7000000 AND 9999999 THEN B.value ELSE 0 END) AS FAM022"
														+ "			,SUM(CASE WHEN B.value BETWEEN 0 AND 49999 THEN B.value ELSE 0 END) AS FAM101"
						            					+ "			,SUM(CASE WHEN B.value BETWEEN 50000 AND 99999 THEN B.value ELSE 0 END) AS FAM102"
						            					+ "			,SUM(CASE WHEN B.value BETWEEN 100000 AND 199999 THEN B.value ELSE 0 END) AS FAM103"
						            					+ "			,SUM(CASE WHEN B.value BETWEEN 200000 AND 499999 THEN B.value ELSE 0 END) AS FAM113"
						            					+ "			,SUM(CASE WHEN B.value BETWEEN 500000 AND 999999 THEN B.value ELSE 0 END) AS FAM104"
						            					+ "			,SUM(CASE WHEN B.value BETWEEN 1000000 AND 2999999 THEN B.value ELSE 0 END) AS FAM105"
						            					+ "			,SUM(CASE WHEN B.value BETWEEN 3000000 AND 4999999 THEN B.value ELSE 0 END) AS FAM106"
						            					+ "			,SUM(CASE WHEN B.value BETWEEN 5000000 AND 9999999 THEN B.value ELSE 0 END) AS FAM107"
														+ "			,SUM(CASE WHEN B.value >= 10000000 THEN B.value ELSE 0 END) AS FAM099"
														+ "			,'"+getProcessDate()+"'	 AS 	processDate"
														+ " FROM	T_PIXEL_FEED A INNER JOIN  "
														+ "			(SELECT	 eventType"
														+ "					,pixelId"
														+ "					,catalogId"
														+ "					,contentIds"
														+ "					,value"
														+ "					,logDateTime"
														+ "			FROM	V_PIXEL_VIEW_CONTENT"
														+ "	 "
														+ "			UNION ALL "
														+ "	 "
														+ "			SELECT	 eventType"
														+ "					,pixelId"
														+ "					,catalogId"
														+ "					,contentIds"
														+ "					,value"
														+ "					,logDateTime"
														+ "			FROM	V_PIXEL_ADD_TO_CART"
														+ "	 "
														+ "			UNION ALL "
														+ "	 "
														+ "			SELECT	'PURCHASED'				AS 	eventType"
														+ "					,pixelId"
														+ "					,catalogId"
														+ "					,contentIds"
														+ "					,value"
														+ "					,logDateTime"
														+ "			FROM	T_PIXEL_PURCHASED	 "
														+ "			) B "
														+ "			ON	A.catalogId	= B.catalogId"
														+ "			AND	A.contentIds	= B.contentIds"
														+ " GROUP BY B.eventType, A.pixelId, A.catalogId, A.contentCategory, A.currency ");
			
		MongodbFactory.dropAndCreateAndShardingCollection(getProcessedDataBase(), getProcessDate() + MongodbMap.COLLECTION_PIXEL_EVENT_AMOUNT );
		MongoSpark.write(amountResultDS).option("database", getProcessedDataBase())
					.option("collection",getProcessDate() + MongodbMap.COLLECTION_PIXEL_EVENT_AMOUNT )
					.mode(DEFAULT_SAVEMODE)
					.save();
		LogStack.process.info("ProcessEvent :  Amount End");
	}

	/**
	 * @param platformCode
	 * @param sparkSession
	 */
	private void productsEvent(SparkSession sparkSession){
		LogStack.process.info("ProcessEvent :  Products Start");
		Dataset<Row> resultDS = sparkSession.sql("SELECT	 '"+getProcessDate()+"'	AS 	processDate "
												+ "			,B.eventType "
												+ "			,A.pixelId "
												+ "			,A.catalogId "
												+ "			,A.contentCategory "
												+ "			,SUBSTRING_INDEX(A.contentCategory, '~', 1)	                	   	 	AS 	contentCategoryLv1 "
												+ "			,SUBSTRING_INDEX(SUBSTRING_INDEX(A.contentCategory, '~', 2), '~', -1) 	AS 	contentCategoryLv2 "
												+ "			,SUBSTRING_INDEX(SUBSTRING_INDEX(A.contentCategory, '~', 3), '~', -1) 	AS 	contentCategoryLv3 "
												+ "			,A.contentIds "
												+ "			,A.contentName "
												+ "			,A.currency "
												+ "			,CAST(SUM(B.value) AS BIGINT)			AS	totalValue "
												+ "			,COUNT(B.catalogId)						AS	totalCount "
												+ "			,RANK() OVER (PARTITION BY B.eventType ORDER BY SUM(B.value) DESC)			AS valueRank "
												+ "			,RANK() OVER (PARTITION BY B.eventType ORDER BY COUNT(A.contentIds) DESC)	AS countRank "
												+ "			,RANK() OVER (ORDER BY COUNT(A.contentIds) DESC)							AS countRank "
												+ "			,COUNT(CASE WHEN B.hour = 00 THEN  	A.contentIds END)	AS countHour00"
												+ "			,COUNT(CASE WHEN B.hour = 01 THEN  	A.contentIds END)	AS countHour01"
												+ "			,COUNT(CASE WHEN B.hour = 02 THEN  	A.contentIds END)	AS countHour02"
												+ "			,COUNT(CASE WHEN B.hour = 03 THEN  	A.contentIds END)	AS countHour03"
												+ "			,COUNT(CASE WHEN B.hour = 04 THEN  	A.contentIds END)	AS countHour04"
												+ "			,COUNT(CASE WHEN B.hour = 05 THEN  	A.contentIds END)	AS countHour05"
												+ "			,COUNT(CASE WHEN B.hour = 06 THEN  	A.contentIds END)	AS countHour06"
												+ "			,COUNT(CASE WHEN B.hour = 07 THEN  	A.contentIds END)	AS countHour07"
												+ "			,COUNT(CASE WHEN B.hour = 08 THEN  	A.contentIds END)	AS countHour08"
												+ "			,COUNT(CASE WHEN B.hour = 09 THEN  	A.contentIds END)	AS countHour09"
												+ "			,COUNT(CASE WHEN B.hour = 10 THEN	A.contentIds END)	AS countHour10"
												+ "			,COUNT(CASE WHEN B.hour = 11 THEN	A.contentIds END)	AS countHour11"
												+ "			,COUNT(CASE WHEN B.hour = 12 THEN	A.contentIds END)	AS countHour12"
												+ "			,COUNT(CASE WHEN B.hour = 13 THEN	A.contentIds END)	AS countHour13"
												+ "			,COUNT(CASE WHEN B.hour = 14 THEN	A.contentIds END)	AS countHour14"
												+ "			,COUNT(CASE WHEN B.hour = 15 THEN	A.contentIds END)	AS countHour15"
												+ "			,COUNT(CASE WHEN B.hour = 16 THEN	A.contentIds END)	AS countHour16"
												+ "			,COUNT(CASE WHEN B.hour = 17 THEN	A.contentIds END)	AS countHour17"
												+ "			,COUNT(CASE WHEN B.hour = 18 THEN	A.contentIds END)	AS countHour18"
												+ "			,COUNT(CASE WHEN B.hour = 19 THEN	A.contentIds END)	AS countHour19"
												+ "			,COUNT(CASE WHEN B.hour = 20 THEN	A.contentIds END)	AS countHour20"
												+ "			,COUNT(CASE WHEN B.hour = 21 THEN	A.contentIds END)	AS countHour21"
												+ "			,COUNT(CASE WHEN B.hour = 22 THEN	A.contentIds END)	AS countHour22"
												+ "			,COUNT(CASE WHEN B.hour = 23 THEN	A.contentIds END)	AS countHour23"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 00 THEN  B.value END), 0) AS BIGINT)	AS valueHour00"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 01 THEN  B.value END), 0) AS BIGINT)	AS valueHour01"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 02 THEN  B.value END), 0) AS BIGINT)	AS valueHour02"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 03 THEN  B.value END), 0) AS BIGINT)	AS valueHour03"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 04 THEN  B.value END), 0) AS BIGINT)	AS valueHour04"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 05 THEN  B.value END), 0) AS BIGINT)	AS valueHour05"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 06 THEN  B.value END), 0) AS BIGINT)	AS valueHour06"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 07 THEN  B.value END), 0) AS BIGINT)	AS valueHour07"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 08 THEN  B.value END), 0) AS BIGINT)	AS valueHour08"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 09 THEN  B.value END), 0) AS BIGINT)	AS valueHour09"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 10 THEN  B.value END), 0) AS BIGINT)	AS valueHour10"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 11 THEN  B.value END), 0) AS BIGINT)	AS valueHour11"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 12 THEN  B.value END), 0) AS BIGINT)	AS valueHour12"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 13 THEN  B.value END), 0) AS BIGINT)	AS valueHour13"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 14 THEN  B.value END), 0) AS BIGINT)	AS valueHour14"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 15 THEN  B.value END), 0) AS BIGINT)	AS valueHour15"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 16 THEN  B.value END), 0) AS BIGINT)	AS valueHour16"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 17 THEN  B.value END), 0) AS BIGINT)	AS valueHour17"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 18 THEN  B.value END), 0) AS BIGINT)	AS valueHour18"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 19 THEN  B.value END), 0) AS BIGINT)	AS valueHour19"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 20 THEN  B.value END), 0) AS BIGINT)	AS valueHour20"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 21 THEN  B.value END), 0) AS BIGINT)	AS valueHour21"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 22 THEN  B.value END), 0) AS BIGINT)	AS valueHour22"
												+ "			,CAST(NVL(SUM(CASE WHEN B.hour = 23 THEN  B.value END), 0) AS BIGINT)	AS valueHour23"
												
												+ " FROM	T_PIXEL_FEED A INNER JOIN  "
												+ "			(SELECT	 eventType"
												+ "					,pixelId"
												+ "					,catalogId"
												+ "					,contentIds"
												+ "					,value"
												+ "					,logDateTime"
												+ "					,hour"
												+ "			FROM	V_PIXEL_VIEW_CONTENT"
												+ "	 "
												+ "			UNION ALL "
												+ "	 "
												+ "			SELECT	 eventType"
												+ "					,pixelId"
												+ "					,catalogId"
												+ "					,contentIds"
												+ "					,value"
												+ "					,logDateTime"
												+ "					,hour"
												+ "			FROM	V_PIXEL_ADD_TO_CART"
												+ "	 "
												+ "			UNION ALL "
												+ "	 "
												+ "			SELECT	 'PURCHASED'				AS 	eventType"
												+ "					,pixelId"
												+ "					,catalogId"
												+ "					,contentIds"
												+ "					,value"
												+ "					,logDateTime"
												+ "					,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logDateTime, 'yyyy-mm-dd HH') AS TIMESTAMP), 'HH')	AS hour"
												+ "			FROM	T_PIXEL_PURCHASED	 "
												+ "			) B "
												+ "			ON	A.pixelId		= B.pixelId"
												+ "			AND	A.catalogId		= B.catalogId"
												+ "			AND	A.contentIds	= B.contentIds"
												+ " GROUP BY B.eventType, A.pixelId, A.catalogId, A.contentCategory, A.contentIds, A.contentName, A.currency");
		
		MongodbFactory.dropAndCreateAndShardingCollection(getProcessedDataBase(), getProcessDate() + MongodbMap.COLLECTION_PIXEL_EVENT_PRODUCTS );
		MongoSpark.write(resultDS).option("database", getProcessedDataBase())
				.option("collection",getProcessDate() + MongodbMap.COLLECTION_PIXEL_EVENT_PRODUCTS )
				.mode(DEFAULT_SAVEMODE)
				.save();
		LogStack.process.info("ProcessEvent :  Products End");
	}
}
