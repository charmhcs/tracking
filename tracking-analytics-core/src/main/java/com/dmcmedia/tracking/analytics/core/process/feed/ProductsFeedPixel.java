package com.dmcmedia.tracking.analytics.core.process.feed;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import com.dmcmedia.tracking.analytics.common.AbstractDataProcess;
import com.dmcmedia.tracking.analytics.common.factory.MongodbFactory;
import com.dmcmedia.tracking.analytics.common.map.CodeMap;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.map.MongodbMap;
import com.dmcmedia.tracking.analytics.common.util.DateUtil;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.config.RawDataSQLTempView;
import com.dmcmedia.tracking.analytics.core.dataset.TrackingPixelDataSet;
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
* 2017. 5. 10.   cshwang   Initial Release
*
*
************************************************************************************************/
public final class ProductsFeedPixel extends AbstractDataProcess{	
			
	/**
	 * 생성자
	 */
	public ProductsFeedPixel(){
		super();
	}
	
	/**
	 * 생성자
	 * @param processType
	 */
	public ProductsFeedPixel(String runType){
		super(runType);
	}
	
	/**
	 * 생성자
	 * 
	 * @param runType
	 * @param processType
	 */
	public ProductsFeedPixel(String runType,  String processType){
		super(runType, processType);
	}
	
	/* (non-Javadoc)
	 * @see com.dmcmedia.tracking.analysis.common.AbstractSparkSessionAnalysis#analysisDataSet(org.apache.spark.sql.SparkSession)
	 */
	public boolean processDataSet(SparkSession sparkSession) {
		JavaSparkContext jsc 	= null;
		
		if(super.isValid() == false){
			return false;
		}
		
		try{			
			jsc = new JavaSparkContext(sparkSession.sparkContext());
			sparkSession.udf().register("URL_DECODE", urldecode, DataTypes.StringType);
			
			if(EACH.equals(this.getRunType().name())){
		    	RawDataSQLTempView rawDataSQLTempView = null;
				if(HOURLY.equals(getProcessType().name())) {
					rawDataSQLTempView = new RawDataSQLTempView(getProcessDateHour());
					rawDataSQLTempView.createPixelRawDataHourlyTempView(sparkSession);
				}else {
					rawDataSQLTempView = new RawDataSQLTempView(getProcessDate());
					rawDataSQLTempView.createPixelRawDataDailyTempView(sparkSession);
				}
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
			}
		
			switch(getProcessType().name()) {
				case HOURLY :
					createHourlyFeed(sparkSession, jsc);
					break;
				
				case DAILY :
					createDailyFeed(sparkSession, jsc);
					break;
				
				case DAYS :
				    this.createDailyFeed(sparkSession, jsc, this.getDays());
				    this.createDaysFeed(sparkSession, jsc);
					break;
					
				case ALL :
					this.createHourlyFeed(sparkSession, jsc);
					this.createDailyFeed(sparkSession, jsc);
				    this.createDaysFeed(sparkSession, jsc);
					break;
					
				default :
					new Exception(ExceptionMap.INVALID_DATA_FORMAT_EXCEPTION);
			}
		}catch(Exception e){
			LogStack.process.error(ExceptionMap.PROCESS_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.process.error(e);
		}
		return true;
	}
	
	
	
	/**
	 * 일 기준 시간별 Fee를 생성함 
	 * 
	 * @param sparkSession
	 * @param jsc
	 */
	private void createHourlyFeed(SparkSession sparkSession, JavaSparkContext jsc) {
		String collectionName 	= null;
		Dataset<Row> resultDs 	= null;
		Dataset<TrackingPixelDataSet> pixelCatalogDS = null;
		
		pixelCatalogDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession)
				.withOption("database", getRawDataBase())
				.withOption("collection",  this.getProcessDate() + MongodbMap.COLLECTION_PIXEL_CATALOG)).toDS(TrackingPixelDataSet.class);
		pixelCatalogDS.createOrReplaceTempView("T_PIXEL_CATALOG");
		
		collectionName = getProcessDate() + MongodbMap.COLLECTION_PIXEL_FEED_HOURLY;
		MongodbFactory.dropAndCreateAndShardingCollection(getProcessedDataBase(), collectionName);
		MongodbFactory.createCollectionIndex(getProcessedDataBase(), collectionName, MongodbMap.FIELD_CATALOG_ID);
		resultDs = sparkSession.sql(getHourlyQuery());
	    MongoSpark.write(resultDs).option("database", getProcessedDataBase())
									.option("collection", collectionName)
									.mode(DEFAULT_SAVEMODE)
									.save();
	}
	
	/**
	 * @param sparkSession
	 * @param jsc
	 */
	private void createDailyFeed(SparkSession sparkSession, JavaSparkContext jsc) {
		this.createDailyFeed(sparkSession, jsc, CodeMap.ONE);
	}
	
	/**
	 * 일 기준 Feed를 생성
	 * @param sparkSession
	 * @param jsc
	 */
	private void createDailyFeed(SparkSession sparkSession, JavaSparkContext jsc, int days ) {
		
		Dataset<Row> resultDs 	= null;
		Dataset<TrackingPixelDataSet> pixelCatalogDS = null;
		
		for (int currentDays = 1 ; currentDays <= days; currentDays++ ) {
			
			String catalogDate = DateUtil.getChangeDateFormatIntervalDay(this.getProcessDate(), CodeMap.DATE_YMD_PATTERN_FOR_ANYS, CodeMap.DATE_YMD_PATTERN_FOR_ANYS, -(currentDays - 1));
			String catalogDateMonth = DateUtil.getChangeDateFormatIntervalDay(this.getProcessDate(), CodeMap.DATE_YMD_PATTERN_FOR_ANYS, CodeMap.DATE_MONTH_PATTERN, -(currentDays - 1));
			
			pixelCatalogDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession)
					.withOption("database", MongodbMap.DATABASE_RAW_MONTH + catalogDateMonth)
					.withOption("collection",  catalogDate + MongodbMap.COLLECTION_PIXEL_CATALOG)).toDS(TrackingPixelDataSet.class);
			pixelCatalogDS.createOrReplaceTempView("T_PIXEL_CATALOG");
			
			MongodbFactory.dropAndCreateAndShardingCollection(MongodbMap.DATABASE_PROCESSED_MONTH + catalogDateMonth, catalogDate + MongodbMap.COLLECTION_PIXEL_FEED_DAILY);
			MongodbFactory.createCollectionIndex(MongodbMap.DATABASE_PROCESSED_MONTH + catalogDateMonth, catalogDate + MongodbMap.COLLECTION_PIXEL_FEED_DAILY, MongodbMap.FIELD_CATALOG_ID);
			resultDs = sparkSession.sql(getDailyQuery());
			MongoSpark.write(resultDs).option("database", MongodbMap.DATABASE_PROCESSED_MONTH + catalogDateMonth)
						.option("collection", catalogDate + MongodbMap.COLLECTION_PIXEL_FEED_DAILY)
						.mode(DEFAULT_SAVEMODE)
						.save();
		}
	}
	
	/**
	 * 일별 누적 Feed를 생성함 
	 * 
	 * @param sparkSession
	 * @param jsc
	 * @param catalogViewSql
	 */
	private void createDaysFeed(SparkSession sparkSession, JavaSparkContext jsc) {
		Dataset<ProductsFeed> feedDS = null;
		Dataset<ProductsFeed> rFeedDS = null;
		

		for (int currentDays = 1 ; currentDays <= this.getDays(); currentDays++ ) {
			String catalogDate = DateUtil.getChangeDateFormatIntervalDay(this.getProcessDate(), CodeMap.DATE_YMD_PATTERN_FOR_ANYS, CodeMap.DATE_YMD_PATTERN_FOR_ANYS, -(currentDays - 1));
			String catalogDateMonth = DateUtil.getChangeDateFormatIntervalDay(this.getProcessDate(), CodeMap.DATE_YMD_PATTERN_FOR_ANYS, CodeMap.DATE_MONTH_PATTERN, -(currentDays - 1));
			LogStack.process.debug("currentDays :" + currentDays);
			LogStack.process.debug(catalogDate);
			LogStack.process.debug(catalogDateMonth);
			if(feedDS == null) {
				feedDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession)
						.withOption("database", MongodbMap.DATABASE_PROCESSED_MONTH + catalogDateMonth)
						.withOption("collection",  catalogDate + MongodbMap.COLLECTION_PIXEL_FEED_DAILY)).toDS(ProductsFeed.class);
			}else {
				rFeedDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession)
						.withOption("database", MongodbMap.DATABASE_PROCESSED_MONTH + catalogDateMonth)
						.withOption("collection",  catalogDate + MongodbMap.COLLECTION_PIXEL_FEED_DAILY)).toDS(ProductsFeed.class);
				LogStack.process.debug(" rFeedDS.count() :" +  rFeedDS.count());
				
				if(rFeedDS != null && rFeedDS.count() > 0) {
					feedDS = feedDS.union(rFeedDS);
				}
			}
			
		    if( currentDays == AbstractDataProcess.DAYS_3 || currentDays == AbstractDataProcess.DAYS_7 || currentDays == AbstractDataProcess.DAYS_14 || currentDays == AbstractDataProcess.DAYS_28) {
	
		    	feedDS.createOrReplaceTempView("V_PIXEL_FEED");
		    	Dataset<Row> resultDs = sparkSession.sql("SELECT  	 T.pixelId"
		    											+ "			,T.catalogId"
		 												+ "			,T.contentName"
														+ "			,T.contentIds"										
														+ "			,T.contentCategory"
														+ "			,T.contentType"
														+ "			,T.productType"
														+ "			,T.brand"
														+ "			,T.link"
														+ "			,T.imageLink"										
														+ "			,T.description"
														+ "			,T.availability"
														+ "			,T.currency"
														+ "			,T.value"
														+ "			,T.logDateTime"	
														+ "			,T.processDate"				
														+ " FROM 	(SELECT  A.* "
														+ " 				,RANK() OVER (PARTITION BY A.contentIds ORDER BY A._id DESC) AS PT "
														+ " 		 FROM 	V_PIXEL_FEED A ) T"
														+ " WHERE T.PT = 1");
		    	
		    	MongodbFactory.dropAndCreateAndShardingCollection(MongodbMap.DATABASE_PROCESSED_FEED, getProcessDate() + MongodbMap.COLLECTION_PIXEL_FEED + MongodbMap.COLLECTION_DOT + currentDays + MongodbMap.COLLECTION_DAYS);
		    	MongodbFactory.createCollectionIndex(MongodbMap.DATABASE_PROCESSED_FEED, getProcessDate() + MongodbMap.COLLECTION_PIXEL_FEED + MongodbMap.COLLECTION_DOT + currentDays + MongodbMap.COLLECTION_DAYS, MongodbMap.FIELD_CATALOG_ID);
		    	
		 		MongoSpark.write(resultDs).option("database", MongodbMap.DATABASE_PROCESSED_FEED)
		 				.option("collection",getProcessDate() + MongodbMap.COLLECTION_PIXEL_FEED + MongodbMap.COLLECTION_DOT + currentDays + MongodbMap.COLLECTION_DAYS)
		 				.mode(DEFAULT_SAVEMODE)
		 				.save();
		 		
		 		if(BATCHJOB.equals(this.getRunType().name())){
					MongodbFactory.dropCollection(MongodbMap.DATABASE_PROCESSED_FEED, getPrevious7DaysProcessedDate() + MongodbMap.COLLECTION_PIXEL_FEED + MongodbMap.COLLECTION_DOT + currentDays + MongodbMap.COLLECTION_DAYS);
				}
		    }			
		}
	}
	
	
	/**
	 * @param sqlHourString
	 * @param sqlRankString
	 * @return
	 */
	private String getHourlyQuery() {
		 return  new StringBuffer("WITH V_PIXEL_EVENT_USER_HOURLY AS ( "
						 		+ "    SELECT   DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')      AS processDate    "
						 		+ "            ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS processHour    "
						 		+ "            ,FLOOR(DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'mm')  / 30) AS processHalfHour    "
						 		+ "            ,pixelId "
						 		+ "            ,catalogId "
						 		+ "            ,pCid "
						 		+ "            ,trackingEventCode "
						 		+ "            ,deviceTypeCode "
						 		+ "            ,ids "
						 		+ "            ,referrer "
						 		+ "            ,contentIds "
						 		+ "            ,value "
						 		+ "            ,isoDate "
						 		+ "            ,logDateTime "
						 		+ "            ,NVL(LEAD(isoDate) OVER(PARTITION BY pixelId, catalogId, pCid, deviceTypeCode ORDER BY pixelId ASC, catalogId ASC, pCid ASC, isoDate ASC) "
						 		+ "                    , CAST(CONCAT(DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd') "
						 		+ "                        ,' ', '23:59:59.999') AS TIMESTAMP))  AS  nextIsoDate "
						 		+ "        FROM (SELECT   trackingEventCode "
						 		+ "                    ,viewContent.pixel_id       AS pixelId "
						 		+ "                    ,viewContent.catalog_id     AS catalogId    "
						 		+ "                    ,viewContent.p_cid		    AS pCid    "
						 		+ "                    ,(CASE WHEN viewContent.user_agent LIKE '%iPhone%' OR viewContent.user_agent LIKE '%Android%' OR viewContent.user_agent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode "
						 		+ "                    ,_id                        AS ids "
						 		+ "                    ,viewContent.referrer       AS referrer "
						 		+ "                    ,viewContent.content_ids    AS contentIds "
						 		+ "                    ,viewContent.value          AS value "
						 		+ "                    ,logDateTime "
						 		+ "                    ,isoDate "
						 		+ "            FROM	T_PIXEL_VIEW_CONTENT "
						 		+ "            UNION ALL  "
						 		+ "            SELECT   trackingEventCode "
						 		+ "                    ,addToCart.pixel_id         AS pixelId "
						 		+ "                    ,addToCart.catalog_id       AS catalogId    "
						 		+ "                    ,addToCart.p_cid            AS pCid    "
						 		+ "                    ,(CASE WHEN addToCart.user_agent LIKE '%iPhone%' OR addToCart.user_agent LIKE '%Android%' OR addToCart.user_agent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode "
						 		+ "                    ,_id                        AS ids "
						 		+ "                    ,addToCart.referrer         AS referrer "
						 		+ "                    ,addToCart.content_ids      AS contentIds "
						 		+ "                    ,addToCart.value            AS value "
						 		+ "                    ,logDateTime "
						 		+ "                    ,isoDate "
						 		+ "            FROM	T_PIXEL_ADD_TO_CART "
						 		+ "            UNION ALL  "
						 		+ "            SELECT	 'TPD004' "
						 		+ "                    ,pixelId "
						 		+ "                    ,catalogId "
						 		+ "                    ,pCid "
						 		+ "                    ,(CASE WHEN userAgent LIKE '%iPhone%' OR userAgent LIKE '%Android%' OR userAgent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode "
						 		+ "                    ,_id "
						 		+ "                    ,'' "
						 		+ "                    ,contentIds "
						 		+ "                    ,value "
						 		+ "                    ,logDateTime "
						 		+ "                    ,isoDate "
						 		+ "            FROM	T_PIXEL_PURCHASED  "
						 		+ "    ) "
						 		+ ") "
						 		+ " "
						 		+ "SELECT    T.processDate "
						 		+ " 		,T.hour "
						 		+ "        	,T.pixelId "
						 		+ "			,T.catalogId "
						 		+ "			,T.contentIds "
						 		+ "			,URL_DECODE(URL_DECODE(T.contentCategory))	AS contentCategory "
						 		+ "			,T.contentType "
						 		+ "			,URL_DECODE(URL_DECODE(T.productType))		AS productType "
						 		+ "			,URL_DECODE(URL_DECODE(T.contentName))		AS contentName "
						 		+ "			,URL_DECODE(URL_DECODE(T.brand))			AS brand "
						 		+ "			,T.link "
						 		+ "			,T.imageLink "
						 		+ "			,URL_DECODE(URL_DECODE(T.description))		AS description "
						 		+ "			,T.availability "
						 		+ "			,T.currency "
						 		+ "			,T.value "
						 		+ "			,T.viewContentRank "
						 		+ "			,T.addToCartRank "
						 		+ "			,T.purchasedRank "
						 		+ "			,NVL(T.viewContent, 0) AS viewContent "
						 		+ "			,NVL(T.addToCart, 0) AS addToCart "
						 		+ "			,NVL(T.purchased, 0) AS purchased "
						 		+ "			,T.logDateTime "
						 		+ " FROM 	(SELECT  D.*  "
						 		+ " 				,RANK() OVER ( PARTITION BY D.hour, D.pixelId, D.catalogId, D.contentIds ORDER BY D.objectId DESC) AS PT "
						 		+ " 				,RANK() OVER ( PARTITION BY D.hour, D.pixelId, D.catalogId  ORDER BY NVL(D.viewContent, 0) DESC) AS viewContentRank "
						 		+ " 				,RANK() OVER ( PARTITION BY D.hour, D.pixelId, D.catalogId  ORDER BY NVL(D.addToCart, 0) DESC) AS addToCartRank "
						 		+ " 				,RANK() OVER ( PARTITION BY D.hour, D.pixelId, D.catalogId  ORDER BY NVL(D.purchased, 0) DESC) AS purchasedRank "
						 		+ " 		 FROM 	(SELECT  DATE_FORMAT(CAST(UNIX_TIMESTAMP(logDateTime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')	AS hour		 "
						 		+ "							,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd') AS processDate "
						 		+ " 		                ,B.pixelId			        AS pixelId "
						 		+ "                         ,A.catalog.catalog_id		AS catalogId "
						 		+ "                         ,A.catalog.content_name		AS contentName "
						 		+ "							,A.catalog.content_ids		AS contentIds "
						 		+ "							,A.catalog.content_category	AS contentCategory "
						 		+ "							,A.catalog.content_type		AS contentType "
						 		+ "							,A.catalog.product_type		AS productType "
						 		+ "							,A.catalog.link				AS link "
						 		+ "							,A.catalog.image_link		AS imageLink "
						 		+ "							,A.catalog.description		AS description "
						 		+ "							,A.catalog.availability		AS availability "
						 		+ "							,A.catalog.brand			AS brand "
						 		+ "							,A.catalog.value			AS value "
						 		+ "							,A.catalog.currency			AS currency "
						 		+ "							,A.logDateTime				AS logDateTime "
						 		+ "					        ,C.viewContent "
						 		+ "					        ,C.addToCart "
						 		+ "					        ,C.purchased "
						 		+ "							,_id						AS objectId "
						 		+ " 				FROM 	T_PIXEL_CATALOG A  "
						 		+ " 				        INNER JOIN (SELECT * FROM T_TRK_PIXEL WHERE TRIM(pixelId) NOT IN('undefined', '<#VARIABLE#>', '') AND TRIM(catalogId) NOT IN('undefined', '<#VARIABLE#>', '')) B   "
						 		+ " 				        ON A.catalog.catalog_id = B.catalogId "
						 		+ " 				        LEFT OUTER JOIN (SELECT   pixelId "
						 		+ "                                            ,catalogId "
						 		+ "                                            ,contentIds "
						 		+ "                                            ,SUM(CASE WHEN trackingEventCode = 'TPD002' THEN 1 ELSE 0 END) AS viewContent "
						 		+ "                                            ,SUM(CASE WHEN trackingEventCode = 'TPD003' THEN 1 ELSE 0 END) AS addToCart "
						 		+ "                                            ,SUM(CASE WHEN trackingEventCode = 'TPD004' THEN 1 ELSE 0 END) AS purchased "
						 		+ "                                    FROM    V_PIXEL_EVENT_USER_HOURLY     "
						 		+ "                                    GROUP BY pixelId, catalogId, contentIds) C "
						 		+ "                        ON A.catalog.catalog_id = C.catalogId AND A.catalog.content_ids = C.contentIds "
						 		+ " 				) D "
						 		+ " 		) T "
						 		+ " 		 "
						 		+ " WHERE T.PT = 1 "
								).toString();	

	}
	
	/**
	 * @param sqlHourString
	 * @param sqlRankString
	 * @return
	 */
	private String getDailyQuery() {
		 return  new StringBuffer("WITH V_PIXEL_EVENT_USER_HOURLY AS ( "
						 		+ "    SELECT   DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')      AS processDate    "
						 		+ "            ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS processHour    "
						 		+ "            ,FLOOR(DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'mm')  / 30) AS processHalfHour    "
						 		+ "            ,pixelId "
						 		+ "            ,catalogId "
						 		+ "            ,pCid "
						 		+ "            ,trackingEventCode "
						 		+ "            ,deviceTypeCode "
						 		+ "            ,ids "
						 		+ "            ,referrer "
						 		+ "            ,contentIds "
						 		+ "            ,value "
						 		+ "            ,isoDate "
						 		+ "            ,logDateTime "
						 		+ "            ,NVL(LEAD(isoDate) OVER(PARTITION BY pixelId, catalogId, pCid, deviceTypeCode ORDER BY pixelId ASC, catalogId ASC, pCid ASC, isoDate ASC) "
						 		+ "                    , CAST(CONCAT(DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd') "
						 		+ "                        ,' ', '23:59:59.999') AS TIMESTAMP))  AS  nextIsoDate "
						 		+ "        FROM (SELECT   trackingEventCode "
						 		+ "                    ,viewContent.pixel_id       AS pixelId "
						 		+ "                    ,viewContent.catalog_id     AS catalogId    "
						 		+ "                    ,viewContent.p_cid		    AS pCid    "
						 		+ "                    ,(CASE WHEN viewContent.user_agent LIKE '%iPhone%' OR viewContent.user_agent LIKE '%Android%' OR viewContent.user_agent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode "
						 		+ "                    ,_id                        AS ids "
						 		+ "                    ,viewContent.referrer       AS referrer "
						 		+ "                    ,viewContent.content_ids    AS contentIds "
						 		+ "                    ,viewContent.value          AS value "
						 		+ "                    ,logDateTime "
						 		+ "                    ,isoDate "
						 		+ "            FROM	T_PIXEL_VIEW_CONTENT "
						 		+ "            UNION ALL  "
						 		+ "            SELECT   trackingEventCode "
						 		+ "                    ,addToCart.pixel_id         AS pixelId "
						 		+ "                    ,addToCart.catalog_id       AS catalogId    "
						 		+ "                    ,addToCart.p_cid            AS pCid    "
						 		+ "                    ,(CASE WHEN addToCart.user_agent LIKE '%iPhone%' OR addToCart.user_agent LIKE '%Android%' OR addToCart.user_agent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode "
						 		+ "                    ,_id                        AS ids "
						 		+ "                    ,addToCart.referrer         AS referrer "
						 		+ "                    ,addToCart.content_ids      AS contentIds "
						 		+ "                    ,addToCart.value            AS value "
						 		+ "                    ,logDateTime "
						 		+ "                    ,isoDate "
						 		+ "            FROM	T_PIXEL_ADD_TO_CART "
						 		+ "            UNION ALL  "
						 		+ "            SELECT	 'TPD004' "
						 		+ "                    ,pixelId "
						 		+ "                    ,catalogId "
						 		+ "                    ,pCid "
						 		+ "                    ,(CASE WHEN userAgent LIKE '%iPhone%' OR userAgent LIKE '%Android%' OR userAgent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode "
						 		+ "                    ,_id "
						 		+ "                    ,'' "
						 		+ "                    ,contentIds "
						 		+ "                    ,value "
						 		+ "                    ,logDateTime "
						 		+ "                    ,isoDate "
						 		+ "            FROM	T_PIXEL_PURCHASED  "
						 		+ "    ) "
						 		+ ") "
						 		+ " "
						 		+ "SELECT    T.processDate "
						 		+ "        	,T.pixelId "
						 		+ "			,T.catalogId "
						 		+ "			,T.contentIds "
						 		+ "			,URL_DECODE(URL_DECODE(T.contentCategory))	AS contentCategory "
						 		+ "			,T.contentType "
						 		+ "			,URL_DECODE(URL_DECODE(T.productType))		AS productType "
						 		+ "			,URL_DECODE(URL_DECODE(T.contentName))		AS contentName "
						 		+ "			,URL_DECODE(URL_DECODE(T.brand))			AS brand "
						 		+ "			,T.link "
						 		+ "			,T.imageLink "
						 		+ "			,URL_DECODE(URL_DECODE(T.description))		AS description "
						 		+ "			,T.availability "
						 		+ "			,T.currency "
						 		+ "			,T.value "
						 		+ "			,T.viewContentRank "
						 		+ "			,T.addToCartRank "
						 		+ "			,T.purchasedRank "
						 		+ "			,NVL(T.viewContent, 0) AS viewContent "
						 		+ "			,NVL(T.addToCart, 0) AS addToCart "
						 		+ "			,NVL(T.purchased, 0) AS purchased "
						 		+ "			,T.logDateTime "
						 		+ " FROM 	(SELECT  D.*  "
						 		+ " 				,RANK() OVER ( PARTITION BY D.pixelId, D.catalogId, D.contentIds ORDER BY D.objectId DESC) AS PT "
						 		+ " 				,RANK() OVER ( PARTITION BY D.pixelId, D.catalogId  ORDER BY NVL(D.viewContent, 0) DESC) AS viewContentRank "
						 		+ " 				,RANK() OVER ( PARTITION BY D.pixelId, D.catalogId  ORDER BY NVL(D.addToCart, 0) DESC) AS addToCartRank "
						 		+ " 				,RANK() OVER ( PARTITION BY D.pixelId, D.catalogId  ORDER BY NVL(D.purchased, 0) DESC) AS purchasedRank "
						 		+ " 		 FROM 	(SELECT  DATE_FORMAT(CAST(UNIX_TIMESTAMP(logDateTime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')	AS hour		 "
						 		+ "							,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd') AS processDate "
						 		+ " 		                ,B.pixelId			        AS pixelId "
						 		+ "                         ,A.catalog.catalog_id		AS catalogId "
						 		+ "                         ,A.catalog.content_name		AS contentName "
						 		+ "							,A.catalog.content_ids		AS contentIds "
						 		+ "							,A.catalog.content_category	AS contentCategory "
						 		+ "							,A.catalog.content_type		AS contentType "
						 		+ "							,A.catalog.product_type		AS productType "
						 		+ "							,A.catalog.link				AS link "
						 		+ "							,A.catalog.image_link		AS imageLink "
						 		+ "							,A.catalog.description		AS description "
						 		+ "							,A.catalog.availability		AS availability "
						 		+ "							,A.catalog.brand			AS brand "
						 		+ "							,A.catalog.value			AS value "
						 		+ "							,A.catalog.currency			AS currency "
						 		+ "							,A.logDateTime				AS logDateTime "
						 		+ "					        ,C.viewContent "
						 		+ "					        ,C.addToCart "
						 		+ "					        ,C.purchased "
						 		+ "							,_id						AS objectId "
						 		+ " 				FROM 	T_PIXEL_CATALOG A  "
						 		+ " 				        INNER JOIN (SELECT * FROM T_TRK_PIXEL WHERE TRIM(pixelId) NOT IN('undefined', '<#VARIABLE#>', '') AND TRIM(catalogId) NOT IN('undefined', '<#VARIABLE#>', '')) B   "
						 		+ " 				        ON A.catalog.catalog_id = B.catalogId "
						 		+ " 				        LEFT OUTER JOIN (SELECT   pixelId "
						 		+ "                                            ,catalogId "
						 		+ "                                            ,contentIds "
						 		+ "                                            ,SUM(CASE WHEN trackingEventCode = 'TPD002' THEN 1 ELSE 0 END) AS viewContent "
						 		+ "                                            ,SUM(CASE WHEN trackingEventCode = 'TPD003' THEN 1 ELSE 0 END) AS addToCart "
						 		+ "                                            ,SUM(CASE WHEN trackingEventCode = 'TPD004' THEN 1 ELSE 0 END) AS purchased "
						 		+ "                                    FROM    V_PIXEL_EVENT_USER_HOURLY     "
						 		+ "                                    GROUP BY pixelId, catalogId, contentIds) C "
						 		+ "                        ON A.catalog.catalog_id = C.catalogId AND A.catalog.content_ids = C.contentIds "
						 		+ " 				) D "
						 		+ " 		) T "
						 		+ " 		 "
						 		+ " WHERE T.PT = 1 "
								).toString();	

	}
}