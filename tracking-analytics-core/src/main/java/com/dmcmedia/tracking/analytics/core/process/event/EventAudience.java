package com.dmcmedia.tracking.analytics.core.process.event;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.AbstractDataProcess;
import com.dmcmedia.tracking.analytics.common.factory.MongodbFactory;
import com.dmcmedia.tracking.analytics.common.map.CodeMap;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.map.MongodbMap;
import com.dmcmedia.tracking.analytics.common.util.DateUtil;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.config.RawDataSQLTempView;
import com.dmcmedia.tracking.analytics.core.dataset.event.Audience;
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
* 2017. 10. 9.   cshwang   Initial Release
*
* 오디언스 데이터 생성 (시간 / 일)
*
************************************************************************************************/
public class EventAudience extends AbstractDataProcess{
	/**
	 * 생성자
	 */
	public EventAudience(){
		super();
	}
	
	/**
	 * 생성자
	 * 
	 * @param processType
	 */
	public EventAudience(String runType){
		super(runType);
	}
	
	/**
	 * 생성자
	 * 
	 * @param processType
	 * @param runType
	 */
	public EventAudience(String runType,  String processType){
		super(runType, processType);
	}

	/* (non-Javadoc)
	 * @see com.dmcmedia.tracking.analysis.common.AbstractSparkSessionAnalysis#analysisDataSet(org.apache.spark.sql.SparkSession)
	 */
	public boolean processDataSet(SparkSession sparkSession){

		JavaSparkContext jsc = null;
		Dataset<ProductsFeed> pixelProdictsFeedDS = null;
		Dataset<ProductsFeed> pixelProdictsFeedHourlyDS = null;

		
		if(super.isValid() == false){
			return false;
		}
		
		try{
			jsc = new JavaSparkContext(sparkSession.sparkContext());
			
			if(DAYS.equals(getProcessType().name())){
				this.createDaysAudience(sparkSession, jsc, this.getDays());
			}else{
				if(EACH.equals(this.getRunType().name())){
			    	RawDataSQLTempView rawDataSQLTempView = new RawDataSQLTempView(getProcessDate());
			    	rawDataSQLTempView.createPixelRawDataDailyTempView(sparkSession);
				}
				pixelProdictsFeedDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database", getProcessedDataBase()).withOption("collection", getProcessDate() + MongodbMap.COLLECTION_PIXEL_FEED_DAILY)).toDS(ProductsFeed.class);
				pixelProdictsFeedHourlyDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database", getProcessedDataBase()).withOption("collection", getProcessDate() + MongodbMap.COLLECTION_PIXEL_FEED_HOURLY)).toDS(ProductsFeed.class);
				pixelProdictsFeedDS.createOrReplaceTempView("T_PIXEL_FEED");
				pixelProdictsFeedHourlyDS.createOrReplaceTempView("T_PIXEL_FEED_HOURLY");

				switch(getProcessType().name()) {
					case HOURLY :
						this.createHourlyAudience(sparkSession);	
						break;
					case DAILY :
						this.createDailyAudience(sparkSession);
						break;						
					case DAYS :
					    this.createDaysAudience(sparkSession, jsc, this.getDays());
						break;						
					case ALL :
						this.createHourlyAudience(sparkSession);
						this.createDailyAudience(sparkSession);
						this.createDaysAudience(sparkSession, jsc, this.getDays());
						break;
						
					default :
						new Exception(ExceptionMap.INVALID_DATA_FORMAT_EXCEPTION);
				}
			}

		}catch(Exception e){
			LogStack.process.error(ExceptionMap.PROCESS_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.process.error(e);
		}
		return true;
	}
	
	/**
	 * 오디언스 데이터를 누적 생성한다. 
	 * 	1.daily 오디언스를 먼저 생성한다. 만약 생성되지 않았으면 생성처리를 진행한다. 
	 *  2.3일 7일 14일 28일 기준으로 uinon한 후 저장한다. 
	 * 
	 * @param sparkSession
	 * @param jsc
	 * @param catalogViewSQL
	 * @param addtocartViewSQL
	 * @param viewcontentViewSQL
	 */
	private void createDaysAudience(SparkSession sparkSession, JavaSparkContext jsc, int days) {

		Dataset<Audience> pixelAudienceDS = null;
		LogStack.process.debug("days :" + days);
		for (int currentDays = 1 ; currentDays <= days; currentDays++ ) {
			String catalogDate = DateUtil.getChangeDateFormatIntervalDay(getProcessDate(), CodeMap.DATE_YMD_PATTERN_FOR_ANYS, CodeMap.DATE_YMD_PATTERN_FOR_ANYS, -(currentDays - 1));
			String catalogDateMonth = DateUtil.getChangeDateFormatIntervalDay(getProcessDate(), CodeMap.DATE_YMD_PATTERN_FOR_ANYS, CodeMap.DATE_MONTH_PATTERN, -(currentDays - 1));
			String processedDataBase = MongodbMap.DATABASE_PROCESSED_MONTH + catalogDateMonth; 
			
			LogStack.process.debug("currentDays :" + currentDays);
			LogStack.process.debug(catalogDate);
			LogStack.process.debug(catalogDateMonth);
			
			if(currentDays == 1) {
				pixelAudienceDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database", processedDataBase).withOption("collection", catalogDate + MongodbMap.COLLECTION_PIXEL_AUDIENCE_DAILY)).toDS(Audience.class);
			}else {	

				Dataset<Audience> rPixelAudienceDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database", processedDataBase).withOption("collection", catalogDate + MongodbMap.COLLECTION_PIXEL_AUDIENCE_DAILY)).toDS(Audience.class);
				pixelAudienceDS = pixelAudienceDS.union(rPixelAudienceDS);
			}
			
		    if( currentDays == DAYS_3 || currentDays == DAYS_7 || currentDays == DAYS_14 || currentDays == DAYS_28) {
		    	pixelAudienceDS.createOrReplaceTempView("T_PIXEL_AUDIENCE");
		    	Dataset<Row> resultDS = sparkSession.sql( "	SELECT	 processDate "
		    											+ "			,pixelId"
		    											+ "			,catalogId"
														+ "			,pCid"
														+ "			,contentCategory"
														+ "			,contentIds"
														+ "		    ,price"
														+ "			,CAST(SUM(viewcontent) AS BIGINT) 		AS viewcontent"
														+ "			,CAST(SUM(addtocart) AS BIGINT)	 		AS addtocart"
														+ "			,CAST(SUM(purchased) AS BIGINT) 		AS purchased"
										    			+ " FROM T_PIXEL_AUDIENCE"
										    			+ " GROUP BY processDate, pixelId, catalogId, pCid, contentCategory, contentIds, price  "
										    			+ "");
		    	String collectionName = getProcessDate() + MongodbMap.COLLECTION_PIXEL_AUDIENCE + MongodbMap.COLLECTION_DOT + currentDays + MongodbMap.COLLECTION_DAYS;
		    	MongodbFactory.dropAndCreateAndShardingCollection(MongodbMap.DATABASE_PROCESSED_AUDIENCE, collectionName);
				MongodbFactory.createCollectionIndex(MongodbMap.DATABASE_PROCESSED_AUDIENCE, collectionName, MongodbMap.FIELD_CATALOG_ID);
				
		 		MongoSpark.write(resultDS).option("database", MongodbMap.DATABASE_PROCESSED_AUDIENCE)
		 				.option("collection",collectionName)
		 				.mode(DEFAULT_SAVEMODE)
		 				.save();
		 		if(BATCHJOB.equals(this.getRunType().name())){
					MongodbFactory.dropCollection(MongodbMap.DATABASE_PROCESSED_AUDIENCE, getPrevious7DaysProcessedDate() + MongodbMap.COLLECTION_PIXEL_AUDIENCE + MongodbMap.COLLECTION_DOT + currentDays + MongodbMap.COLLECTION_DAYS);
				}
		    }			
		}
	}
	
	/**
	 * @param sparkSession
	 */
	private void createHourlyAudience(SparkSession sparkSession){

		Dataset<Row> resultDS = sparkSession.sql("SELECT	 A.processDate"
												+ "			,A.hour "															
												+ "			,A.pixelId "															
												+ "			,A.catalogId "
												+ "			,A.pCid	"
												+ "			,B.contentCategory								AS	contentCategory"
												+ "			,B.contentIds									AS	contentIds"
												+ "			,B.contentName									AS	contentName"
												+ "		    ,CAST(MAX(B.value) AS BIGINT)					AS	price"
												+ "			,COUNT(CASE WHEN A.trackingEventCode = 'TPD002' THEN A.contentIds END)	AS viewcontent"
												+ "			,COUNT(CASE WHEN A.trackingEventCode = 'TPD003'	THEN A.contentIds END) 	AS addtocart"
												+ "			,COUNT(CASE WHEN A.trackingEventCode = 'TPD004'	THEN A.contentIds END)	AS purchased"
												+ " FROM (	SELECT	 trackingEventCode "
												+ "					,viewContent.pixel_id	   AS  pixelId"
												+ "					,viewContent.catalog_id	   AS  catalogId"
												+ "					,viewContent.content_ids   AS  contentIds"
												+ "					,viewContent.p_cid		   AS  pCid"
												+ "                 ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd') AS processDate "
												+ "					,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logDateTime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')	AS hour"
												+ "			FROM	T_PIXEL_VIEW_CONTENT"
												+ "			UNION ALL"
												+ "			SELECT	 trackingEventCode "
												+ "					,addToCart.pixel_id	   		AS  pixelId"
												+ "					,addToCart.catalog_id		AS	catalogId"
												+ "					,addToCart.content_ids		AS	contentIds"
												+ "					,addToCart.p_cid			AS	pCid"
												+ "                 ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd') AS processDate "
												+ "					,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logDateTime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')	AS hour"
												+ "			FROM	T_PIXEL_ADD_TO_CART"
												+ "			UNION ALL"
												+ "			SELECT	 'TPD004'"
												+ "					,pixelId"
												+ "					,catalogId"
												+ "					,contentIds"
												+ "					,pCid"
												+ "                 ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd') AS processDate "
												+ "					,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logDateTime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')	AS hour"
												+ "			FROM	T_PIXEL_PURCHASED	 "
												+ "			) A "
												+ "			INNER JOIN T_PIXEL_FEED_HOURLY B "
												+ "			ON 	A.processDate 	= B.processDate "
												+ "			AND A.hour			= B.hour " 
												+ "			AND A.catalogId 	= B.catalogId "
												+ "			AND A.contentIds 	= B.contentIds"
												+ "	WHERE TRIM(A.pCid) != ''"
												+ " GROUP BY A.processDate, A.hour, A.pixelId, A.catalogId, A.pCid, B.contentCategory, B.contentIds, B.contentName");

		String collectionName = getProcessDate() + MongodbMap.COLLECTION_PIXEL_AUDIENCE_HOURLY ;
		MongodbFactory.dropAndCreateAndShardingCollection(getProcessedDataBase(), collectionName );
		MongodbFactory.createCollectionIndex(getProcessedDataBase(), collectionName , MongodbMap.FIELD_CATALOG_ID);
		
		MongoSpark.write(resultDS).option("database", getProcessedDataBase())
				.option("collection", collectionName)
				.mode(DEFAULT_SAVEMODE)
				.save();
	}
	
	/**
	 * @param sparkSession
	 * @param platformCode
	 * @param catalogViewSql
	 */
	private void createDailyAudience(SparkSession sparkSession){

		
		Dataset<Row> resultDS = sparkSession.sql("SELECT	 A.processDate"
												+ "			,A.pixelId "															
												+ "			,A.catalogId "
												+ "			,A.pCid	"
												+ "			,B.contentCategory								AS 	contentCategory"
												+ "			,B.contentIds									AS	contentIds"
												+ "			,B.contentName									AS	contentName"
												+ "		    ,CAST(MAX(B.value) AS BIGINT)					AS	price"
												+ "			,COUNT(CASE WHEN A.trackingEventCode = 'TPD002' THEN A.contentIds END)	AS viewcontent"
												+ "			,COUNT(CASE WHEN A.trackingEventCode = 'TPD003'	THEN A.contentIds END) 	AS addtocart"
												+ "			,COUNT(CASE WHEN A.trackingEventCode = 'TPD004'	THEN A.contentIds END)	AS purchased"
												+ " FROM (SELECT	 trackingEventCode "
												+ "					,viewContent.pixel_id	   	AS  pixelId"
												+ "					,viewContent.catalog_id	   	AS  catalogId"
												+ "					,viewContent.content_ids   	AS  contentIds"
												+ "					,viewContent.p_cid		   	AS  pCid"
												+ "                 ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd') AS processDate "
												+ "			FROM	T_PIXEL_VIEW_CONTENT"
												+ "			UNION ALL"
												+ "			SELECT	 trackingEventCode "
												+ "					,addToCart.pixel_id			AS	pixelId"
												+ "					,addToCart.catalog_id		AS	catalogId"
												+ "					,addToCart.content_ids		AS	contentIds"
												+ "					,addToCart.p_cid			AS	pCid"
												+ "                 ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd') AS processDate "
												+ "			FROM	T_PIXEL_ADD_TO_CART"
												+ "			UNION ALL"
												+ "			SELECT	 'TPD004'"
												+ "					,pixelId"
												+ "					,catalogId"
												+ "					,contentIds"
												+ "					,pCid"
												+ "					,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd') AS processDate "
												+ "			FROM	T_PIXEL_PURCHASED	 "
												+ "			) A INNER JOIN T_PIXEL_FEED B "
												+ "				ON  A.catalogId = B.catalogId "
												+ "				AND A.contentIds = B.contentIds"
												+ "	WHERE TRIM(A.pCid) != ''"
												+ " GROUP BY A.processDate, A.pixelId, A.catalogId, A.pCid, B.contentCategory, B.contentIds, B.contentName");
		
		String collectionName = getProcessDate() + MongodbMap.COLLECTION_PIXEL_AUDIENCE_DAILY ;
		MongodbFactory.dropAndCreateAndShardingCollection(getProcessedDataBase(), collectionName );
		MongodbFactory.createCollectionIndex(getProcessedDataBase(), collectionName , MongodbMap.FIELD_CATALOG_ID);
		
		MongoSpark.write(resultDS).option("database", getProcessedDataBase())
					.option("collection", collectionName)
					.mode(DEFAULT_SAVEMODE)
					.save();
	}
}