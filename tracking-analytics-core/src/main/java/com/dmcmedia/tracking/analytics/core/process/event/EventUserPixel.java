package com.dmcmedia.tracking.analytics.core.process.event;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.AbstractDataProcess;
import com.dmcmedia.tracking.analytics.common.factory.MongodbFactory;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.map.MongodbMap;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.config.RawDataSQLTempView;
import com.dmcmedia.tracking.analytics.core.dataset.event.ConversionRawData;
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
* 2018. 4. 12.   cshwang   Initial Release
* 
* pixel 기준으로 유저별 이벤트 내역을 저장한다.
* ConversionRawData.java가 선행 되어야 한다. 
*
************************************************************************************************/
public class EventUserPixel extends AbstractDataProcess{
	
	final static String MAX_DURATION_SECONE = "900"; // 15분 기준  
	/**
	 * 생성자
	 */
	public EventUserPixel(){
		super();
	}
	
	/**
	 * 생성자 
	 * @param processType
	 * @param runType
	 */
	public EventUserPixel(String runType){
		super(runType);
	}
	
	/**
	 * 생성자
	 * 
	 * @param processType
	 * @param runType
	 */
	public EventUserPixel(String runType, String processType){
		super(runType, processType);
	}

	@Override
	public boolean processDataSet(SparkSession sparkSession) {
	
		if(super.isValid() == false){
			return false;
		}

		try{			
			if(EACH.equals(getRunType().name())){
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
			
			if(HOURLY.equals(getProcessType().name())) {
				MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", getProcessedDataBase())
						.withOption("collection", getProcessDate() + MongodbMap.COLLECTION_PIXEL_CONVERSION_RAWDATA),ConversionRawData.class)
						.where("logDateTime LIKE '"+getProcessDateHour()+"%'").createOrReplaceTempView("T_PIXEL_CONVERSION_RAWDATA");
				
			}else {
				MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", getProcessedDataBase())
						.withOption("collection", getProcessDate() + MongodbMap.COLLECTION_PIXEL_CONVERSION_RAWDATA),ConversionRawData.class)
						.createOrReplaceTempView("T_PIXEL_CONVERSION_RAWDATA");
			}
			
			switch(getProcessType().name()) {
				case HOURLY :
					createHourlyEventUser(sparkSession);	
					break;
				case DAILY :
					createDailyEventUser(sparkSession);
					break;						
				case ALL :
					createHourlyEventUser(sparkSession);
					createDailyEventUser(sparkSession);
					break;
				default :
					new Exception(ExceptionMap.INVALID_DATA_FORMAT_EXCEPTION);
			}
			
		}catch(Exception e){
			LogStack.process.error(ExceptionMap.PROCESS_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.process.error(e);
		}finally {
		}
		return true;
	}
	
	/**
	 * @param sparkSession
	 */
	private void createDailyEventUser(SparkSession sparkSession) {
		Dataset<Row> resultEventUserDS = null;
		resultEventUserDS = sparkSession.sql("WITH V_PIXEL_EVENT_USER_HOURLY AS ("
										+ "     SELECT   DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')      AS processDate"
										+ "             ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS processHour"
										+ "             ,FLOOR(DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'mm')  / 30) AS processHalfHour"
										+ "             ,pixelId"
										+ "             ,catalogId"
										+ "             ,pCid"
										+ "             ,trackingEventCode"
										+ "             ,deviceTypeCode"
										+ "             ,ids"
										+ "             ,href"
										+ "             ,referrer"
										+ "				,contentIds" 
										+ "    			,value"
										+ "             ,isoDate"
										+ "             ,logDateTime"
										+ "             ,NVL(LEAD(isoDate) OVER(PARTITION BY pixelId, catalogId, pCid, deviceTypeCode ORDER BY pixelId ASC, catalogId ASC, pCid ASC, isoDate ASC)"
										+ "					,CAST(CONCAT(DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')"
										+ "					,' '"
										+ "					,'23:59:59.999') AS TIMESTAMP))  AS nextIsoDate"
										+ "         FROM (SELECT trackingEventCode"
										+ "                     ,pageView.pixel_id      AS pixelId"
										+ "             		,pageView.catalog_id	AS catalogId   "
										+ "             	    ,pageView.p_cid		    AS pCid     "
										+ "             	    ,(CASE WHEN pageView.user_agent LIKE '%iPhone%' OR pageView.user_agent LIKE '%Android%' OR pageView.user_agent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode"
										+ "                     ,_id                    AS ids"
										+ "                     ,pageView.href          AS href"
										+ "                     ,pageView.referrer      AS referrer"
										+ "						,''				        AS contentIds"  
										+ "                    	,0      				AS value"
										+ "                     ,logDateTime"
										+ "                     ,isoDate"
										+ "             FROM	T_PIXEL_PAGE_VIEW"
										+ "             UNION ALL "
										+ "             SELECT   trackingEventCode"
										+ "                     ,viewContent.pixel_id   	AS pixelId"
										+ "             		,viewContent.catalog_id 	AS catalogId"
										+ "             	    ,viewContent.p_cid			AS pCid"
										+ "             	    ,(CASE WHEN viewContent.user_agent LIKE '%iPhone%' OR viewContent.user_agent LIKE '%Android%' OR viewContent.user_agent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode"
										+ "                     ,_id                    	AS ids"
										+ "                     ,''"
										+ "                     ,viewContent.referrer   	AS referrer"
										+ "						,viewContent.content_ids	AS contentIds"  
										+ "                    	,viewContent.value			AS value"
										+ "                     ,logDateTime"
										+ "                     ,isoDate"
										+ "             FROM	T_PIXEL_VIEW_CONTENT"
										+ "             UNION ALL "
										+ "             SELECT   trackingEventCode"
										+ "                     ,addToCart.pixel_id   		AS pixelId"
										+ "             		,addToCart.catalog_id 		AS catalogId"
										+ "             	    ,addToCart.p_cid			AS pCid"
										+ "             	    ,(CASE WHEN addToCart.user_agent LIKE '%iPhone%' OR addToCart.user_agent LIKE '%Android%' OR addToCart.user_agent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode"
										+ "                     ,_id                    	AS ids"
										+ "                     ,''"
										+ "                     ,addToCart.referrer     	AS referrer"
										+ "						,addToCart.content_ids		AS contentIds"  
										+ "                    	,addToCart.value			AS value"
										+ "                     ,logDateTime"
										+ "                     ,isoDate"
										+ "             FROM	T_PIXEL_ADD_TO_CART"
										+ "             UNION ALL "
										+ "             SELECT	 'TPD004'"
										+ "                     ,pixelId"
										+ "                     ,catalogId"
										+ "                     ,pCid"
										+ "                     ,(CASE WHEN userAgent LIKE '%iPhone%' OR userAgent LIKE '%Android%' OR userAgent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode"
										+ "                     ,_id"
										+ "                     ,''"
										+ "                     ,''"
										+ "						,contentIds"
										+ "						,value"
										+ "						,logDateTime"
										+ "                     ,isoDate"
										+ "             FROM	T_PIXEL_PURCHASED"
										+ "     ) "
										+ " ),V_PIXEL_CONVERSION_RAWDATA AS ("
										+ "     SELECT   DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')      AS processDate"
										+ "             ,pixelId"
										+ "             ,catalogId"
										+ "             ,mediaId"
										+ "             ,channelId"
										+ "             ,productsId"
										+ "             ,campaignId"
										+ "             ,linkId"
										+ "             ,deviceTypeCode"
										+ "             ,pCid"
										+ "             ,conversionRawdataTypeCode"
										+ "             ,href"
										+ "             ,referrer"
										+ "             ,utmMedium"
										+ "             ,utmSource"
										+ "             ,utmCampaign"
										+ "             ,utmTerm"
										+ "             ,utmContent"
										+ "             ,isoDate"
										+ "             ,NVL(LEAD(isoDate) OVER(PARTITION BY pixelId, catalogId, pCid, deviceTypeCode ORDER BY pixelId ASC, catalogId ASC, pCid ASC, isoDate ASC)"
										+ "					,CAST(CONCAT(DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')"
										+ "					,' '"
										+ "					,'23:59:59.999') AS TIMESTAMP))   AS nextIsoDate"
										+ "     FROM    T_PIXEL_CONVERSION_RAWDATA"
										+ "     WHERE   conversionRawdataTypeCode != 'CRT006'"
										+ " )"
										+ " "
										+ " SELECT   processDate"
										+ "         ,pixelId"
										+ "         ,catalogId"
										+ "         ,pCid"
										+ "         ,deviceTypeCode"
										+ "         ,conversionRawdataTypeCode"
										+ "         ,mediaId"
										+ "         ,channelId"
										+ "         ,productsId"
										+ "         ,campaignId"
										+ "         ,linkId"
										+ "         ,SUM(duration)     		AS duration"
										+ "         ,SUM(session)      		AS session "
										+ "         ,SUM(pageview)	    	AS pageview"
										+ "         ,SUM(viewcontent)		AS viewcontent"
										+ "         ,SUM(addtocart)			AS addtocart"
										+ "         ,SUM(purchased) 		AS purchased"
										+ "         ,SUM(viewcontentValue) 	AS viewcontentValue"
										+ "         ,SUM(addtocartValue) 	AS addtocartValue"
										+ "         ,SUM(purchasedValue) 	AS purchasedValue"
										+ "			,MIN(logDateTime)		AS logDateTime" 
										+ " FROM ("
										+ "     SELECT	 processDate"
										+ "     		,processHour"
										+ "     		,processHalfHour"
										+ "             ,pixelId"
										+ "             ,catalogId"
										+ "             ,pCid"
										+ "             ,deviceTypeCode"
										+ "             ,conversionRawdataTypeCode"
										+ "             ,mediaId"
										+ "             ,channelId"
										+ "             ,productsId"
										+ "             ,campaignId"
										+ "             ,linkId"
										+ "             ,SUM(durationSecond) AS duration"
										+ "             ,CASE WHEN NVL(COUNT(CASE WHEN trackingEventCode = 'TPD001' THEN pCid  END), 0) > 0 "
										+ "                     OR NVL(COUNT(CASE WHEN trackingEventCode = 'TPD002' THEN pCid  END), 0) > 0 "
										+ "                     OR NVL(COUNT(CASE WHEN trackingEventCode = 'TPD003' THEN pCid  END), 0) > 0"
										+ "                     OR NVL(COUNT(CASE WHEN trackingEventCode = 'TPD004' THEN pCid  END), 0) > 0 "
										+ "                     THEN 1 ELSE 0 END   AS session   "
										+ "             ,CAST(NVL(COUNT(CASE WHEN trackingEventCode = 'TPD001' THEN pCid  END), 0) AS BIGINT)	AS pageview"
										+ "             ,CAST(NVL(COUNT(CASE WHEN trackingEventCode = 'TPD002' THEN pCid  END), 0) AS BIGINT)	AS viewcontent"
										+ "             ,CAST(NVL(COUNT(CASE WHEN trackingEventCode = 'TPD003' THEN pCid  END), 0) AS BIGINT)	AS addtocart"
										+ "             ,CAST(NVL(COUNT(CASE WHEN trackingEventCode = 'TPD004' THEN pCid  END), 0) AS BIGINT)	AS purchased"
										+ "         	,CAST(NVL(SUM(CASE WHEN trackingEventCode = 'TPD002' THEN value ELSE 0 END), 0) AS DECIMAL(12,2))	AS viewcontentValue" 
										+ "         	,CAST(NVL(SUM(CASE WHEN trackingEventCode = 'TPD003' THEN value ELSE 0 END), 0) AS DECIMAL(12,2))	AS addtocartValue" 
										+ "         	,CAST(NVL(SUM(CASE WHEN trackingEventCode = 'TPD004' THEN value ELSE 0 END), 0) AS DECIMAL(12,2))	AS purchasedValue"
										+ "				,MIN(logDateTime)													    							AS logDateTime" 
										+ "     FROM("
										+ "         SELECT   A.processDate"
										+ "                 ,A.processHour"
										+ "                 ,A.processHalfHour"
										+ "                 ,A.pixelId"
										+ "                 ,A.catalogId"
										+ "                 ,A.deviceTypeCode"
										+ "                 ,NVL(B.conversionRawdataTypeCode, 'CRT999') AS conversionRawdataTypeCode"
										+ "                 ,A.pCid"
										+ "                 ,A.ids"
										+ "                 ,IFNULL(B.mediaId, '')      AS mediaId"
										+ "                 ,IFNULL(B.channelId, '')    AS channelId"
										+ "                 ,IFNULL(B.productsId, '')   AS productsId"
										+ "                 ,IFNULL(B.campaignId, '')   AS campaignId"
										+ "                 ,IFNULL(B.linkId, '')       AS linkId"
										+ "                 ,A.trackingEventCode"
										+ "					,A.contentIds"
										+ "					,A.value"
										+ "                 ,A.isoDate"
										+ "					,A.logDateTime"
										+ "                 ,CASE WHEN UNIX_TIMESTAMP(A.nextIsoDate) - UNIX_TIMESTAMP(A.isoDate) >= "+MAX_DURATION_SECONE+" THEN 0 ELSE UNIX_TIMESTAMP(A.nextIsoDate) - UNIX_TIMESTAMP(A.isoDate) END  AS durationSecond"
										+ "             FROM    V_PIXEL_EVENT_USER_HOURLY A"
										+ "             LEFT OUTER JOIN "
										+ "                 V_PIXEL_CONVERSION_RAWDATA B"
										+ "             ON  A.processDate       = B.processDate"
										+ "             AND A.pixelId           = B.pixelId "
										+ "             AND A.catalogId         = B.catalogId "
										+ "             AND A.pCid              = B.pCid "
										+ "             AND A.deviceTypeCode    = B.deviceTypeCode"
										+ "             AND A.isoDate           >= B.isoDate "
										+ "             AND A.isoDate           < B.nextIsoDate"
										+ "         )"
										+ "     GROUP BY processDate, processHour, processHalfHour, pixelId, catalogId, pCid, deviceTypeCode, conversionRawdataTypeCode, mediaId, channelId, productsId, campaignId, linkId"
										+ "     )"
										+ "  GROUP BY processDate, pixelId, catalogId, pCid, deviceTypeCode, conversionRawdataTypeCode, mediaId, channelId, productsId, campaignId, linkId"
										+ " ");

		MongodbFactory.dropAndCreateAndShardingCollection(getProcessedDataBase(), getProcessDate() + MongodbMap.COLLECTION_PIXEL_EVENT_USER );
		MongodbFactory.createCollectionCompoundIndex(getProcessedDataBase(), getProcessDate() + MongodbMap.COLLECTION_PIXEL_EVENT_USER, new String[]{"pixelId","catalogId","pCid"});
		MongoSpark.write(resultEventUserDS).option("database", getProcessedDataBase())
											.option("collection",getProcessDate() + MongodbMap.COLLECTION_PIXEL_EVENT_USER )
											.mode(DEFAULT_SAVEMODE)					
											.save();
	}
	
	/**
	 * @param sparkSession
	 */
	private void createHourlyEventUser(SparkSession sparkSession) {
		Dataset<Row> resultEventUserDS = null;
		resultEventUserDS = sparkSession.sql("WITH V_PIXEL_EVENT_USER_HOURLY AS ("
										+ "     SELECT   DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')      AS processDate"
										+ "             ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS processHour"
										+ "             ,FLOOR(DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'mm')  / 30) AS processHalfHour"
										+ "             ,pixelId"
										+ "             ,catalogId"
										+ "             ,pCid"
										+ "             ,trackingEventCode"
										+ "             ,deviceTypeCode"
										+ "             ,ids"
										+ "             ,href"
										+ "             ,referrer"
										+ "				,contentIds" 
										+ "    			,value"
										+ "             ,isoDate"
										+ "             ,logDateTime"
										+ "             ,NVL(LEAD(isoDate) OVER(PARTITION BY pixelId, catalogId, pCid, deviceTypeCode ORDER BY pixelId ASC, catalogId ASC, pCid ASC, isoDate ASC)"
										+ "					,CAST(CONCAT(DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')"
										+ "					,' '"
										+ "					,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')"
										+ "					,':59:59.999') AS TIMESTAMP))  AS nextIsoDate"
										+ "         FROM (SELECT trackingEventCode"
										+ "                     ,pageView.pixel_id      AS pixelId"
										+ "             		,pageView.catalog_id	AS catalogId   "
										+ "             	    ,pageView.p_cid		    AS pCid     "
										+ "             	    ,(CASE WHEN pageView.user_agent LIKE '%iPhone%' OR pageView.user_agent LIKE '%Android%' OR pageView.user_agent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode"
										+ "                     ,_id                    AS ids"
										+ "                     ,pageView.href          AS href"
										+ "                     ,pageView.referrer      AS referrer"
										+ "						,''				        AS contentIds"  
										+ "                    	,0      				AS value"
										+ "                     ,logDateTime"
										+ "                     ,isoDate"
										+ "             FROM	T_PIXEL_PAGE_VIEW"
										+ "             UNION ALL "
										+ "             SELECT   trackingEventCode"
										+ "                     ,viewContent.pixel_id   	AS pixelId"
										+ "             		,viewContent.catalog_id 	AS catalogId"
										+ "             	    ,viewContent.p_cid			AS pCid"
										+ "             	    ,(CASE WHEN viewContent.user_agent LIKE '%iPhone%' OR viewContent.user_agent LIKE '%Android%' OR viewContent.user_agent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode"
										+ "                     ,_id                    	AS ids"
										+ "                     ,''"
										+ "                     ,viewContent.referrer   	AS referrer"
										+ "						,viewContent.content_ids	AS contentIds"  
										+ "                    	,viewContent.value			AS value"
										+ "                     ,logDateTime"
										+ "                     ,isoDate"
										+ "             FROM	T_PIXEL_VIEW_CONTENT"
										+ "             UNION ALL "
										+ "             SELECT   trackingEventCode"
										+ "                     ,addToCart.pixel_id   		AS pixelId"
										+ "             		,addToCart.catalog_id 		AS catalogId"
										+ "             	    ,addToCart.p_cid			AS pCid"
										+ "             	    ,(CASE WHEN addToCart.user_agent LIKE '%iPhone%' OR addToCart.user_agent LIKE '%Android%' OR addToCart.user_agent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode"
										+ "                     ,_id                    	AS ids"
										+ "                     ,''"
										+ "                     ,addToCart.referrer     	AS referrer"
										+ "						,addToCart.content_ids		AS contentIds"  
										+ "                    	,addToCart.value			AS value"
										+ "                     ,logDateTime"
										+ "                     ,isoDate"
										+ "             FROM	T_PIXEL_ADD_TO_CART"
										+ "             UNION ALL "
										+ "             SELECT	 'TPD004'"
										+ "                     ,pixelId"
										+ "                     ,catalogId"
										+ "                     ,pCid"
										+ "                     ,(CASE WHEN userAgent LIKE '%iPhone%' OR userAgent LIKE '%Android%' OR userAgent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode"
										+ "                     ,_id"
										+ "                     ,''"
										+ "                     ,''"
										+ "						,contentIds"
										+ "						,value"
										+ "						,logDateTime"
										+ "                     ,isoDate"
										+ "             FROM	T_PIXEL_PURCHASED"
										+ "     ) " 
										+ " ),V_PIXEL_CONVERSION_RAWDATA AS (" 
										+ "     SELECT   DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')      AS processDate" 
										+ "             ,pixelId" 
										+ "             ,catalogId" 
										+ "             ,mediaId" 
										+ "             ,channelId" 
										+ "             ,productsId" 
										+ "             ,campaignId" 
										+ "             ,linkId" 
										+ "             ,deviceTypeCode" 
										+ "             ,pCid" 
										+ "             ,conversionRawdataTypeCode" 
										+ "             ,href" 
										+ "             ,referrer" 
										+ "             ,utmMedium" 
										+ "             ,utmSource" 
										+ "             ,utmCampaign" 
										+ "             ,utmTerm" 
										+ "             ,utmContent" 
										+ "             ,isoDate" 
										+ "             ,NVL(LEAD(isoDate) OVER(PARTITION BY pixelId, catalogId, pCid, deviceTypeCode ORDER BY pixelId ASC, catalogId ASC, pCid ASC, isoDate ASC)"
										+ "					,CAST(CONCAT(DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')"
										+ "					,' '"
										+ "					,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')"
										+ "					,':59:59.999') AS TIMESTAMP))  AS nextIsoDate"
										+ "     FROM    T_PIXEL_CONVERSION_RAWDATA" 
										+ "     WHERE   conversionRawdataTypeCode != 'CRT006'" 
										+ " )" 
										+ " " 
										+ " SELECT	 processDate" 
										+ " 		,processHour" 
										+ " 		,processHalfHour" 
										+ "         ,pixelId" 
										+ "         ,catalogId" 
										+ "         ,pCid" 
										+ "         ,deviceTypeCode" 
										+ "         ,conversionRawdataTypeCode" 
										+ "         ,mediaId" 
										+ "         ,channelId" 
										+ "         ,productsId" 
										+ "         ,campaignId" 
										+ "         ,linkId" 
										+ "         ,SUM(durationSecond) AS duration" 
										+ "         ,CASE WHEN NVL(COUNT(CASE WHEN trackingEventCode = 'TPD001' THEN pCid  END), 0) > 0" 
										+ "                 OR NVL(COUNT(CASE WHEN trackingEventCode = 'TPD002' THEN pCid  END), 0) > 0" 
										+ "                 OR NVL(COUNT(CASE WHEN trackingEventCode = 'TPD003' THEN pCid  END), 0) > 0" 
										+ "                 OR NVL(COUNT(CASE WHEN trackingEventCode = 'TPD004' THEN pCid  END), 0) > 0" 
										+ "                 THEN 1 ELSE 0 END   AS session   " 
										+ "         ,NVL(COUNT(CASE WHEN trackingEventCode = 'TPD001' THEN pCid  END), 0)		AS pageview" 
										+ "         ,NVL(COUNT(CASE WHEN trackingEventCode = 'TPD002' THEN pCid  END), 0)		AS viewcontent" 
										+ "         ,NVL(COUNT(CASE WHEN trackingEventCode = 'TPD003' THEN pCid  END), 0)		AS addtocart" 
										+ "         ,NVL(COUNT(CASE WHEN trackingEventCode = 'TPD004' THEN pCid  END), 0)		AS purchased" 
										+ "         ,NVL(SUM(CASE WHEN trackingEventCode = 'TPD002' THEN value ELSE 0 END), 0)	AS viewcontentValue" 
										+ "         ,NVL(SUM(CASE WHEN trackingEventCode = 'TPD003' THEN value ELSE 0 END), 0)	AS addtocartValue" 
										+ "         ,NVL(SUM(CASE WHEN trackingEventCode = 'TPD004' THEN value ELSE 0 END), 0)	AS purchasedValue"
										+ "			,MIN(logDateTime)													    	AS logDateTime" 
										+ " FROM(" 
										+ "     SELECT   A.processDate   " 
										+ "             ,A.processHour" 
										+ "             ,A.processHalfHour" 
										+ "             ,A.pixelId" 
										+ "             ,A.catalogId" 
										+ "             ,A.deviceTypeCode" 
										+ "             ,NVL(B.conversionRawdataTypeCode, 'CRT999') AS conversionRawdataTypeCode" 
										+ "             ,A.pCid" 
										+ "             ,A.ids" 
										+ "             ,IFNULL(B.mediaId, '')      AS mediaId" 
										+ "             ,IFNULL(B.channelId, '')    AS channelId" 
										+ "             ,IFNULL(B.productsId, '')   AS productsId" 
										+ "             ,IFNULL(B.campaignId, '')   AS campaignId" 
										+ "             ,IFNULL(B.linkId, '')       AS linkId" 
										+ "             ,A.trackingEventCode" 
										+ "				,A.contentIds"
										+ "				,A.value"
										+ "				,A.logDateTime"
										+ "             ,A.isoDate" 
										+ "             ,CASE WHEN UNIX_TIMESTAMP(A.nextIsoDate) - UNIX_TIMESTAMP(A.isoDate) >= "+MAX_DURATION_SECONE+" THEN 0 ELSE UNIX_TIMESTAMP(A.nextIsoDate) - UNIX_TIMESTAMP(A.isoDate) END  AS durationSecond" 
										+ "         FROM    V_PIXEL_EVENT_USER_HOURLY A" 
										+ "         LEFT OUTER JOIN " 
										+ "             V_PIXEL_CONVERSION_RAWDATA B" 
										+ "         ON  A.processDate       = B.processDate" 
										+ "         AND A.pixelId           = B.pixelId " 
										+ "         AND A.catalogId         = B.catalogId " 
										+ "         AND A.pCid              = B.pCid " 
										+ "         AND A.deviceTypeCode    = B.deviceTypeCode" 
										+ "         AND A.isoDate           >= B.isoDate " 
										+ "         AND A.isoDate           < B.nextIsoDate" 
										+ "     )" 
										+ " GROUP BY processDate, processHour, processHalfHour, pixelId, catalogId, pCid, deviceTypeCode, conversionRawdataTypeCode, mediaId, channelId, productsId, campaignId, linkId"
										+ " ");
		
		if(!MongodbFactory.collectionExists(getProcessedDataBase(), getProcessDate() + MongodbMap.COLLECTION_PIXEL_EVENT_USER_HOURLY) || ALL.equals(getProcessType().name())){
			MongodbFactory.dropAndCreateAndShardingCollection(getProcessedDataBase(), getProcessDate() + MongodbMap.COLLECTION_PIXEL_EVENT_USER_HOURLY);
			MongodbFactory.createCollectionIndex(getProcessedDataBase(), getProcessDate() + MongodbMap.COLLECTION_PIXEL_EVENT_USER_HOURLY, "logDateTime");
			MongodbFactory.createCollectionCompoundIndex(getProcessedDataBase(), getProcessDate() + MongodbMap.COLLECTION_PIXEL_EVENT_USER_HOURLY, new String[]{"pixelId","catalogId","pCid"});
		}
		MongoSpark.write(resultEventUserDS).option("database", getProcessedDataBase())
											.option("collection",getProcessDate() + MongodbMap.COLLECTION_PIXEL_EVENT_USER_HOURLY )
											.mode(DEFAULT_SAVEMODE)					
											.save();
	}
}
