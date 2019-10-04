package com.dmcmedia.tracking.analytics.core.report.ecomm;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import com.dmcmedia.tracking.analytics.common.AbstractDataReport;
import com.dmcmedia.tracking.analytics.common.factory.SparkJdbcConnectionFactory;
import com.dmcmedia.tracking.analytics.common.map.CodeMap;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.map.MongodbMap;
import com.dmcmedia.tracking.analytics.common.util.DateUtil;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.config.RawDataSQLTempView;
import com.dmcmedia.tracking.analytics.core.dataset.ConversionAttribution;
import com.dmcmedia.tracking.analytics.core.dataset.TrackingPixelDataSet;
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
* 2018. 9. 16.   cshwang   Initial Release
*
*
************************************************************************************************/
public class EcommLastClickConversionChannels extends AbstractDataReport {
	
	/**
	 * 생성자 
	 */
	public EcommLastClickConversionChannels(){
		super();
	}
	
	/**
	 * 생성자
	 * @param runType
	 */
	public EcommLastClickConversionChannels(String runType){
		super(runType);
	}
	
	/**
	 * 생성자
	 * @param runType
	 * @param reportType
	 */
	public EcommLastClickConversionChannels(String runType, String reportType){
		super(runType, reportType);
	}
	
	public boolean reportsDataSet(SparkSession sparkSession) {

		Dataset<Row> attrubutionListDS = null;
		Dataset<Row> maxConversionDaysDS = null;
		Dataset<ConversionRawData> pixelConversionRawDataDS = null;
		Dataset<TrackingPixelDataSet> pixelViewcontentDS = null;
		Dataset<TrackingPixelDataSet> pixelAddtocartDS = null;
		List<String> maxConversionDaysList = null;
		List<ConversionAttribution> attributionList = null;
		String targetHourlyTableName 	= "tracking.TS_TRK_WEB_LINK_ECOMM_HOURLY";
		String targetDailyTableName 	= "tracking.TS_TRK_WEB_LINK_ECOMM_DAILY";
		int maxConversionDays = 0;
		JavaSparkContext jsc = null;	

		if(super.isValid() == false){
			return false;
		}

		try{
		    
		    if(EACH.equals(this.getRunType().name())){
		    	RawDataSQLTempView rawDataSQLTempView = new RawDataSQLTempView(getReportDate());
		    	rawDataSQLTempView.createPixelRawDataDailyTempView(sparkSession);
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
			}
		    sparkSession.udf().register("ADD_HOURS", addhours, DataTypes.TimestampType);
		    jsc = new JavaSparkContext(sparkSession.sparkContext());
		    
		    String trkAttribution = "(	SELECT	 COMMON_CODE_ITEM AS attributionCode"
		    					+ "				,COMMON_CODE_ITEM_NAME AS attribution "
		    					+ "		FROM 	common.TC_CMM_COMM_CODE_ITEM "
		    					+ "		WHERE 	SYSTEM_TYPE = 'TRK' "
		    					+ "		AND COMMON_CODE = 'TTC016' "
		    					+ "		AND USE_YN = 'Y' "
		    					+ "		AND DEL_YN = 'N') AS T_TRK_ATTRIBUTION";
		    
			String trkMaxConversionDays = "(SELECT CEIL(MAX(CAST(COMMON_CODE_ITEM_NAME AS UNSIGNED)) / 24) + 1 AS maxConversionDays "
										+ "	FROM common.TC_CMM_COMM_CODE_ITEM "
										+ "	WHERE SYSTEM_TYPE = 'TRK' "
										+ "	AND COMMON_CODE = 'TTC016' "
										+ "	AND USE_YN = 'Y' AND DEL_YN = 'N') AS T_TRK_CONVERSION_DAYS";
		    
			attrubutionListDS = SparkJdbcConnectionFactory.getSelectJdbcDfLoad(sparkSession, trkAttribution);
			maxConversionDaysDS = SparkJdbcConnectionFactory.getSelectJdbcDfLoad(sparkSession, trkMaxConversionDays);
			maxConversionDaysList =  maxConversionDaysDS.as(Encoders.STRING()).collectAsList();
			attributionList =  attrubutionListDS.as(Encoders.bean(ConversionAttribution.class)).collectAsList();
			maxConversionDays = Integer.parseInt(maxConversionDaysList.get(0));
			
			LogStack.report.info(maxConversionDays);
			LogStack.report.info(attributionList);
			
			for (int currentDays = 1 ; currentDays <= maxConversionDays; currentDays++ ) {
				String conversionDate = DateUtil.getChangeDateFormatIntervalDay(this.getReportDate(), CodeMap.DATE_YMD_PATTERN_FOR_ANYS, CodeMap.DATE_YMD_PATTERN_FOR_ANYS, -(currentDays - 1));
				String conversionDateMonth = DateUtil.getChangeDateFormatIntervalDay(this.getReportDate(), CodeMap.DATE_YMD_PATTERN_FOR_ANYS, CodeMap.DATE_MONTH_PATTERN, -(currentDays - 1));
				String conversionProcessedDataBase = MongodbMap.DATABASE_PROCESSED_MONTH + conversionDateMonth; 
				String conversionRawDataBase = MongodbMap.DATABASE_RAW_MONTH + conversionDateMonth;
				
				if(currentDays == 1) {
					pixelConversionRawDataDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database", conversionProcessedDataBase).withOption("collection", conversionDate + MongodbMap.COLLECTION_PIXEL_CONVERSION_RAWDATA)).toDS(ConversionRawData.class);
					pixelViewcontentDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database", conversionRawDataBase).withOption("collection", conversionDate + MongodbMap.COLLECTION_PIXEL_VIEW_CONTENT)).toDS(TrackingPixelDataSet.class);
					pixelAddtocartDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database", conversionRawDataBase).withOption("collection",  conversionDate + MongodbMap.COLLECTION_PIXEL_ADD_TO_CART)).toDS(TrackingPixelDataSet.class);
					LogStack.report.info("currentDays :" + currentDays);
				}else {
					Dataset<ConversionRawData> rPixelConversionRawdataDS= MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database", conversionProcessedDataBase).withOption("collection", conversionDate + MongodbMap.COLLECTION_PIXEL_CONVERSION_RAWDATA)).toDS(ConversionRawData.class);
					if(rPixelConversionRawdataDS != null && rPixelConversionRawdataDS.count() > 0) {
						pixelConversionRawDataDS = pixelConversionRawDataDS.union(rPixelConversionRawdataDS);
					}
					
					Dataset<TrackingPixelDataSet> rPixelViewcontentDS= MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database", conversionRawDataBase).withOption("collection", conversionDate + MongodbMap.COLLECTION_PIXEL_VIEW_CONTENT)).toDS(TrackingPixelDataSet.class);
					if(rPixelViewcontentDS != null && rPixelViewcontentDS.count() > 0) {
						pixelViewcontentDS = pixelViewcontentDS.union(rPixelViewcontentDS);
					}
					
					Dataset<TrackingPixelDataSet> rPixelAddtocartDS= MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database", conversionRawDataBase).withOption("collection", conversionDate + MongodbMap.COLLECTION_PIXEL_ADD_TO_CART)).toDS(TrackingPixelDataSet.class);
					if(rPixelAddtocartDS != null && rPixelAddtocartDS.count() > 0) {
						pixelAddtocartDS = pixelAddtocartDS.union(rPixelAddtocartDS);
					}
					LogStack.report.info("currentDays :" + currentDays);
				}
			}

			pixelConversionRawDataDS.persist().createOrReplaceTempView("V_PIXEL_CONVERSION_RAWDATA");
		    pixelViewcontentDS.persist().createOrReplaceTempView("V_PIXEL_VIEW_CONTENT");
			pixelAddtocartDS.persist().createOrReplaceTempView("V_PIXEL_ADD_TO_CART");
		    
			LogStack.report.info("Report Status : " +getReportType().name());
			
		    switch(getReportType().name()) {
				case HOURLY :
					for (ConversionAttribution attribution : attributionList) {
				    	LogStack.report.debug(attribution.getAttribution());
				    	LogStack.report.debug(attribution.getAttributionCode());
				    	Dataset<Row> resultHourlyDS  = sparkSession.sql(geHourlyQuery(attribution.getAttribution(), attribution.getAttributionCode()));		    
				    	resultHourlyDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), targetHourlyTableName, SparkJdbcConnectionFactory.getDefaultJdbcOptions());
				    }
					break;
				case DAILY :
					for (ConversionAttribution attribution : attributionList) {
				    	LogStack.report.debug(attribution.getAttribution());
				    	LogStack.report.debug(attribution.getAttributionCode());
				    	Dataset<Row> resultDailyDS  = sparkSession.sql(getDailyQuery(attribution.getAttribution(), attribution.getAttributionCode()));		    
				    	resultDailyDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), targetDailyTableName, SparkJdbcConnectionFactory.getDefaultJdbcOptions());
				    }
				case ALL :
					for (ConversionAttribution attribution : attributionList) {
						LogStack.report.debug(attribution.getAttribution());
						LogStack.report.debug(attribution.getAttributionCode());
						Dataset<Row> resultHourlyDS  = sparkSession.sql(geHourlyQuery(attribution.getAttribution(), attribution.getAttributionCode()));		    
				    	resultHourlyDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), targetHourlyTableName, SparkJdbcConnectionFactory.getDefaultJdbcOptions());
						Dataset<Row> resultDailyDS  = sparkSession.sql(getDailyQuery(attribution.getAttribution(), attribution.getAttributionCode()));		    
						resultDailyDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), targetDailyTableName, SparkJdbcConnectionFactory.getDefaultJdbcOptions());
					}
				default :
					new Exception(ExceptionMap.INVALID_DATA_FORMAT_EXCEPTION);
					return false;
		    }		

		}catch(Exception e){
			LogStack.report.error(ExceptionMap.REPORT_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.report.error(e);
		}finally {
			if(pixelConversionRawDataDS != null) {
				pixelConversionRawDataDS.unpersist();
			}
			if(pixelViewcontentDS != null) {
				pixelViewcontentDS.unpersist();
			}
			if(pixelAddtocartDS != null) {
				pixelAddtocartDS.unpersist();
			}
		}
		return true;
	}

	private String getDailyQuery(String attribution, String attributionCode) {
		StringBuffer queryString = new StringBuffer();
		queryString.append("SELECT   V.reportDate 			AS REPORT_DATE "
						+ "         ,TW.webId  				AS WEB_ID "
						+ " 		,V.mediaId  			AS MEDIA_ID "
						+ " 		,V.channelId			AS CHANNEL_ID "
						+ " 		,V.campaignId			AS CAMPAIGN_ID "
						+ " 		,V.linkId  				AS LINK_ID "
						+ " 		,'CTC002' 				AS CONVERSION_TYPE_CODE "
						+ " 		,'"+attributionCode+"'  AS ATTRIBUTION_CODE "
						+ "         ,V.revenue              AS TOTAL_REVENUE  "
						+ " 		,V.transaction          AS TRANSACTION "
						+ "         ,V.uniquePurchased 	    AS UNIQUE_PURCHASE "
						+ " 		,V.uniqueViewContent    AS UNIQUE_VIEW_CONTENT "
						+ " 		,V.uniqueAddToCart      AS UNIQUE_ADD_TO_CART "
						+ "  "
						+ " FROM(SELECT  TT3.reportDate "
						+ "             ,TT3.catalogId "
						+ "             ,TT3.mediaId "
						+ "     		,TT3.channelId "
						+ "     		,TT3.campaignId "
						+ "     		,TT3.linkId "
						+ "             ,'CTC002'                           AS conversionTypeCode "
						+ "             ,'ATC001'                           AS attributionCode "
						+ "             ,SUM(TT3.value)                     AS revenue "
						+ "             ,COUNT(TT3.purchasedId)             AS transaction "
						+ "             ,COUNT(TT3.purchasedId)             AS purchased "
						+ "             ,SUM(TT3.viewContent)               AS viewContent "
						+ "             ,SUM(TT3.addToCart)                 AS addToCart "
						+ "             ,COUNT(DISTINCT TT3.purchasedpCid)  AS uniquePurchased "
						+ "             ,SUM(TT3.uniqueViewContent)         AS uniqueViewContent "
						+ "             ,SUM(TT3.uniqueAddToCart)           AS uniqueAddToCart "
						+ "      "
						+ "     FROM (  SELECT   TT1.reportDate "
						+ "                     ,TT1.catalogId "
						+ "                     ,TT1.mediaId "
						+ "             		,TT1.channelId "
						+ "             		,TT1.campaignId "
						+ "             		,TT1.linkId "
						+ "                     ,TT1.value "
						+ "                     ,TT1.purchasedpCid "
						+ "                     ,TT1.purchasedId "
						+ "                     ,SUM(CASE WHEN TT2.eventType = 'VIEW_CONTENT' THEN 1 ELSE 0 END)    AS viewContent "
						+ "                     ,SUM(CASE WHEN TT2.eventType = 'ADD_TO_CART'  THEN 1 ELSE 0 END)    AS addToCart "
						+ "                     ,COUNT(DISTINCT (CASE WHEN TT2.eventType = 'VIEW_CONTENT'  THEN TT2.pCid END))      AS uniqueViewContent "
						+ "                     ,COUNT(DISTINCT (CASE WHEN TT2.eventType = 'ADD_TO_CART'   THEN TT2.pCid END))      AS uniqueAddToCart "
						+ "                      "
						+ "             FROM ( SELECT    DATE_FORMAT(CAST(UNIX_TIMESTAMP(T1.purchasedDateTime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'yyyy-MM-dd')	AS reportDate "
						+ "                             ,T1.catalogId "
						+ "                             ,T1.mediaId "
						+ "                             ,T1.channelId "
						+ "                             ,T1.campaignId "
						+ "                             ,T1.linkId "
						+ "                             ,T1.contentIds "
						+ "                             ,T1.value "
						+ "                             ,T1.purchasedDateTime "
						+ "                             ,T1.purchasedpCid "
						+ "                             ,T1.purchasedId "
						+ "                             ,RANK() OVER (PARTITION BY T1.catalogId, T1.contentIds, T1.purchasedpCid, T1.purchasedDateTime  ORDER BY  T1.conversionDateTime DESC, T1.conversionId DESC ) AS pt "
						+ "      "
						+ "                     FROM    (SELECT  A.catalogId "
						+ "                                     ,NVL(B.mediaId, '')		AS mediaId "
						+ "                                     ,NVL(B.channelId, '')  	AS channelId "
						+ "                                     ,NVL(B.campaignId, '')	AS campaignId "
						+ "                                     ,NVL(B.linkId , '')     AS linkId "
						+ "                                     ,A.contentIds "
						+ "                                     ,A.value "
						+ "                                     ,A.logDateTime  AS purchasedDateTime "
						+ "                                     ,B.logDateTime  AS conversionDateTime "
						+ "                                     ,A.pCid         AS purchasedpCid "
						+ "                                     ,B.pCid         AS conversionpCid "
						+ "                                     ,A._id          AS purchasedId "
						+ "                                     ,B.ids			AS conversionId "
						+ "                             FROM    T_PIXEL_PURCHASED A "
						+ "                                     LEFT OUTER JOIN  V_PIXEL_CONVERSION_RAWDATA B "
						+ "                                     ON  A.catalogId  = B.catalogId "
						+ "                                     AND A.pCid       = B.pCid  "
						+ "                                     AND CAST(B.logDateTime AS TIMESTAMP) BETWEEN ADD_HOURS(CAST(A.logDateTime AS TIMESTAMP), -"+attribution+") AND CAST(A.logDateTime AS TIMESTAMP)  "
						+ "                             ) T1 "
						+ "                     ) TT1  "
						+ "                 INNER JOIN "
						+ "                 (SELECT  A.eventType "
						+ "                         ,A.pCid "
						+ "                         ,A.catalogId "
						+ "                         ,A.contentIds  "
						+ "                         ,A.logDateTime "
						+ "                 FROM (SELECT     'VIEW_CONTENT'             AS eventType "
						+ "                                 ,viewContent.p_cid          AS pCid "
						+ "                                 ,viewContent.catalog_id     AS catalogId "
						+ "                                 ,viewContent.content_ids    AS contentIds "
						+ "                                 ,logDateTime "
						+ "                      FROM       V_PIXEL_VIEW_CONTENT "
						+ "                          "
						+ "                      UNION  "
						+ "                          "
						+ "                      SELECT      'ADD_TO_CART'              AS eventType "
						+ "                                 ,addToCart.p_cid            AS pCid "
						+ "                                 ,addToCart.catalog_id       AS catalogId "
						+ "                                 ,addToCart.content_ids      AS contentIds "
						+ "                                 ,logDateTime "
						+ "                      FROM    V_PIXEL_ADD_TO_CART) A "
						+ "                 ) TT2 "
						+ "                 ON  TT1.catalogId = TT2.catalogId  "
						+ "                     AND TT1.purchasedpCid = TT2.pCid "
						+ "                     AND CAST(TT2.logDateTime AS TIMESTAMP) BETWEEN ADD_HOURS(CAST(TT1.purchasedDateTime AS TIMESTAMP), -"+attribution+") AND CAST(TT1.purchasedDateTime AS TIMESTAMP) "
						+ "              WHERE TT1.pt = 1 "
						+ "              GROUP BY TT1.reportDate, TT1.catalogId, TT1.mediaId, TT1.channelId, TT1.campaignId, TT1.linkId, TT1.value,TT1.purchasedpCid, TT1.purchasedId "
						+ "         ) TT3  "
						+ "     GROUP BY TT3.reportDate, TT3.catalogId, TT3.mediaId, TT3.channelId, TT3.campaignId, TT3.linkId "
						+ " ) V "
						+ " INNER JOIN T_TRK_WEB  TW  ON	V.catalogId = TW.catalogId");
		return queryString.toString();
	}
	
	private String geHourlyQuery(String attribution, String attributionCode) {
		StringBuffer queryString = new StringBuffer();
		queryString.append("SELECT   V.reportDate 			AS REPORT_DATE "
						+ "         ,V.reportHour 			AS REPORT_HOUR "
						+ "         ,TW.webId  				AS WEB_ID "
						+ " 		,V.mediaId  			AS MEDIA_ID "
						+ " 		,V.channelId			AS CHANNEL_ID "
						+ " 		,V.campaignId			AS CAMPAIGN_ID "
						+ " 		,V.linkId  				AS LINK_ID "
						+ " 		,'CTC002' 				AS CONVERSION_TYPE_CODE "
						+ " 	  	,'"+attributionCode+"'  AS ATTRIBUTION_CODE "
						+ " 		,V.purchased            AS PURCHASE  "
						+ " 		,V.viewContent          AS VIEW_CONTENT "
						+ " 		,V.addToCart            AS ADD_TO_CART "
						+ "         ,V.revenue              AS REVENUE  "
						+ "         ,V.uniquePurchased 	    AS UNIQUE_PURCHASE "
						+ " 		,V.uniqueViewContent    AS UNIQUE_VIEW_CONTENT "
						+ " 		,V.uniqueAddToCart      AS UNIQUE_ADD_TO_CART "
						+ "  "
						+ " FROM (SELECT TT3.reportDate "
						+ "             ,TT3.reporthour "
						+ "             ,TT3.catalogId		 "
						+ "             ,TT3.mediaId "
						+ "     		,TT3.channelId "
						+ "     		,TT3.campaignId "
						+ "     		,TT3.linkId "
						+ "             ,SUM(TT3.value)                     AS revenue "
						+ "             ,COUNT(TT3.purchasedId)             AS purchased "
						+ "             ,SUM(TT3.viewContent)               AS viewContent "
						+ "             ,SUM(TT3.addToCart)                 AS addToCart "
						+ "             ,COUNT(DISTINCT TT3.purchasedpCid)  AS uniquePurchased "
						+ "             ,SUM(TT3.uniqueViewContent)         AS uniqueViewContent "
						+ "             ,SUM(TT3.uniqueAddToCart)           AS uniqueAddToCart "
						+ "      "
						+ "     FROM (  SELECT   TT1.reportDate "
						+ "                     ,TT1.reportHour "
						+ "                     ,TT1.catalogId "
						+ "                     ,TT1.mediaId "
						+ "                     ,TT1.channelId "
						+ "                     ,TT1.campaignId "
						+ "                     ,TT1.linkId "
						+ "                     ,TT1.value "
						+ "                     ,TT1.purchasedpCid "
						+ "                     ,TT1.purchasedId "
						+ "                     ,SUM(CASE WHEN TT2.eventType = 'VIEW_CONTENT' THEN 1 ELSE 0 END)    AS viewContent "
						+ "                     ,SUM(CASE WHEN TT2.eventType = 'ADD_TO_CART'  THEN 1 ELSE 0 END)    AS addToCart "
						+ "                     ,COUNT(DISTINCT (CASE WHEN TT2.eventType = 'VIEW_CONTENT'  THEN TT2.pCid END))      AS uniqueViewContent "
						+ "                     ,COUNT(DISTINCT (CASE WHEN TT2.eventType = 'ADD_TO_CART'   THEN TT2.pCid END))      AS uniqueAddToCart "
						+ "                      "
						+ "             FROM ( SELECT    DATE_FORMAT(CAST(UNIX_TIMESTAMP(T1.purchasedDateTime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'yyyy-MM-dd')	AS reportDate "
						+ "                             ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(T1.purchasedDateTime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')	        AS reportHour "
						+ "                             ,T1.catalogId "
						+ "                             ,T1.mediaId "
						+ "                             ,T1.channelId "
						+ "                             ,T1.campaignId "
						+ "                             ,T1.linkId "
						+ "                             ,T1.contentIds "
						+ "                             ,T1.value "
						+ "                             ,T1.purchasedDateTime "
						+ "                             ,T1.purchasedpCid "
						+ "                             ,T1.purchasedId "
						+ "                             ,RANK() OVER (PARTITION BY T1.catalogId, T1.contentIds, T1.purchasedpCid, T1.purchasedDateTime  ORDER BY  T1.conversionDateTime DESC, T1.conversionId DESC ) AS pt "
						+ "                     FROM    (SELECT  A.catalogId "
						+ "                                     ,NVL(B.mediaId, '')     AS mediaId "
						+ "                                     ,NVL(B.channelId, '')	AS channelId "
						+ "                                     ,NVL(B.campaignId, '')  AS campaignId "
						+ "                                     ,NVL(B.linkId , '')     AS linkId "
						+ "                                     ,A.contentIds "
						+ "                                     ,A.value "
						+ "                                     ,A.logDateTime  AS purchasedDateTime "
						+ "                                     ,B.logDateTime  AS conversionDateTime "
						+ "                                     ,A.pCid         AS purchasedpCid "
						+ "                                     ,B.pCid         AS conversionpCid "
						+ "                                     ,A._id          AS purchasedId "
						+ "                                     ,B.ids			AS conversionId "
						+ "                             FROM    T_PIXEL_PURCHASED A "
						+ "                                     LEFT OUTER JOIN V_PIXEL_CONVERSION_RAWDATA  B "
						+ "                                     ON  A.catalogId  = B.catalogId "
						+ "                                     AND A.pCid       = B.pCid  "
						+ "                                     AND CAST(B.logDateTime AS TIMESTAMP) BETWEEN ADD_HOURS(CAST(A.logDateTime AS TIMESTAMP), -"+attribution+") AND CAST(A.logDateTime AS TIMESTAMP)  "
						+ "                             	  WHERE   A.logDateTime LIKE '"+getReportDateHour()+"%' "
						+ "                             ) T1 "
						+ "                     ) TT1  "
						+ "                 INNER JOIN "
						+ "                 (SELECT  A.eventType "
						+ "                         ,A.pCid "
						+ "                         ,A.catalogId "
						+ "                         ,A.contentIds  "
						+ "                         ,A.logDateTime "
						+ "                 FROM (SELECT     'VIEW_CONTENT'             AS eventType "
						+ "                                 ,viewContent.p_cid          AS pCid "
						+ "                                 ,viewContent.catalog_id     AS catalogId "
						+ "                                 ,viewContent.content_ids    AS contentIds "
						+ "                                 ,logDateTime "
						+ "                      FROM       V_PIXEL_VIEW_CONTENT "
						+ "                          "
						+ "                      UNION  "
						+ "                          "
						+ "                      SELECT      'ADD_TO_CART'              AS eventType "
						+ "                                 ,addToCart.p_cid            AS pCid "
						+ "                                 ,addToCart.catalog_id       AS catalogId "
						+ "                                 ,addToCart.content_ids      AS contentIds "
						+ "                                 ,logDateTime "
						+ "                      FROM    V_PIXEL_ADD_TO_CART) A "
						+ "                 ) TT2 "
						+ "                 ON  TT1.catalogId = TT2.catalogId  "
						+ "                     AND TT1.purchasedpCid = TT2.pCid "
						+ "                     AND CAST(TT2.logDateTime AS TIMESTAMP) BETWEEN ADD_HOURS(CAST(TT1.purchasedDateTime AS TIMESTAMP), -"+attribution+") AND CAST(TT1.purchasedDateTime AS TIMESTAMP) "
						+ "              WHERE TT1.pt = 1 "
						+ "              GROUP BY TT1.reportDate, TT1.reportHour, TT1.catalogId, TT1.mediaId, TT1.channelId, TT1.campaignId,TT1.linkId,  TT1.value,TT1.purchasedpCid, TT1.purchasedId "
						+ "         ) TT3  "
						+ "     GROUP BY TT3.reportDate, TT3.reportHour, TT3.catalogId, TT3.mediaId, TT3.channelId, TT3.campaignId, TT3.linkId "
						+ " ) V "
						+ " INNER JOIN T_TRK_WEB  TW  ON	V.catalogId = TW.catalogId "
						+ " ");
				return queryString.toString();
	}
	
	/**
	 * @param reportType
	 * @return
	 */
//	private String getQuery(String reportType, String attribution, String attributionCode) {
//		
//		StringBuffer queryString = new StringBuffer();
//		queryString.append("SELECT   	 A.reportDate 						AS REPORT_DATE ");
//		
//		if(reportType.equals(HOURLY)) {
//			queryString.append(" 		,A.reportHour 						AS REPORT_HOUR ");
//		}
//		queryString.append(	  "        	,B.webId  							AS WEB_ID"
//				    		+ "        	,A.mediaId  						AS MEDIA_ID"
//				    		+ "        	,A.channelId						AS CHANNEL_ID"
//				    		+ "        	,A.campaignId						AS CAMPAIGN_ID"
//				    	    + "        	,A.linkId  							AS LINK_ID"
//							+ "			,'CTC002' 							AS CONVERSION_TYPE_CODE "
//				    	    + "        	,'"+attributionCode+"'              AS ATTRIBUTION_CODE ");
//	    if(reportType.equals(HOURLY)) {
//			queryString.append( "       ,SUM(A.value)                    	AS REVENUE "
//							+ "        	,COUNT(A.purchasedId)             	AS PURCHASE " 
//				    	    + "        	,SUM(A.viewContent)               	AS VIEW_CONTENT "
//				    	    + "        	,SUM(A.addToCart)                 	AS ADD_TO_CART ");
//		}else {
//			queryString.append( "       ,SUM(A.value)                    	AS TOTAL_REVENUE " 
//							+ "       	,COUNT(A.purchasedId)             	AS TRANSACTION ");
//		}		    	    
//	    queryString.append(	  "        	,COUNT(DISTINCT A.purchasedpCid)  	AS UNIQUE_PURCHASE "
//				    	    + "        	,SUM(A.uniqueViewContent)         	AS UNIQUE_VIEW_CONTENT "
//				    	    + "        	,SUM(A.uniqueAddToCart)           	AS UNIQUE_ADD_TO_CART "
//				    	    + " "
//				    	    + "  FROM (  SELECT TT1.reportDate ");
//
//		if(reportType.equals(HOURLY)) {
//			queryString.append( "              ,TT1.reportHour ");
//		}				    	    
//		queryString.append(	"				   ,TT1.catalogId "
//				    	    + "                ,NVL(TT1.mediaId, '')  	AS mediaId"
//				    	    + "                ,NVL(TT1.channelId, '') 	AS channelId "
//				    	    + "                ,NVL(TT1.campaignId, '') AS campaignId "
//				    	    + "                ,TT1.linkId "
//				    	    + "                ,TT1.value "
//				    	    + "                ,TT1.purchasedpCid "
//				    	    + "                ,TT1.purchasedId "
//				    	    + "                ,SUM(CASE WHEN TT2.eventType = 'VIEW_CONTENT' THEN 1 ELSE 0 END)    AS viewContent "
//				    	    + "                ,SUM(CASE WHEN TT2.eventType = 'ADD_TO_CART'  THEN 1 ELSE 0 END)    AS addToCart "
//				    	    + "                ,COUNT(DISTINCT (CASE WHEN TT2.eventType = 'VIEW_CONTENT'  THEN TT2.pCid END))      AS uniqueViewContent "
//				    	    + "                ,COUNT(DISTINCT (CASE WHEN TT2.eventType = 'ADD_TO_CART'   THEN TT2.pCid END))      AS uniqueAddToCart "
//				    	    + "        FROM ( SELECT    DATE_FORMAT(CAST(UNIX_TIMESTAMP(T1.purchasedDateTime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'yyyy-MM-dd')	AS reportDate "
//				    	    + "						   ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(T1.purchasedDateTime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')	        AS reportHour"
//				    	    + "						   ,T1.catalogId "
//				    	    + "                        ,T1.mediaId "
//				    	    + "                        ,T1.channelId "
//				    	    + "                        ,T1.campaignId "
//				    	    + "                        ,T1.linkId "
//				    	    + "                        ,T1.contentIds "
//				    	    + "                        ,T1.value "
//				    	    + "                        ,T1.purchasedDateTime "
//				    	    + "                        ,T1.purchasedpCid "
//				    	    + "                        ,T1.purchasedId "
//				    	    + "                        ,RANK() OVER (PARTITION BY T1.catalogId, T1.contentIds, T1.purchasedpCid, T1.purchasedDateTime  ORDER BY  T1.conversionDateTime DESC, T1.conversionId DESC ) AS pt "
//				    	    + "                FROM    (SELECT  A.catalogId "
//				    	    + "                                ,B.mediaId "
//				    	    + "                                ,B.channelId "
//				    	    + "                                ,B.campaignId "
//				    	    + "                                ,B.linkId "
//				    	    + "                                ,A.contentIds "
//				    	    + "                                ,A.value "
//				    	    + "                                ,A.logDateTime  AS purchasedDateTime "
//				    	    + "                                ,B.logDateTime  AS conversionDateTime "
//				    	    + "                                ,A.pCid         AS purchasedpCid "
//				    	    + "                                ,B.pCid         AS conversionpCid "
//				    	    + "                                ,A._id          AS purchasedId "
//				    	    + "                                ,B.ids			AS conversionId "
//				    	    + "                        FROM    T_PIXEL_PURCHASED A "
//				    	    + "                                LEFT OUTER JOIN V_CONVERSION_RAWDATA  B "
//				    	    + "                                ON  A.catalogId  = B.catalogId "
//				    	    + "                                AND A.pCid       = B.pCid  "
//				    	    + "                                AND A.contentIds = B.contentIds "
//				    	    + "                                AND CAST(B.logDateTime AS TIMESTAMP) BETWEEN ADD_HOURS(CAST(A.logDateTime AS TIMESTAMP), -"+attribution+") AND CAST(A.logDateTime AS TIMESTAMP) " );
//		if(reportType.equals(HOURLY)) {
//			queryString.append( "             				   WHERE A.logDateTime LIKE '"+getReportDateHour()+"%' ");
//		}		    	    
//		queryString.append(	" 	                       ) T1 "
//				    	    + "                ) TT1  "
//				    	    + "            LEFT OUTER JOIN "
//				    	    + "            (SELECT  A.eventType "
//				    	    + "                    ,A.pCid "
//				    	    + "                    ,A.catalogId "
//				    	    + "                    ,A.contentIds  "
//				    	    + "                    ,A.logDateTime "
//				    	    + "            FROM (SELECT     'VIEW_CONTENT'             AS eventType "
//				    	    + "                            ,viewContent.p_cid          AS pCid "
//				    	    + "                            ,viewContent.catalog_id     AS catalogId "
//				    	    + "                            ,viewContent.content_ids    AS contentIds "
//				    	    + "                            ,logDateTime "
//				    	    + "                 FROM       V_VIEW_CONTENT "
//				    	    + "                     "
//				    	    + "                 UNION  "
//				    	    + "                     "				    	    + "                 SELECT      'ADD_TO_CART'              AS eventType "
//				    	    + "                            ,addToCart.p_cid            AS pCid "
//				    	    + "                            ,addToCart.catalog_id       AS catalogId "
//				    	    + "                            ,addToCart.content_ids      AS contentIds "
//				    	    + "                            ,logDateTime "
//				    	    + "                 FROM    V_ADD_TO_CART) A "
//				    	    + "            ) TT2 "
//				    	    + "            ON  TT1.catalogId = TT2.catalogId  "
//				    	    + "                AND TT1.contentIds = TT2.contentIds "
//				    	    + "                AND TT1.purchasedpCid = TT2.pCid "
//				    	    + "                AND CAST(TT2.logDateTime AS TIMESTAMP) BETWEEN ADD_HOURS(CAST(TT1.purchasedDateTime AS TIMESTAMP), -"+attribution+") AND CAST(TT1.purchasedDateTime AS TIMESTAMP) "
//				    	    + "         WHERE TT1.pt = 1 " );
//		
//		if(reportType.equals(HOURLY)) {
//			queryString.append("         GROUP BY TT1.reportDate, TT1.reportHour,TT1.catalogId, TT1.mediaId, TT1.channelId, TT1.campaignId,TT1.linkId, TT1.value,TT1.purchasedpCid, TT1.purchasedId ");
//		}else {
//			queryString.append("         GROUP BY TT1.reportDate, TT1.catalogId, TT1.mediaId, TT1.channelId, TT1.campaignId,TT1.linkId, TT1.value,TT1.purchasedpCid, TT1.purchasedId ");
//		}		    	
//				    	    
//		queryString.append(	"				    	       	) A " 
//				    	    + "		INNER JOIN T_TRK_WEB B  	ON	A.catalogId = B.catalogId " );
//		
//	    if(reportType.equals(HOURLY)) {
//			queryString.append("    GROUP BY A.reportDate, A.reportHour, B.webId, A.mediaId, A.channelId, A.campaignId, A.linkId ");
//		}else {
//			queryString.append("    GROUP BY A.reportDate, B.webId, A.mediaId, A.channelId, A.campaignId, A.linkId ");
//		}
//		
//		return queryString.toString();
//		
//	}
}
