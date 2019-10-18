package com.charmhcs.tracking.analytics.core.report.trackinglink;

import com.charmhcs.tracking.analytics.core.dataset.event.ConversionRawData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.charmhcs.tracking.analytics.common.AbstractDataReport;
import com.charmhcs.tracking.analytics.common.factory.SparkJdbcConnectionFactory;
import com.charmhcs.tracking.analytics.common.map.ExceptionMap;
import com.charmhcs.tracking.analytics.common.map.MongodbMap;
import com.charmhcs.tracking.analytics.common.util.LogStack;
import com.charmhcs.tracking.analytics.core.config.RawDataSQLTempView;
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
* 2018. 9. 15.   cshwang   Initial Release
*
*
************************************************************************************************/
public class TrackingLinkReport extends AbstractDataReport {

	public TrackingLinkReport(){
		super();
	}
	
	/**
	 * 생성
	 * @param runType
	 */
	public TrackingLinkReport(String runType) {
		super(runType);
	}

	/**
	 * 생성
	 * @param runType
	 * @param reportType
	 */
	public TrackingLinkReport(String runType, String reportType) {
		super(runType, reportType);
	}
	
	/* (non-Javadoc)
	 * @see com.dmcmedia.tracking.analytics.common.DataReport#reportsDataSet(org.apache.spark.sql.SparkSession)
	 */
	@Override
	public boolean reportsDataSet(SparkSession sparkSession) {
		
		Dataset<Row> resultHourlyDS 	= null;
		Dataset<Row> resultDailyDS 		= null;
		String targetHourlyTableName 	= "tracking.TS_TRK_WEB_LINK_HOURLY";
		String targetDailyTableName 	= "tracking.TS_TRK_WEB_LINK_DAILY";
		
		if(super.isValid() == false){
			return false;
		}
		try{
			if(EACH.equals(this.getRunType().name())){
				RawDataSQLTempView rawDataSQLTempView = new RawDataSQLTempView(getReportDate());
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
			}
			
			MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", getProcessedDataBase())
					.withOption("collection", this.getReportDate() + MongodbMap.COLLECTION_PIXEL_CONVERSION_RAWDATA), ConversionRawData.class)
					.where("logDateTime LIKE '"+getReportDateHour()+"%'")
					.createOrReplaceTempView("T_PIXEL_CONVERSION_RAWDATA");

			switch(getReportType().name()) {
				case HOURLY :
					resultHourlyDS = sparkSession.sql(getHourlyQuery());
					resultHourlyDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), targetHourlyTableName, SparkJdbcConnectionFactory.getDefaultJdbcOptions());
					break;
				case DAILY :
					resultDailyDS = sparkSession.sql(getDailyQuery());
					resultDailyDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), targetDailyTableName, SparkJdbcConnectionFactory.getDefaultJdbcOptions());
					break;
				case ALL :
					resultHourlyDS = sparkSession.sql(getHourlyQuery());
					resultHourlyDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), targetHourlyTableName, SparkJdbcConnectionFactory.getDefaultJdbcOptions());
					resultDailyDS = sparkSession.sql(getDailyQuery());
					resultDailyDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), targetDailyTableName, SparkJdbcConnectionFactory.getDefaultJdbcOptions());
					break;
				default :
					new Exception(ExceptionMap.INVALID_DATA_FORMAT_EXCEPTION);
					return false;		   
			}
		}catch(Exception e){
			LogStack.report.error(ExceptionMap.REPORT_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.report.error(e);
		}
		return true;
	}

	/**
	 * @return
	 */
	private String getDailyQuery() {		
		StringBuffer queryString = new StringBuffer();
		queryString.append("WITH V_PIXEL_CONVERSION_RAWDATA AS "
						+ "  (SELECT  DATE_FORMAT(CAST(UNIX_TIMESTAMP(logDateTime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'yyyy-MM-dd')	AS reportDate "
						+ "          ,pixelId"
						+ "          ,mediaId "
						+ "          ,productsId"
						+ "          ,CASE WHEN TRIM(channelId) = ''  OR channelId IS NULL THEN '' ELSE channelId END   AS channelId "
						+ "          ,campaignId "
						+ "          ,linkId "
						+ "          ,_id "
						+ "          ,pCid "
						+ "          ,utmContent"
						+ "          ,conversionRawdataTypeCode"
						+ "  FROM    T_PIXEL_CONVERSION_RAWDATA)"
						+ "  "
						+ "  SELECT   C.reportDate      AS REPORT_DATE "
						+ "          ,TW.webId		    AS WEB_ID  "
						+ "          ,C.mediaId         AS MEDIA_ID "
						+ "          ,C.channelId       AS CHANNEL_ID "
						+ "          ,C.productsId		AS PRODUCTS_ID "
						+ "          ,C.campaignId      AS CAMPAIGN_ID "
						+ "          ,C.linkId          AS LINK_ID "
						+ "          ,0                 AS IMPRESSION "
						+ "          ,0				    AS REACH "
						+ "          ,C.click           AS CLICK "
						+ "          ,C.uniqueClick     AS UNIQUE_CLICK "
						+ "          ,IFNULL(D.clickAndPageView, 0 )         AS CLICK_AND_PAGEVIEW"
						+ "          ,IFNULL(D.uniqueClickAndPageView, 0)    AS UNIQUE_CLICK_AND_PAGEVIEW"
						+ "  FROM (SELECT reportDate "
						+ "              ,pixelId"
						+ "              ,mediaId "
						+ "              ,productsId"
						+ "              ,channelId "
						+ "              ,campaignId "
						+ "              ,linkId "
						+ "              ,COUNT(pCid)   		    AS click  "
						+ "              ,COUNT(DISTINCT pCid) AS uniqueClick"
						+ "      FROM  V_PIXEL_CONVERSION_RAWDATA "
						+ "      WHERE conversionRawdataTypeCode != 'CRT001'"
						+ "      GROUP BY reportDate, pixelId,  mediaId, productsId, channelId, campaignId, linkId ) C"
						+ "  LEFT OUTER JOIN  "
						+ "      (SELECT  B.reportDate "
						+ "              ,B.pixelId"
						+ "              ,B.mediaId "
						+ "              ,B.productsId"
						+ "              ,B.channelId "
						+ "              ,B.campaignId "
						+ "              ,B.linkId "
						+ "              ,COUNT(B.pCid)   		  AS clickAndPageView  "
						+ "              ,COUNT(DISTINCT B.pCid)   AS uniqueClickAndPageView"
						+ "      FROM    (SELECT * FROM V_PIXEL_CONVERSION_RAWDATA WHERE conversionRawdataTypeCode = 'CRT001') A"
						+ "              INNER JOIN "
						+ "              (SELECT * FROM V_PIXEL_CONVERSION_RAWDATA WHERE conversionRawdataTypeCode = 'CRT006') B"
						+ "              ON A.reportDate = B.reportDate AND A.pixelId = B.pixelId"
						+ "              AND A.mediaId = B.mediaId AND A.productsId = B.productsId AND A.channelId = B.channelId "
						+ "              AND A.campaignId = B.campaignId AND A.linkId = B.linkId AND A.utmContent = B.utmContent "
						+ "      GROUP BY B.reportDate, B.pixelId, B.mediaId, B.productsId, B.channelId, B.campaignId, B.linkId) D"
						+ "  ON  C.reportDate = D.reportDate AND C.pixelId = D.pixelId "
						+ "  AND C.mediaId = C.mediaId AND C.productsId = D.productsId AND C.channelId = D.channelId "
						+ "  AND C.campaignId = D.campaignId AND C.linkId = D.linkId"
						+ "  INNER JOIN T_TRK_WEB  TW  ON C.pixelId = TW.pixelId ");
		return queryString.toString();
	}
		
	/**
	 * @return
	 */
	private String getHourlyQuery() {		
		StringBuffer queryString = new StringBuffer();
		queryString.append(" WITH V_PIXEL_CONVERSION_RAWDATA AS "
						+ "  (SELECT  DATE_FORMAT(CAST(UNIX_TIMESTAMP(logDateTime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'yyyy-MM-dd')	AS reportDate "
						+ "          ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logDateTime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')	        AS reportHour "
						+ "          ,pixelId"
						+ "          ,mediaId "
						+ "          ,productsId"
						+ "          ,CASE WHEN TRIM(channelId) = ''  OR channelId IS NULL THEN '' ELSE channelId END   AS channelId "
						+ "          ,campaignId "
						+ "          ,linkId "
						+ "          ,_id "
						+ "          ,pCid "
						+ "          ,utmContent"
						+ "          ,conversionRawdataTypeCode"
						+ "  FROM    T_PIXEL_CONVERSION_RAWDATA)"
						+ "  "
						+ "  SELECT   C.reportDate      AS REPORT_DATE "
						+ "          ,C.reportHour      AS REPORT_HOUR"
						+ "          ,TW.webId		    AS WEB_ID  "
						+ "          ,C.mediaId         AS MEDIA_ID "
						+ "          ,C.channelId       AS CHANNEL_ID "
						+ "          ,C.productsId		AS PRODUCTS_ID "
						+ "          ,C.campaignId      AS CAMPAIGN_ID "
						+ "          ,C.linkId          AS LINK_ID "
						+ "          ,0                 AS IMPRESSION "
						+ "          ,0				    AS REACH "
						+ "          ,C.click           AS CLICK "
						+ "          ,C.uniqueClick     AS UNIQUE_CLICK "
						+ "          ,IFNULL(D.clickAndPageView, 0 )         AS CLICK_AND_PAGEVIEW"
						+ "          ,IFNULL(D.uniqueClickAndPageView, 0)    AS UNIQUE_CLICK_AND_PAGEVIEW"
						+ "  FROM (SELECT reportDate"
						+ "              ,reportHour"
						+ "              ,pixelId"
						+ "              ,mediaId "
						+ "              ,productsId"
						+ "              ,channelId "
						+ "              ,campaignId "
						+ "              ,linkId "
						+ "              ,COUNT(pCid)   		    AS click  "
						+ "              ,COUNT(DISTINCT pCid) AS uniqueClick"
						+ "      FROM  V_PIXEL_CONVERSION_RAWDATA "
						+ "      WHERE conversionRawdataTypeCode != 'CRT001'"
						+ "      GROUP BY reportDate, reportHour, pixelId, mediaId, productsId, channelId, campaignId, linkId ) C"
						+ "  LEFT OUTER JOIN  "
						+ "      (SELECT  B.reportDate"
						+ "              ,B.reportHour"
						+ "              ,B.pixelId"
						+ "              ,B.mediaId "
						+ "              ,B.productsId"
						+ "              ,B.channelId "
						+ "              ,B.campaignId "
						+ "              ,B.linkId "
						+ "              ,COUNT(B.pCid)   		  AS clickAndPageView  "
						+ "              ,COUNT(DISTINCT B.pCid)   AS uniqueClickAndPageView"
						+ "      FROM    (SELECT * FROM V_PIXEL_CONVERSION_RAWDATA WHERE conversionRawdataTypeCode = 'CRT001') A"
						+ "              INNER JOIN "
						+ "              (SELECT * FROM V_PIXEL_CONVERSION_RAWDATA WHERE conversionRawdataTypeCode = 'CRT006') B"
						+ "              ON A.reportDate = B.reportDate AND A.pixelId = B.pixelId "
						+ "              AND A.mediaId = B.mediaId AND A.productsId = B.productsId AND A.channelId = B.channelId "
						+ "              AND A.campaignId = B.campaignId AND A.linkId = B.linkId AND A.utmContent = B.utmContent "
						+ "      GROUP BY B.reportDate, B.reportHour, B.pixelId, B.mediaId, B.productsId, B.channelId, B.campaignId, B.linkId) D"
						+ "  ON  C.reportDate = D.reportDate AND C.pixelId = D.pixelId "
						+ "  AND C.mediaId = C.mediaId AND C.productsId = D.productsId AND C.channelId = D.channelId "
						+ "  AND C.campaignId = D.campaignId AND C.linkId = D.linkId"
						+ "  INNER JOIN T_TRK_WEB  TW  ON C.pixelId = TW.pixelId ");
		return queryString.toString();
	}
}