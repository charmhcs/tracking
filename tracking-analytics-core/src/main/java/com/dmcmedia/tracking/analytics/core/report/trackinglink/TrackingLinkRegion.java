package com.dmcmedia.tracking.analytics.core.report.trackinglink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.AbstractDataReport;
import com.dmcmedia.tracking.analytics.common.factory.SparkJdbcConnectionFactory;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.map.MongodbMap;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.config.RawDataSQLTempView;
import com.dmcmedia.tracking.analytics.core.dataset.entity.web.Click;
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
* 2018-11-26  cshwang   Initial Release
*
*
************************************************************************************************/
public class TrackingLinkRegion extends AbstractDataReport {
	public TrackingLinkRegion(){
		super(EACH, HOURLY);
	}
	
	/**
	 * 생성
	 * @param runType
	 */
	public TrackingLinkRegion(String runType) {
		super(runType, HOURLY);
	}
	
	public boolean reportsDataSet(SparkSession sparkSession) {
		
		Dataset<Row> resultDS = null;
		String targetRdbTableName = "TS_TRK_WEB_LINK_REGION";
		String trkRegionMobileIp = "(SELECT NETWORK_TYPE  AS networkType , COMPANY AS company, IP_BAND AS ipBand   FROM tracking.ZS_TRK_REGION_MOBILE_IP WHERE USE_YN = 'Y' AND DEL_YN = 'N') AS ZS_TRK_REGION_MOBILE_IP ";
		
		if(super.isValid() == false){
			return false;
		}
		
		try{
			if(EACH.equals(this.getRunType().name())){
				RawDataSQLTempView rawDataSQLTempView = new RawDataSQLTempView(getReportDate());
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
			}
			LogStack.report.info("getReportDate :" + getReportDate());
			LogStack.report.info("getReportDateHour :" + getReportDateHour());
			
			SparkJdbcConnectionFactory.getSelectJdbcDfLoad(sparkSession, trkRegionMobileIp).persist().createOrReplaceTempView("ZS_TRK_REGION_MOBILE_IP");
			MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", getRawDataBase())
					.withOption("collection", this.getReportDate() + MongodbMap.COLLECTION_WEB_CLICK), Click.class)
					.where("logDateTime LIKE '"+getReportDateHour()+"%'")
					.createOrReplaceTempView("T_WEB_CLICK");
			
			
			/*resultDS = sparkSession.sql("SELECT  reportDate              AS REPORT_DATE "
									+ "         ,reportHour             AS REPORT_HOUR "
									+ "         ,webId                  AS WEB_ID "
									+ "         ,mediaId                AS MEDIA_ID "
									+ "         ,channelId              AS CHANNEL_ID "
									+ "         ,productsId             AS PRODUCTS_ID"
									+ "         ,campaignId             AS CAMPAIGN_ID "
									+ "         ,linkId                 AS LINK_ID "
									+ "         ,regionSid              AS REGION_SID "
									+ "         ,COUNT(pCid)            AS TOTAL_COUNT "
									+ " 		,COUNT(DISTINCT pCid)   AS AUDIENCE_SIZE"
									+ " FROM (SELECT DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')      AS reportDate "
									+ "             ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS reportHour "
									+ "             ,webId "
									+ "             ,mediaId "
									+ "             ,channelId "
									+ "             ,productsId"
									+ "             ,campaignId "
									+ "             ,linkId "
									+ "             ,regionSid "
									+ "             ,pCid "
									+ "             ,httpRemoteAddress"
									+ "     FROM T_WEB_CLICK) A "
									+ " WHERE NOT EXISTS(   SELECT  0 "
									+ "                     FROM    ZS_TRK_REGION_MOBILE_IP B "
									+ "                     WHERE   A.httpRemoteAddress LIKE CONCAT(B.ipBand, '%'))"
									+ " AND A.regionSid NOT LIKE '99%'"
									+ " GROUP BY reportDate, reportHour, webId, mediaId, channelId, productsId, campaignId, linkId, regionSid "
									+ "");*/
			
			
			resultDS = sparkSession.sql("SELECT  reportDate              AS REPORT_DATE "
									+ "         ,reportHour             AS REPORT_HOUR "
									+ "         ,webId                  AS WEB_ID "
									+ "         ,mediaId                AS MEDIA_ID "
									+ "         ,channelId              AS CHANNEL_ID "
									+ "         ,productsId             AS PRODUCTS_ID"
									+ "         ,campaignId             AS CAMPAIGN_ID "
									+ "         ,linkId                 AS LINK_ID "
									+ "			,CASE WHEN company = 'SKT' THEN 5 " 
									+ "				WHEN company = 'KT' THEN 6 " 
									+ "				WHEN company = 'LGU+' THEN 7 " 
									+ "				ELSE regionSid"
									+ "			 END AS REGION_SID"
									+ "         ,COUNT(pCid)            AS TOTAL_COUNT "
									+ " 		,COUNT(DISTINCT pCid)   AS AUDIENCE_SIZE"
									+ " FROM (SELECT DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')      AS reportDate "
									+ "             ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS reportHour "
									+ "             ,webId "
									+ "             ,mediaId "
									+ "             ,channelId "
									+ "             ,productsId "
									+ "             ,campaignId "
									+ "             ,linkId "
									+ "             ,regionSid "
									+ "             ,pCid "
									+ "             ,httpRemoteAddress"
									+ "     FROM T_WEB_CLICK) A "
									+ " LEFT JOIN ZS_TRK_REGION_MOBILE_IP B on A.httpRemoteAddress LIKE CONCAT(B.ipBand, '%')"
									+ " WHERE A.regionSid NOT LIKE '99%'"
									+ " GROUP BY reportDate, reportHour, webId, mediaId, channelId, productsId, campaignId, linkId, regionSid, company ");
			resultDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), targetRdbTableName, SparkJdbcConnectionFactory.getDefaultJdbcOptions());
			
		}catch(Exception e){
			LogStack.report.error(ExceptionMap.REPORT_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.report.error(e);
		}finally {
	
		}
		return true;
	}
}
