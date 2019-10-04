package com.dmcmedia.tracking.analytics.core.report.event;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.AbstractDataMinutelyAnalysis;
import com.dmcmedia.tracking.analytics.common.factory.SparkJdbcConnectionFactory;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.map.MongodbMap;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.dataset.TrackingPixelDataSet;
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
* 2018-12-29   cshwang  Initial Release
*
*
************************************************************************************************/
public class EventRealTime extends AbstractDataMinutelyAnalysis{

	/**
	 * 생성자 
	 * 	 
	 * */
	public EventRealTime(){
		super();
	}
	
	/**
	 * 생성자 
	 * 	 
	 * */
	public EventRealTime(int reportMinuteRange){
		super(reportMinuteRange);
	}
	
	public boolean analyzeDataSet(SparkSession sparkSession) {
		
		Dataset<Row> resultDS 	= null;
		String targetRdbTableName = "tracking.TS_TRK_WEB_EVENT_REALTIME";
		if(super.isValid() == false){
			return false;
		}
		
		try{
//			if(EACH.equals(this.getRunType().name())){
//				RawDataSQLTempView rawDataSQLTempView = new RawDataSQLTempView(getAnalysisDate());
//				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
//			}
			
			LogStack.report.info(getAnalysisDate());
			LogStack.report.info(getAnalysisDateHourMinute());
			LogStack.report.info(getCurrentAnalysisDateHourMinute());
			
			MongoSpark.load(sparkSession, ReadConfig.create(sparkSession)
					.withOption("database", getRawDataBase())
					.withOption("collection", getAnalysisDate() + MongodbMap.COLLECTION_PIXEL_PAGE_VIEW)
					,TrackingPixelDataSet.class).where("logDateTime BETWEEN '"+getAnalysisDateHourMinute()+"' AND '"+ getCurrentAnalysisDateHourMinute()+ "'").createOrReplaceTempView("T_PIXEL_PAGE_VIEW");
			
			resultDS = sparkSession.sql("SELECT  '"+getCurrentAnalysisDateHourMinute()+"'      AS REPORT_DATETIME "
									+ " 		,TW.webId               AS WEB_ID "
									+ " 		,A.deviceTypeCode       AS DEVICE_TYPE_CODE "
									+ " 		,COUNT(DISTINCT A.pCid) AS ACTIVE_USER "
									+ " 		,COUNT(A.eventCount)    AS SESSION "
									+ " FROM ( 	SELECT	 reportDate "
									+ " 			    ,reportHour "
									+ " 			    ,half "
									+ " 				,pixelId "
									+ " 				,deviceTypeCode "
									+ " 				,pCid "
									+ " 				,COUNT(pCid)    AS eventCount "
									+ " 		 FROM (SELECT	 trackingEventCode "
									+ " 						,pageView.pixel_id	    AS  pixelId "
									+ " 					    ,pageView.p_cid		    AS  pCid "
									+ " 					    ,logDateTime "
									+ " 					    ,(CASE WHEN pageView.user_agent LIKE '%iPhone%' OR pageView.user_agent LIKE '%Android%' OR pageView.user_agent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode "
									+ " 						,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')      AS reportDate "
									+ " 						,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS reportHour "
									+ " 						,FLOOR(DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'mm')  / 30) AS half "
									+ " 				FROM	T_PIXEL_PAGE_VIEW "
									+ " 			) "
									+ " 		 GROUP BY reportDate, reportHour, half, pixelId, deviceTypeCode, pCid "
									+ " 		 ) A "
									+ " INNER JOIN T_TRK_WEB TW  ON	A.pixelId = TW.pixelId "
									+ " GROUP BY A.reportDate, TW.webId, A.deviceTypeCode ");
			resultDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), targetRdbTableName, SparkJdbcConnectionFactory.getDefaultJdbcOptions());
		}catch(Exception e){
			LogStack.report.error(ExceptionMap.REPORT_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.report.error(e);
		}finally {

		}
		return true;
	}
}
