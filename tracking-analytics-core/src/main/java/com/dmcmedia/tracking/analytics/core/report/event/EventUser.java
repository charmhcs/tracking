package com.dmcmedia.tracking.analytics.core.report.event;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.AbstractDataReport;
import com.dmcmedia.tracking.analytics.common.factory.SparkJdbcConnectionFactory;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.config.RawDataSQLTempView;

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
* 2018. 10. 7.   cshwang   Initial Release
*
*
************************************************************************************************/
public class EventUser extends AbstractDataReport  {

	/**
	 * 생성자 
	 */
	public EventUser(){
		super();
	}
	
	/**
	 * 생성자
	 * @param runType
	 */
	public EventUser(String runType){
		super(runType);
	}
	
	/**
	 * 생성자
	 * @param runType
	 * @param reportType
	 */
	public EventUser(String runType, String reportType){
		super(runType, reportType);
	}
	
	public boolean reportsDataSet(SparkSession sparkSession) {
		
		Dataset<Row> resultHourlyDS 	= null;
		Dataset<Row> resultDailyDS 		= null;
		String targetHourlyTableName 	= "tracking.TS_TRK_WEB_EVENT_HOURLY";
		String targetDailyTableName 	= "tracking.TS_TRK_WEB_EVENT_DAILY";
			
		if(super.isValid() == false){
			return false;
		}
		try{
			if(EACH.equals(this.getRunType().name())){
				RawDataSQLTempView rawDataSQLTempView = null;
				if(HOURLY.equals(getReportType().name())) {
					rawDataSQLTempView = new RawDataSQLTempView(getReportDateHour());
					rawDataSQLTempView.createPixelProcessDataHourlyTempView(sparkSession);
				}else {
					rawDataSQLTempView = new RawDataSQLTempView(getReportDate());
					rawDataSQLTempView.createPixelProcessDataDailyTempView(sparkSession);
				}
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
			}
			
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
		}finally {

		}
		return true;
	}
	
	/**
	 * @return
	 */
	private String getDailyQuery() {		
		StringBuffer queryString = new StringBuffer();
		queryString.append(	"SELECT	 reportDate                 AS REPORT_DATE " 
						+ "         ,webId			    		AS WEB_ID" 
						+ "         ,deviceTypeCode             AS DEVICE_TYPE_CODE" 
						+ "         ,COUNT(DISTINCT pCid)       AS DAILY_ACTIVE_USER " 
						+ "         ,SUM(session)               AS SESSION " 
						+ "         ,SUM(bounce)                AS BOUNCE " 
						+ "         ,NVL(ROUND(AVG(CASE WHEN duration > 0 THEN duration END)), 0)   AS DURATION_SECOND" 
						+ "         ,SUM(pageview)              AS PAGE_VIEW " 
						+ "         ,SUM(viewcontent)           AS VIEW_CONTENT " 
						+ "         ,SUM(addtocart)             AS ADD_TO_CART " 
						+ "         ,SUM(purchased)             AS PURCHASE " 
						+ "         ,SUM(CASE WHEN A.pageView > 0 THEN 1 ELSE 0 END)            AS SESSION_PAGE_VIEW " 
						+ "         ,SUM(CASE WHEN A.viewcontent > 0 THEN 1 ELSE 0 END)         AS SESSION_VIEW_CONTENT " 
						+ "         ,SUM(CASE WHEN A.addToCart > 0 THEN 1 ELSE 0 END)           AS SESSION_ADD_TO_CART " 
						+ "         ,SUM(CASE WHEN A.purchased > 0 THEN 1 ELSE 0 END)           AS SESSION_PURCHASE " 
						+ "         ,COUNT(DISTINCT CASE WHEN pageview > 0 THEN pCid  END)      AS UNIQUE_PAGE_VIEW " 
						+ "         ,COUNT(DISTINCT CASE WHEN viewcontent > 0 THEN pCid  END)   AS UNIQUE_VIEW_CONTENT " 
						+ "         ,COUNT(DISTINCT CASE WHEN addtocart > 0 THEN pCid  END)     AS UNIQUE_ADD_TO_CART " 
						+ "         ,COUNT(DISTINCT CASE WHEN purchased > 0 THEN pCid END)      AS UNIQUE_PURCHASE " 
						+ " FROM  (SELECT    processDate    AS reportDate" 
						+ "                 ,processHour    AS reportHour" 
						+ "                 ,pixelId        " 
						+ "                 ,pCid   " 
						+ "                 ,deviceTypeCode" 
						+ "                 ,SUM(CASE WHEN pageView > 0 OR viewcontent > 0 OR addtocart > 0 OR purchased > 0 THEN 1 ELSE 0 END)             AS session   " 
						+ "                 ,CASE WHEN (SUM(pageview) + SUM(viewcontent)) = 1 AND (SUM(addtocart) + SUM(purchased)) = 0  THEN 1 ELSE 0 END  AS bounce " 
						+ "                 ,SUM(duration)                                              AS duration" 
						+ "                 ,SUM(pageview)                                              AS pageview " 
						+ "                 ,SUM(viewcontent)                                           AS viewcontent " 
						+ "                 ,SUM(addtocart)                                             AS addtocart " 
						+ "                 ,SUM(purchased)                                             AS purchased " 
						+ "         FROM    T_PIXEL_EVENT_USER" 
						+ "         GROUP BY processDate, processHour, pixelId, pCid, deviceTypeCode)  A  " 
						+ " INNER JOIN T_TRK_WEB TW  ON	A.pixelId = TW.pixelId " 
					 	+ " GROUP BY A.reportDate, TW.webId, A.deviceTypeCode" 
						+ " " 
						+ " UNION ALL" 
						+ " " 
						+ " SELECT	 reportDate                     AS REPORT_DATE " 
						+ "         ,webId			    		    AS WEB_ID" 
						+ "         ,'PMC999'                       AS DEVICE_TYPE_CODE" 
						+ "         ,COUNT(DISTINCT pCid)           AS ACTIVE_USER " 
						+ "         ,SUM(session)                   AS SESSION " 
						+ "         ,SUM(bounce)                    AS BOUNCE " 
						+ "         ,NVL(ROUND(AVG(CASE WHEN duration > 0 THEN duration END)), 0)   AS DURATION_SECOND" 
						+ "         ,SUM(pageview)                  AS PAGE_VIEW " 
						+ "         ,SUM(viewcontent)               AS VIEW_CONTENT " 
						+ "         ,SUM(addtocart)                 AS ADD_TO_CART " 
						+ "         ,SUM(purchased)                 AS PURCHASE " 
						+ "         ,SUM(CASE WHEN A.pageView > 0 THEN 1 ELSE 0 END)            AS SESSION_PAGE_VIEW " 
						+ "         ,SUM(CASE WHEN A.viewcontent > 0 THEN 1 ELSE 0 END)         AS SESSION_VIEW_CONTENT " 
						+ "         ,SUM(CASE WHEN A.addToCart > 0 THEN 1 ELSE 0 END)           AS SESSION_ADD_TO_CART " 
						+ "         ,SUM(CASE WHEN A.purchased > 0 THEN 1 ELSE 0 END)           AS SESSION_PURCHASE " 
						+ "         ,COUNT(DISTINCT CASE WHEN pageview > 0 THEN pCid  END)      AS UNIQUE_PAGE_VIEW " 
						+ "         ,COUNT(DISTINCT CASE WHEN viewcontent > 0 THEN pCid  END)   AS UNIQUE_VIEW_CONTENT " 
						+ "         ,COUNT(DISTINCT CASE WHEN addtocart > 0 THEN pCid  END)     AS UNIQUE_ADD_TO_CART " 
						+ "         ,COUNT(DISTINCT CASE WHEN purchased > 0 THEN pCid END)      AS UNIQUE_PURCHASE " 
						+ " FROM  (SELECT    processDate    AS reportDate" 
						+ "                 ,processHour    AS reportHour" 
						+ "                 ,pixelId        " 
						+ "                 ,pCid   " 
						+ "                 ,SUM(CASE WHEN pageView > 0 OR viewcontent > 0 OR addtocart > 0 OR purchased > 0 THEN 1 ELSE 0 END)             AS session   " 
						+ "                 ,CASE WHEN (SUM(pageview) + SUM(viewcontent)) = 1 AND (SUM(addtocart) + SUM(purchased)) = 0  THEN 1 ELSE 0 END  AS bounce " 
						+ "                 ,SUM(duration)                                              AS duration" 
						+ "                 ,SUM(pageview)                                              AS pageview " 
						+ "                 ,SUM(viewcontent)                                           AS viewcontent " 
						+ "                 ,SUM(addtocart)                                             AS addtocart " 
						+ "                 ,SUM(purchased)                                             AS purchased " 
						+ "         FROM    T_PIXEL_EVENT_USER" 
						+ "         GROUP BY processDate, processHour, pixelId, pCid)  A  " 
						+ " INNER JOIN T_TRK_WEB TW  ON	A.pixelId = TW.pixelId " 
						+ " GROUP BY A.reportDate, TW.webId "
						+ "");		
		return queryString.toString();

	}
		
	/**
	 * @return
	 */
	private String getHourlyQuery() {		
		StringBuffer queryString = new StringBuffer();
		queryString.append(	"SELECT	 reportDate                 AS REPORT_DATE " 
						+ "         ,reportHour                 AS REPORT_HOUR" 
						+ "         ,webId			    		AS WEB_ID" 
						+ "         ,deviceTypeCode             AS DEVICE_TYPE_CODE" 
						+ "         ,COUNT(DISTINCT pCid)       AS ACTIVE_USER " 
						+ "         ,SUM(session)               AS SESSION " 
						+ "         ,SUM(bounce)                AS BOUNCE " 
						+ "         ,NVL(ROUND(AVG(CASE WHEN duration > 0 THEN duration END)), 0)   AS DURATION_SECOND" 
						+ "         ,SUM(pageview)                  AS PAGE_VIEW " 
						+ "         ,SUM(viewcontent)               AS VIEW_CONTENT " 
						+ "         ,SUM(addtocart)                 AS ADD_TO_CART " 
						+ "         ,SUM(purchased)                 AS PURCHASE " 
						+ "         ,SUM(CASE WHEN A.pageView > 0 THEN 1 ELSE 0 END)            AS SESSION_PAGE_VIEW " 
						+ "         ,SUM(CASE WHEN A.viewcontent > 0 THEN 1 ELSE 0 END)         AS SESSION_VIEW_CONTENT " 
						+ "         ,SUM(CASE WHEN A.addToCart > 0 THEN 1 ELSE 0 END)           AS SESSION_ADD_TO_CART " 
						+ "         ,SUM(CASE WHEN A.purchased > 0 THEN 1 ELSE 0 END)           AS SESSION_PURCHASE " 
						+ "         ,COUNT(DISTINCT CASE WHEN pageview > 0 THEN pCid  END)      AS UNIQUE_PAGE_VIEW " 
						+ "         ,COUNT(DISTINCT CASE WHEN viewcontent > 0 THEN pCid  END)   AS UNIQUE_VIEW_CONTENT " 
						+ "         ,COUNT(DISTINCT CASE WHEN addtocart > 0 THEN pCid  END)     AS UNIQUE_ADD_TO_CART " 
						+ "         ,COUNT(DISTINCT CASE WHEN purchased > 0 THEN pCid END)      AS UNIQUE_PURCHASE " 
						+ " FROM  (SELECT    processDate    AS reportDate" 
						+ "                 ,processHour    AS reportHour" 
						+ "                 ,pixelId        " 
						+ "                 ,pCid   " 
						+ "                 ,deviceTypeCode" 
						+ "                 ,SUM(CASE WHEN pageView > 0 OR viewcontent > 0 OR addtocart > 0 OR purchased > 0 THEN 1 ELSE 0 END)             AS session   " 
						+ "                 ,CASE WHEN (SUM(pageview) + SUM(viewcontent)) = 1 AND (SUM(addtocart) + SUM(purchased)) = 0  THEN 1 ELSE 0 END  AS bounce " 
						+ "                 ,SUM(duration)                                              AS duration" 
						+ "                 ,SUM(pageview)                                              AS pageview " 
						+ "                 ,SUM(viewcontent)                                           AS viewcontent " 
						+ "                 ,SUM(addtocart)                                             AS addtocart " 
						+ "                 ,SUM(purchased)                                             AS purchased " 
						+ "         FROM    T_PIXEL_EVENT_USER_HOURLY" 
						+ "         GROUP BY processDate, processHour, pixelId, pCid, deviceTypeCode)  A  " 
						+ " INNER JOIN T_TRK_WEB TW  ON	A.pixelId = TW.pixelId" 
						+ " GROUP BY A.reportDate, A.reportHour, TW.webId, A.deviceTypeCode" 
						+ " " 
						+ " UNION ALL " 
						+ " " 
						+ " SELECT	 reportDate                 AS REPORT_DATE " 
						+ "         ,reportHour                 AS REPORT_HOUR" 
						+ "         ,webId			    		AS WEB_ID" 
						+ "         ,'PMC999'                   AS DEVICE_TYPE_CODE" 
						+ "         ,COUNT(DISTINCT pCid)       AS ACTIVE_USER " 
						+ "         ,SUM(session)               AS SESSION " 
						+ "         ,SUM(bounce)                AS BOUNCE " 
						+ "         ,NVL(ROUND(AVG(CASE WHEN duration > 0 THEN duration END)), 0)   AS DURATION_SECOND" 
						+ "         ,SUM(pageview)                  AS PAGE_VIEW " 
						+ "         ,SUM(viewcontent)               AS VIEW_CONTENT " 
						+ "         ,SUM(addtocart)                 AS ADD_TO_CART " 
						+ "         ,SUM(purchased)                 AS PURCHASE " 
						+ "         ,SUM(CASE WHEN A.pageView > 0 THEN 1 ELSE 0 END)            AS SESSION_PAGE_VIEW " 
						+ "         ,SUM(CASE WHEN A.viewcontent > 0 THEN 1 ELSE 0 END)         AS SESSION_VIEW_CONTENT " 
						+ "         ,SUM(CASE WHEN A.addToCart > 0 THEN 1 ELSE 0 END)           AS SESSION_ADD_TO_CART " 
						+ "         ,SUM(CASE WHEN A.purchased > 0 THEN 1 ELSE 0 END)           AS SESSION_PURCHASE " 
						+ "         ,COUNT(DISTINCT CASE WHEN pageview > 0 THEN pCid  END)      AS UNIQUE_PAGE_VIEW " 
						+ "         ,COUNT(DISTINCT CASE WHEN viewcontent > 0 THEN pCid  END)   AS UNIQUE_VIEW_CONTENT " 
						+ "         ,COUNT(DISTINCT CASE WHEN addtocart > 0 THEN pCid  END)     AS UNIQUE_ADD_TO_CART " 
						+ "         ,COUNT(DISTINCT CASE WHEN purchased > 0 THEN pCid END)      AS UNIQUE_PURCHASE " 
						+ " FROM  (SELECT    processDate    AS reportDate" 
						+ "                 ,processHour    AS reportHour" 
						+ "                 ,pixelId        " 
						+ "                 ,pCid   " 
						+ "                 ,SUM(CASE WHEN pageView > 0 OR viewcontent > 0 OR addtocart > 0 OR purchased > 0 THEN 1 ELSE 0 END)             AS session   " 
						+ "                 ,CASE WHEN (SUM(pageview) + SUM(viewcontent)) = 1 AND (SUM(addtocart) + SUM(purchased)) = 0  THEN 1 ELSE 0 END  AS bounce " 
						+ "                 ,SUM(duration)                                              AS duration" 
						+ "                 ,SUM(pageview)                                              AS pageview " 
						+ "                 ,SUM(viewcontent)                                           AS viewcontent " 
						+ "                 ,SUM(addtocart)                                             AS addtocart " 
						+ "                 ,SUM(purchased)                                             AS purchased " 
						+ "         FROM    T_PIXEL_EVENT_USER_HOURLY" 
						+ "         GROUP BY processDate, processHour, pixelId, pCid)  A  " 
						+ " INNER JOIN T_TRK_WEB TW  ON	A.pixelId = TW.pixelId " 
						+ " GROUP BY A.reportDate, A.reportHour, TW.webId ");		
		
		return queryString.toString();
	}
}
