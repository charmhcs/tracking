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
* 2018. 11. 9.   cshwang   Initial Release
*
*
************************************************************************************************/
public class EventUserDuration  extends AbstractDataReport{ 
	
	/**
	 * 
	 */
	public EventUserDuration(){
		super();
	}
	
	/**
	 * @param runType
	 */
	public EventUserDuration(String runType){
		super(runType);
	}
	
	public boolean reportsDataSet(SparkSession sparkSession) {
		
		Dataset<Row> resultDS = null;
		String targetRdbTableName = "TS_TRK_WEB_EVENT_DURATION";
		if(super.isValid() == false){
			return false;
		}
		try{
			if(EACH.equals(this.getRunType().name())){
				RawDataSQLTempView rawDataSQLTempView = new RawDataSQLTempView(getReportDate());
				rawDataSQLTempView.createPixelRawDataDailyTempView(sparkSession);
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
			}
			String durationTable = "(SELECT ATTRIBUTE_ITEM AS attributeItem, START_VALUE AS startValue, END_VALUE AS endValue FROM tracking.ZS_TRK_ATTRIBUTE_ITEM WHERE USE_YN = 'Y' AND DEL_YN = 'N' AND ATTRIBUTE = 'RNG001') AS T_DURATION_ATTRIBUTE";
			SparkJdbcConnectionFactory.getSelectJdbcDfLoad(sparkSession, durationTable).persist().createOrReplaceTempView("T_DURATION_ATTRIBUTE");

			resultDS = sparkSession.sql("SELECT  V.reportDate       AS REPORT_DATE "
									+ "         ,TW.webId		    AS WEB_ID  "
									+ "         ,V.attributeItem    AS DURATION_ATTRIBUTE "
									+ "         ,V.audienceSize     AS AUDIENCE_SIZE "
									+ " FROM(   SELECT   C.reportDate "
									+ "                 ,C.pixelId "
									+ "                 ,D.attributeItem "
									+ "                 ,SUM(CASE WHEN D.endValue is null THEN (CASE WHEN duration >= D.startValue THEN 1 ELSE 0 END)  "
									+ "                       WHEN D.endValue is not null THEN (CASE WHEN duration BETWEEN D.startValue AND D.endValue THEN 1 ELSE 0 END) "
									+ "                       END) AS audienceSize  "
									+ "         FROM(SELECT  B.reportDate "
									+ "                     ,B.reportHour "
									+ "                     ,B.pixelId "
									+ "                     ,B.pCid "
									+ "                     ,SUM(duration) AS duration "
									+ "             FROM (SELECT A.reportDate "
									+ "                         ,A.reportHour "
									+ "                         ,A.pixelId "
									+ "                         ,A.pCid "
									+ "                         ,UNIX_TIMESTAMP(MAX(isoDate), 'yyyy-MM-dd HH:mm:ss') - UNIX_TIMESTAMP(MIN(isoDate), 'yyyy-MM-dd HH:mm:ss')  AS duration "
									+ "                     FROM   (SELECT	trackingEventCode "
									+ "                        				,pageView.pixel_id	AS	pixelId "
									+ "                        			    ,pageView.p_cid		    AS	pCid "
									+ "                        			    ,logDateTime            AS  logDateTime "
									+ "                        			    ,isoDate "
									+ "                        			    ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')      AS reportDate "
									+ "                                     ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS reportHour "
									+ "                        		FROM	T_PIXEL_PAGE_VIEW   "
									+ " "
									+ "                        		UNION ALL"
									+ " "
									+ "                             SELECT	trackingEventCode "
									+ "                        				,viewContent.pixel_id	AS pixelId "
									+ "                        				,viewContent.p_cid      AS pCid "
									+ "                        				,logDateTime "
									+ "                        				,isoDate "
									+ "                        				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')      AS reportDate "
									+ "                                     ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS reportHour "
									+ "                        		FROM	T_PIXEL_VIEW_CONTENT "
									+ " "
									+ "                        		UNION ALL"
									+ " "
									+ "                             SELECT	trackingEventCode "
									+ "                        				,addToCart.pixel_id	AS	pixelId "
									+ "                        				,addToCart.p_cid		AS	pCid "
									+ "                        				,logDateTime "
									+ "                        				,isoDate "
									+ "                        				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')      AS reportDate "
									+ "                                     ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS reportHour "
									+ "                        		FROM	T_PIXEL_ADD_TO_CART "
									+ " "
									+ "                        		UNION ALL"
									+ " "
									+ "                        		SELECT   trackingEventCode "
									+ "                        				,purchased.pixel_id   AS pixelId "
									+ "                        				,purchased.p_cid        AS pCid "
									+ "                        				,logDateTime "
									+ "                        				,isoDate "
									+ "                        				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')      AS reportDate "
									+ "                                     ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS reportHour "
									+ "                        		FROM	T_PIXEL_PURCHASED_RAW "
									+ "                        		) A "
									+ "                     GROUP BY A.reportDate, A.reportHour, A.pixelId, A.pCid "
									+ "                 ) B "
									+ "                 GROUP BY  B.reportDate, B.reportHour, B.pixelId, B.pCid "
									+ "             ) C "
									+ "             CROSS JOIN T_DURATION_ATTRIBUTE D "
									+ "         GROUP BY C.reportDate, C.pixelId, D.attributeItem "
									+ "     ) V "
									+ " INNER JOIN T_TRK_WEB TW  ON	V.pixelId = TW.pixelId ");
			resultDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), targetRdbTableName, SparkJdbcConnectionFactory.getDefaultJdbcOptions());
		}catch(Exception e){
			LogStack.report.error(ExceptionMap.REPORT_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.report.error(e);			
		}finally {

		}
		return true;
		
	}
}
