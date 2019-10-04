package com.dmcmedia.tracking.analytics.core.report.event;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.AbstractDataReport;
import com.dmcmedia.tracking.analytics.common.factory.SparkJdbcConnectionFactory;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.map.MongodbMap;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.config.RawDataSQLTempView;
import com.dmcmedia.tracking.analytics.core.dataset.event.NewUser;
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
* 2018. 11. 13.   cshwang   Initial Release
*
*
************************************************************************************************/
public class EventNewUser extends AbstractDataReport  {

	/**
	 * 생성자 
	 */
	public EventNewUser(){
		super();
	}
	
	/**
	 * 생성자
	 * @param runType
	 */
	public EventNewUser(String runType){
		super(runType);
	}
	
	public boolean reportsDataSet(SparkSession sparkSession) {

		Dataset<Row> resultDS 		= null;
		String targetRdbTableName 	= "tracking.TS_TRK_WEB_EVENT_NEWUSER";
		
		if(super.isValid() == false){
			return false;
		}
		try{
			if(EACH.equals(this.getRunType().name())){
				RawDataSQLTempView rawDataSQLTempView = new RawDataSQLTempView(getReportDate());
				rawDataSQLTempView.createPixelRawDataDailyTempView(sparkSession);
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
			}
			MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", getProcessedDataBase())
					.withOption("collection", this.getReportDate() + MongodbMap.COLLECTION_PIXEL_NEWUSER), NewUser.class)
					.createOrReplaceTempView("T_PIXEL_NEWUSER");
			
			resultDS = sparkSession.sql("SELECT  A.reportDate       AS REPORT_DATE "
									+ "         ,A.reportHour       AS REPORT_HOUR "
									+ "         ,TW.webId           AS WEB_ID "
									+ "         ,'PMC999'           AS DEVICE_TYPE_CODE "
									+ "         ,COUNT(DISTINCT A.pCid)		AS NEW_USER "
									+ " FROM ( SELECT	 DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')      AS reportDate "
									+ "             	,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS reportHour "
									+ "             	,pixelId "
									+ "                 ,pCid "
									+ "                 ,deviceTypeCode "
									+ "         FROM T_PIXEL_NEWUSER "
									+ "         ) A"
									+ " INNER JOIN T_TRK_WEB TW  ON	A.pixelId = TW.pixelId "
									+ " GROUP BY A.reportDate, A.reportHour, TW.webId "
									+ " UNION ALL"
									+ " SELECT   A.reportDate       AS REPORT_DATE "
									+ " 	    ,A.reportHour       AS REPORT_HOUR "
									+ "         ,TW.webId           AS WEB_ID "
									+ "         ,A.deviceTypeCode   AS DEVICE_TYPE_CODE "
									+ "         ,COUNT(DISTINCT A.pCid)      AS NEW_USER "
									+ " FROM ( SELECT    DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')      AS reportDate "
									+ "                 ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS reportHour "
									+ "                 ,pixelId"
									+ "                 ,pCid"
									+ "                 ,deviceTypeCode"
									+ "         FROM T_PIXEL_NEWUSER   "
									+ "         ) A "
									+ " INNER JOIN T_TRK_WEB TW  ON A.pixelId = TW.pixelId  "
									+ " GROUP BY A.reportDate, A.reportHour, TW.webId, A.deviceTypeCode ");
			resultDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), targetRdbTableName, SparkJdbcConnectionFactory.getDefaultJdbcOptions());
		
		}catch(Exception e){
			LogStack.report.error(ExceptionMap.REPORT_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.report.error(e);
		}finally {
			
		}
		return true;
	}
}
