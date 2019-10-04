package com.dmcmedia.tracking.analytics.core.report.event;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.AbstractDataReport;
import com.dmcmedia.tracking.analytics.common.factory.SparkJdbcConnectionFactory;
import com.dmcmedia.tracking.analytics.common.map.CodeMap;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.map.MongodbMap;
import com.dmcmedia.tracking.analytics.common.util.DateUtil;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.config.RawDataSQLTempView;
import com.dmcmedia.tracking.analytics.core.dataset.event.NewUser;
import com.dmcmedia.tracking.analytics.core.dataset.event.User;
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
* 2018. 10. 2.   cshwang   Initial Release
*
*
************************************************************************************************/
public class EventNewUserRetention extends AbstractDataReport{ 
	
	public EventNewUserRetention(){
		super();
	}
	
	public EventNewUserRetention(String runType){
		super(runType);
	}
	
	public boolean reportsDataSet(SparkSession sparkSession) {

		String targetRdbTableName = "TS_TRK_WEB_EVENT_RETENTION";
		Dataset<Row> maxRetentionDaysDS = null;
		List<String> maxConversionDaysList = null;
		int maxRetentionDays = 0;
		JavaSparkContext jsc = null;
		if(super.isValid() == false){
			return false;
		}
		
		try {
			if(EACH.equals(this.getRunType().name())){
		    	RawDataSQLTempView rawDataSQLTempView = new RawDataSQLTempView(getReportDate());
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
			}
			
			String trkMaxRetentionDays = "(	SELECT 	MAX(CAST(COMMON_CODE_ITEM_NAME AS UNSIGNED)) AS maxRetentionDays "
										+ "	FROM 	common.TC_CMM_COMM_CODE_ITEM "
										+ "	WHERE 	SYSTEM_TYPE = 'TRK' "
										+ "	AND 	COMMON_CODE = 'TTC017' "
										+ "	AND	 	USE_YN = 'Y' "
										+ "	AND 	DEL_YN = 'N') AS T_TRK_RETENTION_DAYS";
			maxRetentionDaysDS = SparkJdbcConnectionFactory.getSelectJdbcDfLoad(sparkSession, trkMaxRetentionDays);
			maxConversionDaysList =  maxRetentionDaysDS.as(Encoders.STRING()).collectAsList();
			maxRetentionDays = Integer.parseInt(maxConversionDaysList.get(0));
			
			jsc = new JavaSparkContext(sparkSession.sparkContext());

			Dataset<User> eventUserDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database", getProcessedDataBase())
					.withOption("collection", this.getReportDate() + MongodbMap.COLLECTION_PIXEL_EVENT_USER)).toDS(User.class);
			eventUserDS.createOrReplaceTempView("T_PIXEL_EVENT_USER");
			
			for (int currentDays = 0 ; currentDays < maxRetentionDays; currentDays++ ) {
				
				String newUserDate = DateUtil.getChangeDateFormatIntervalDay(this.getReportDate(), CodeMap.DATE_YMD_PATTERN_FOR_ANYS, CodeMap.DATE_YMD_PATTERN_FOR_ANYS, -currentDays);
				String newUserProcessedDataBase = MongodbMap.DATABASE_PROCESSED_MONTH + DateUtil.getChangeDateFormatIntervalDay(this.getReportDate(), CodeMap.DATE_YMD_PATTERN_FOR_ANYS, CodeMap.DATE_MONTH_PATTERN, -currentDays);
				
				Dataset<NewUser> pixelNewUserDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database", newUserProcessedDataBase).withOption("collection", newUserDate + MongodbMap.COLLECTION_PIXEL_NEWUSER)).toDS(NewUser.class);
				pixelNewUserDS.createOrReplaceTempView("T_PIXEL_NEWUSER");
				Dataset<Row> resultDS = sparkSession.sql( "	SELECT 	 '"+getReportDate()+"'	AS REPORT_DATE"
														+ "			,V2.webId				AS WEB_ID "
														+ "			,V1.newUserDate 		AS NEWUSER_DATE"
														+ "			,V1.retentionCount		AS RETENTION "
														+ "	FROM 	(SELECT  A.processDate 	AS newUserDate"
														+ "		    		,A.pixelId "  
														+ "        			,COUNT(DISTINCT A.pCid) 	AS retentionCount"  
														+ "			FROM    T_PIXEL_NEWUSER A"  
														+ "			WHERE   EXISTS (SELECT 0 FROM T_PIXEL_EVENT_USER B WHERE A.pixelId = B.pixelId AND A.pCid = B.pCid)"  
														+ "			GROUP BY A.processDate, A.pixelId ) V1"
														+ " INNER JOIN T_TRK_WEB V2   ON	V1.pixelId 	= V2.pixelId ");
				resultDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), targetRdbTableName, SparkJdbcConnectionFactory.getDefaultJdbcOptions());
							
			}
		}catch(Exception e){
			LogStack.report.error(ExceptionMap.REPORT_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.report.error(e);
		}finally {

		}
		return true;
	}
}
