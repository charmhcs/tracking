package com.dmcmedia.tracking.analytics.core.main.report.event;

import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.CommonDataAnalytics;
import com.dmcmedia.tracking.analytics.common.factory.SparkMongodbConnectionFactory;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.config.RawDataSQLTempView;
import com.dmcmedia.tracking.analytics.core.report.event.EventUser;
import com.dmcmedia.tracking.analytics.core.report.event.EventUserChannels;

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
* 2019. 1. 11.   cshwang   Initial Release
*
*
************************************************************************************************/
public class RunEventHourlyReport extends CommonDataAnalytics {
	
	private final static String EVENT_USER = "EventUser";
	private final static String EVENT_USER_CHANNELS = "EventUserChannels";
	
	/**
	 * @param args
	 * 
	 * args[0] = yyyy-MM-DD
	 * args[1] = MediaKeyword|MediaUrl
	 * 
	 */
	public static void main(String[] args) {	
	
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();
		RawDataSQLTempView rawDataSQLTempView = null;
		
		try{
			if(args.length == 0){
				rawDataSQLTempView = new RawDataSQLTempView();
				rawDataSQLTempView.createPixelRawDataHourlyTempView(sparkSession);
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
				rawDataSQLTempView.createPixelProcessDataHourlyTempView(sparkSession);

				new EventUser(BATCHJOB, HOURLY).reportsDataSet(sparkSession);
				new EventUserChannels(BATCHJOB, HOURLY).reportsDataSet(sparkSession);
				  
			}else if(args.length == 1) {
				rawDataSQLTempView = new RawDataSQLTempView(args[0]);
				rawDataSQLTempView.createPixelRawDataHourlyTempView(sparkSession);
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
				rawDataSQLTempView.createPixelProcessDataHourlyTempView(sparkSession);
				
				new EventUser(BATCHJOB, HOURLY).reportsDataSet(sparkSession, args[0]);
				new EventUserChannels(BATCHJOB, HOURLY).reportsDataSet(sparkSession, args[0]);
			
			}else if(args.length == 2) {	
				
				switch(args[1]) {
					case EVENT_USER :
						new EventUser(EACH, HOURLY).reportsDataSet(sparkSession, args[0]);
						break;
					case EVENT_USER_CHANNELS :
						new EventUserChannels(EACH, HOURLY).reportsDataSet(sparkSession, args[0]);
						break;
					default :
						throw new Exception(ExceptionMap.INVALID_DATA_FORMAT_EXCEPTION);
				}
			}			
		} catch (Exception e) {
			LogStack.batch.error(e);
		} finally{
			sparkSession.stop();
		}
	}
}