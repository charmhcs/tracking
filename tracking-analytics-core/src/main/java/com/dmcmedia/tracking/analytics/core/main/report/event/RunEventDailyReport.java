package com.dmcmedia.tracking.analytics.core.main.report.event;

import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.CommonDataAnalytics;
import com.dmcmedia.tracking.analytics.common.factory.SparkMongodbConnectionFactory;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.config.RawDataSQLTempView;
import com.dmcmedia.tracking.analytics.core.report.event.EventNewUserRetention;
import com.dmcmedia.tracking.analytics.core.report.event.EventNewUser;
import com.dmcmedia.tracking.analytics.core.report.event.EventNewUserChannels;
import com.dmcmedia.tracking.analytics.core.report.event.EventPopularityRanking;
import com.dmcmedia.tracking.analytics.core.report.event.EventUser;
import com.dmcmedia.tracking.analytics.core.report.event.EventUserChannels;
import com.dmcmedia.tracking.analytics.core.report.event.EventUserDuration;

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
* 2018-11-28  cshwang   Initial Release
*
*
************************************************************************************************/
public class RunEventDailyReport extends CommonDataAnalytics {
	
	private final static String EVENT_USER_DURATION = "EventUserDuration";
	private final static String EVENT_USER = "EventUser";
	private final static String EVENT_USER_CHANNELS = "EventUserChannels";
	private final static String EVENT_NEW_USER_CHANNELS = "NewUserChannels";
	private final static String EVENT_NEW_USER_RETENTION = "NewUserRetention";
	private final static String EVENT_NEW_USER = "NewUser";
	private final static String EVENT_POPULARITY_RANKING = "EventPopularityRanking";
	
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
				rawDataSQLTempView.createPixelRawDataDailyTempView(sparkSession);
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
				
				new EventUser(BATCHJOB, DAILY).reportsDataSet(sparkSession); // 일,시간 
				new EventUserChannels(BATCHJOB, DAILY).reportsDataSet(sparkSession); // 일, 시간 
				new EventNewUser(BATCHJOB).reportsDataSet(sparkSession); // 시간
				new EventNewUserChannels(BATCHJOB).reportsDataSet(sparkSession); // 시간 
				new EventNewUserRetention(BATCHJOB).reportsDataSet(sparkSession); // 일 
				new EventUserDuration(BATCHJOB).reportsDataSet(sparkSession); // 일
				new EventPopularityRanking(BATCHJOB).reportsDataSet(sparkSession); // 일
				 
				  
			}else if(args.length == 1) {
				rawDataSQLTempView = new RawDataSQLTempView(args[0]);
				rawDataSQLTempView.createPixelRawDataDailyTempView(sparkSession);
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);

				new EventUser(BATCHJOB, DAILY).reportsDataSet(sparkSession, args[0]);
				new EventUserChannels(BATCHJOB, DAILY).reportsDataSet(sparkSession, args[0]);

				new EventNewUser(BATCHJOB).reportsDataSet(sparkSession, args[0]);
				new EventNewUserChannels(BATCHJOB).reportsDataSet(sparkSession, args[0]);
				new EventNewUserRetention(BATCHJOB).reportsDataSet(sparkSession, args[0]);
				new EventUserDuration(BATCHJOB).reportsDataSet(sparkSession, args[0]);
				new EventPopularityRanking(BATCHJOB).reportsDataSet(sparkSession, args[0]);
				
			}else if(args.length == 2) {
				
				switch(args[1]) {
					case EVENT_USER :
						new EventUser(EACH, DAILY).reportsDataSet(sparkSession, args[0]);
						break;
					case EVENT_USER_CHANNELS :
						new EventUserChannels(EACH, DAILY).reportsDataSet(sparkSession, args[0]);
						break;
					case EVENT_NEW_USER :
						new EventNewUser().reportsDataSet(sparkSession, args[0]);
						break;
					case EVENT_NEW_USER_CHANNELS :
						new EventNewUserChannels().reportsDataSet(sparkSession, args[0]);
						break;
					case EVENT_NEW_USER_RETENTION :
						new EventNewUserRetention().reportsDataSet(sparkSession, args[0]);
						break;
					case EVENT_USER_DURATION :
						new EventUserDuration().reportsDataSet(sparkSession, args[0]);
						break;
					case EVENT_POPULARITY_RANKING :
						new EventPopularityRanking().reportsDataSet(sparkSession, args[0]);
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