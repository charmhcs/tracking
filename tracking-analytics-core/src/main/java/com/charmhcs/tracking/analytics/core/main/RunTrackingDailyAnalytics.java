package com.charmhcs.tracking.analytics.core.main;

import com.charmhcs.tracking.analytics.core.config.RawDataSQLTempView;
import com.charmhcs.tracking.analytics.core.process.feed.ProductsFeedPixel;
import com.charmhcs.tracking.analytics.core.report.ecomm.EcommLastClickConversionChannels;
import com.charmhcs.tracking.analytics.core.report.pathing.MediaKeyword;
import com.charmhcs.tracking.analytics.core.report.pathing.MediaUrl;
import com.charmhcs.tracking.analytics.core.report.system.CreateCodeAndDimensionData;
import com.charmhcs.tracking.analytics.core.report.trackinglink.TrackingLinkReport;
import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.CommonDataAnalytics;
import com.dmcmedia.tracking.analytics.common.factory.SparkMongodbConnectionFactory;
import com.dmcmedia.tracking.analytics.common.map.CodeMap;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.util.DateUtil;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
//import com.dmcmedia.tracking.analytics.core.process.event.EventPixelProcess;
//import com.dmcmedia.tracking.analytics.core.process.feed.CategoriesPixelProcess;
import com.charmhcs.tracking.analytics.core.report.event.EventNewUserRetention;
import com.charmhcs.tracking.analytics.core.report.event.EventNewUser;
import com.charmhcs.tracking.analytics.core.report.event.EventNewUserChannels;
import com.charmhcs.tracking.analytics.core.report.event.EventPopularityRanking;
import com.charmhcs.tracking.analytics.core.report.event.EventUser;
import com.charmhcs.tracking.analytics.core.report.event.EventUserChannels;
import com.charmhcs.tracking.analytics.core.report.event.EventUserDuration;

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
* 2018. 9. 17.   cshwang   Initial Release
*
*
************************************************************************************************/
public class RunTrackingDailyAnalytics extends CommonDataAnalytics{
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();
		
		try{
			RawDataSQLTempView rawDataSQLTempView = null;
			
			if(args.length == 0){
				rawDataSQLTempView = new RawDataSQLTempView();
				rawDataSQLTempView.createPixelRawDataDailyTempView(sparkSession);
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
				

				/* Pixel Process
				 * 1. ProductsFeedPixelProcess - Feed 생성 (1일/3일/7일/14일/28일)
				 * 2. AudiencePixelProcess - pCid 별 오디언스 데이터 생성 (1일/3일/7일/14일/28일)
				 * 3. NewUserPixelProcess - pixel 기준 신규 유저 (일)
				 * */
				new ProductsFeedPixel(BATCHJOB).processDataSet(sparkSession);
				new EventUserPixel(BATCHJOB, DAILY).processDataSet(sparkSession); // daily, hourly
				new EventUserAccumulation(BATCHJOB).processDataSet(sparkSession);
				new EventNewUserPixel(BATCHJOB).processDataSet(sparkSession);

				//  Create Code
				rawDataSQLTempView.createPixelProcessDataDailyTempView(sparkSession);
				new CreateCodeAndDimensionData().reportsDataSet(sparkSession);
						
				// Event Report
				new EventUser(BATCHJOB, DAILY).reportsDataSet(sparkSession);
				new EventUserChannels(BATCHJOB, DAILY).reportsDataSet(sparkSession);
								
				new EventPopularityRanking(BATCHJOB).reportsDataSet(sparkSession);
				new EventUserDuration(BATCHJOB).reportsDataSet(sparkSession);
				
				new EventNewUserRetention(BATCHJOB).reportsDataSet(sparkSession);
				new EventNewUser(BATCHJOB).reportsDataSet(sparkSession);
				// Event Channels Report
				new EventNewUserChannels(BATCHJOB).reportsDataSet(sparkSession);

				// Tracking Report
				new TrackingLinkReport(BATCHJOB, DAILY).reportsDataSet(sparkSession);
//				new TrackingLinkRegion(BATCHJOB).reportsDataSet(sparkSession);
				
				// Ecomm
				new EcommLastClickConversionChannels(BATCHJOB).reportsDataSet(sparkSession);
//				new EcommReportWeb(ALL, BATCHJOB).reportsDataSet(sparkSession);
				
				// Pathing Report
				new MediaKeyword(BATCHJOB).reportsDataSet(sparkSession);
				new MediaUrl(BATCHJOB).reportsDataSet(sparkSession);
				
				new EventAudience(BATCHJOB).processDataSet(sparkSession);
				  
			}else{
				
				if(!DateUtil.checkDateFormat(args[0], CodeMap.DATE_YMD_PATTERN_FOR_ANYS)){ // yyyy-MM-dd
					throw new Exception(ExceptionMap.INVALID_DATA_FORMAT_EXCEPTION);
				}

				rawDataSQLTempView = new RawDataSQLTempView(args[0]);
				rawDataSQLTempView.createPixelRawDataDailyTempView(sparkSession);
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);

				
				/* Pixel Process
				 * 1. ProductsFeedPixelProcess - Feed 생성 (1일/3일/7일/14일/28일)
				 * 2. AudiencePixelProcess - pCid 별 오디언스 데이터 생성 (1일/3일/7일/14일/28일)
				 * 3. PathingPixelProcess - 유입경로 데이터 생성 (일)
				 * 4. ConversionRawDataPixelProcess - 전환 데이터 생성 (일)
				 * 5. NewUserPixelProcess - pixel 기준 신규 유저 (일)
				 * */
				new ProductsFeedPixel(BATCHJOB).processDataSet(sparkSession, args[0]);	
				new EventUserPixel(BATCHJOB).processDataSet(sparkSession, args[0]);
				new EventUserAccumulation(BATCHJOB).processDataSet(sparkSession, args[0]);
				new EventNewUserPixel(BATCHJOB).processDataSet(sparkSession, args[0]);

				//  Create Code
				rawDataSQLTempView.createPixelProcessDataDailyTempView(sparkSession);
				new CreateCodeAndDimensionData().reportsDataSet(sparkSession, args[0]);
							
				// Event Report
				new EventUser(BATCHJOB, DAILY).reportsDataSet(sparkSession, args[0]);
				new EventUserChannels(BATCHJOB, DAILY).reportsDataSet(sparkSession, args[0]);
				
				new EventPopularityRanking(BATCHJOB).reportsDataSet(sparkSession, args[0]);
				new EventUserDuration(BATCHJOB).reportsDataSet(sparkSession, args[0]);

				new EventNewUserRetention(BATCHJOB).reportsDataSet(sparkSession, args[0]);
				new EventNewUser(BATCHJOB).reportsDataSet(sparkSession, args[0]);
				
				// Event Channels Report
				new EventNewUserChannels(BATCHJOB).reportsDataSet(sparkSession, args[0]);
				
				
				// Tracking Report
				new TrackingLinkReport(BATCHJOB, DAILY).reportsDataSet(sparkSession, args[0]);
				//new TrackingLinkRegion(BATCHJOB).reportsDataSet(sparkSession, args[0]);
				
				// Ecomm
				new EcommLastClickConversionChannels(BATCHJOB).reportsDataSet(sparkSession, args[0]);
//				new EcommReportWeb(BATCHJOB).reportsDataSet(sparkSession);
				
				// Pathing Report
				new MediaKeyword(BATCHJOB).reportsDataSet(sparkSession, args[0]);
				new MediaUrl(BATCHJOB).reportsDataSet(sparkSession, args[0]);	
				
				new EventAudience(BATCHJOB).processDataSet(sparkSession, args[0]);
			}
			
		} catch (Exception e) {
			LogStack.batch.error(e);	
		} finally{
			sparkSession.stop();
		}
	}
}
