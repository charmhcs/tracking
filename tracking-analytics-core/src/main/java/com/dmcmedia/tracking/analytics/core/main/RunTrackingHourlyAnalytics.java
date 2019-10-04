package com.dmcmedia.tracking.analytics.core.main;

import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.CommonDataAnalytics;
import com.dmcmedia.tracking.analytics.common.factory.SparkMongodbConnectionFactory;
import com.dmcmedia.tracking.analytics.common.map.CodeMap;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.util.DateUtil;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.config.RawDataSQLTempView;
import com.dmcmedia.tracking.analytics.core.process.event.EventAudience;
import com.dmcmedia.tracking.analytics.core.process.event.RawDataConversion;
import com.dmcmedia.tracking.analytics.core.process.event.EventUserPixel;
//import com.dmcmedia.tracking.analytics.core.process.event.ConversionRawDataPixelProcess;
import com.dmcmedia.tracking.analytics.core.process.feed.ProductsFeedPixel;
import com.dmcmedia.tracking.analytics.core.report.event.EventUser;
import com.dmcmedia.tracking.analytics.core.report.event.EventUserChannels;
import com.dmcmedia.tracking.analytics.core.report.trackinglink.TrackingLinkRegion;
import com.dmcmedia.tracking.analytics.core.report.trackinglink.TrackingLinkReport;

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
public class RunTrackingHourlyAnalytics extends CommonDataAnalytics {
	
	/**
	 * @param arg
	 */
	public static void main(String[] args) {
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();
		RawDataSQLTempView rawDataSQLTempView = null;
		
		try{
					
			if(args.length == 0){
				rawDataSQLTempView = new RawDataSQLTempView();
				rawDataSQLTempView.createPixelRawDataHourlyTempView(sparkSession);
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
				
				new RawDataConversion(BATCHJOB, HOURLY).processDataSet(sparkSession); // all
				new EventUserPixel(BATCHJOB, HOURLY).processDataSet(sparkSession); // daily, hourly
				
				rawDataSQLTempView.createPixelProcessDataHourlyTempView(sparkSession);
				
				new EventUser(BATCHJOB, HOURLY).reportsDataSet(sparkSession); // daily,hourly
				new EventUserChannels(BATCHJOB, HOURLY).reportsDataSet(sparkSession); // daily,hourly				
				new TrackingLinkReport(BATCHJOB, HOURLY).reportsDataSet(sparkSession); // daily,hourly
				new TrackingLinkRegion(BATCHJOB).reportsDataSet(sparkSession); // hourly
				
				new ProductsFeedPixel(EACH, HOURLY).processDataSet(sparkSession); // daily,hourly
				new EventAudience(EACH, HOURLY).processDataSet(sparkSession); // daily,hourly
				
			}else {
				if(!DateUtil.checkDateFormat(args[0], CodeMap.DATE_YMDH_PATTERN_FOR_ANYS)){ // yyyy-MM-dd HH
					throw new Exception(ExceptionMap.INVALID_DATA_FORMAT_EXCEPTION);
				}				
				rawDataSQLTempView = new RawDataSQLTempView(args[0]);
				rawDataSQLTempView.createPixelRawDataHourlyTempView(sparkSession);
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
				
				new RawDataConversion(BATCHJOB, HOURLY).processDataSet(sparkSession, args[0]);
				new EventUserPixel(BATCHJOB, HOURLY).processDataSet(sparkSession, args[0]); // daily, hourly

				rawDataSQLTempView.createPixelProcessDataHourlyTempView(sparkSession);

				new EventUser(BATCHJOB, HOURLY).reportsDataSet(sparkSession ,args[0]); // daily,hourly
				new EventUserChannels(BATCHJOB, HOURLY).reportsDataSet(sparkSession ,args[0]); // daily,hourly 
				new TrackingLinkReport(BATCHJOB, HOURLY).reportsDataSet(sparkSession,args[0]); // daily,hourly
				new TrackingLinkRegion(BATCHJOB).reportsDataSet(sparkSession,args[0]); // hourly
				
				new ProductsFeedPixel(EACH, HOURLY).processDataSet(sparkSession ,args[0]); // daily,hourly
				new EventAudience(EACH, HOURLY).processDataSet(sparkSession ,args[0]); // daily,hourly
			}
		} catch (Exception e) {
			LogStack.batch.error(e);	
		} finally{
			sparkSession.stop();
		}
	}
}