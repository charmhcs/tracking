package com.dmcmedia.tracking.analytics.core.main;

import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.CommonDataAnalytics;
import com.dmcmedia.tracking.analytics.common.factory.SparkMongodbConnectionFactory;
import com.dmcmedia.tracking.analytics.common.map.CodeMap;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.util.DateUtil;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.config.RawDataSQLTempView;
import com.dmcmedia.tracking.analytics.core.report.event.EventRealTime;

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
* 2018-12-30   cshwang  Initial Release
* 
* 
*
************************************************************************************************/
public class RunTrackingMinutelyAnalytics extends CommonDataAnalytics {
	/**
	 * @param arg
	 * args[0] yyyy-MM-dd HH:mm
	 */
	public static void main(String[] args) {
		
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();
		
		try{
			RawDataSQLTempView rawDataSQLTempView = null;
			if(args.length == 0){
				rawDataSQLTempView = new RawDataSQLTempView();
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
				
				new EventRealTime().analyzeDataSet(sparkSession);
				
			}else {
				if(!DateUtil.checkDateFormat(args[0], CodeMap.DATE_YMDHM_PATTERN_FOR_ANYS)){ // yyyy-MM-dd HH:mm
					throw new Exception(ExceptionMap.INVALID_DATA_FORMAT_EXCEPTION);
				}
				rawDataSQLTempView = new RawDataSQLTempView(args[0]);
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
				
				new EventRealTime().analyzeDataSet(sparkSession,args[0]);
			}
		} catch (Exception e) {
			LogStack.batch.error(e);	
		} finally{
			sparkSession.stop();
		}
	}
}
