package com.dmcmedia.tracking.analytics.core.main.report.event;

import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.CommonDataAnalytics;
import com.dmcmedia.tracking.analytics.common.factory.SparkMongodbConnectionFactory;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
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
* 2018-12-29   cshwang  Initial Release
*
*
************************************************************************************************/
public class RunEventRealTimeReport  extends CommonDataAnalytics {
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) {		
		
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();
		
		try {
			if(args.length == 0){
				new EventRealTime().analyzeDataSet(sparkSession);
				
			}else{
				new EventRealTime().analyzeDataSet(sparkSession, args[0]);
			}
		} catch (Exception e) {
			LogStack.batch.error(e);
		} finally{
			sparkSession.stop();
		}
	}
}
