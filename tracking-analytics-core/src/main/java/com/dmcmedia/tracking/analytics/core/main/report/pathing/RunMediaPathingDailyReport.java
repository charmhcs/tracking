package com.dmcmedia.tracking.analytics.core.main.report.pathing;

import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.CommonDataAnalytics;
import com.dmcmedia.tracking.analytics.common.factory.SparkMongodbConnectionFactory;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.report.pathing.MediaKeyword;
import com.dmcmedia.tracking.analytics.core.report.pathing.MediaUrl;

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
* 2018. 11. 8.   cshwang   Initial Release
*
*
************************************************************************************************/
public class RunMediaPathingDailyReport  extends CommonDataAnalytics{
	
	public final static String MEDIA_URL = "MediaUrl";
	public final static String MEDIA_KEYWORD = "MediaKeyword";
	
	/**
	 * @param args
	 * 
	 * args[0] = yyyy-MM-DD
	 * args[1] = MediaKeyword|MediaUrl
	 * 
	 */
	public static void main(String[] args) {		
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();
		
		try{
			if(args.length == 0){
				new MediaUrl().reportsDataSet(sparkSession);
				new MediaKeyword().reportsDataSet(sparkSession);
			}else if(args.length == 1) {
				new MediaUrl().reportsDataSet(sparkSession, args[0]);
				new MediaKeyword().reportsDataSet(sparkSession, args[0]);
			}else if(args.length == 2) {
				
				switch(args[1]) {
					case MEDIA_URL :
						new MediaUrl().reportsDataSet(sparkSession, args[0]);
						break;
					case MEDIA_KEYWORD : 
						new MediaKeyword().reportsDataSet(sparkSession, args[0]);
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