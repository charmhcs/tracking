package com.dmcmedia.tracking.analytics.core.main.process.event;

import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.CommonDataAnalytics;
import com.dmcmedia.tracking.analytics.common.factory.SparkMongodbConnectionFactory;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.process.event.RawDataConversion;
import com.dmcmedia.tracking.analytics.core.process.event.EventUserPixel;

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
* 2018. 9. 19.   cshwang   Initial Release
*
*
************************************************************************************************/
public class RunEventUserHourlyProcess extends CommonDataAnalytics{
	
	private final static String EVENT_USER_PIXEL = "EventUserPixel";
	private final static String CONVERSION_RAW_DATA_PIXEL = "ConversionRawDataPixel";
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) {		
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();
		
		try{
			if(args.length == 0){
				new RawDataConversion(EACH, HOURLY).processDataSet(sparkSession);
				new EventUserPixel(EACH, HOURLY).processDataSet(sparkSession);
			}else if(args.length == 1) {
				new RawDataConversion(EACH, HOURLY).processDataSet(sparkSession, args[0]);
				new EventUserPixel(EACH, HOURLY).processDataSet(sparkSession, args[0]);
				
			}else if(args.length == 2) {
				switch(args[1]) {
					case EVENT_USER_PIXEL :
						new EventUserPixel(EACH, HOURLY).processDataSet(sparkSession, args[0]);
						break;
					case CONVERSION_RAW_DATA_PIXEL :
						new RawDataConversion(EACH, HOURLY).processDataSet(sparkSession, args[0]);
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
