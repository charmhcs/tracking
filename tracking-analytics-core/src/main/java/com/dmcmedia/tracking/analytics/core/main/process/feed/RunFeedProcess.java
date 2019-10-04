package com.dmcmedia.tracking.analytics.core.main.process.feed;

import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.AbstractDataProcess;
import com.dmcmedia.tracking.analytics.common.factory.SparkMongodbConnectionFactory;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.process.feed.ProductsFeedPixel;

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
* 2018. 2. 28.   cshwang   Initial Release
*
*
************************************************************************************************/
public class RunFeedProcess {
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) {		
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();
		
		try{
			if(args.length == 0){
				ProductsFeedPixel processFeed = new ProductsFeedPixel();
				processFeed.processDataSet(sparkSession);				
			  
			}else{
				ProductsFeedPixel processFeed = new ProductsFeedPixel(AbstractDataProcess.ALL);
				processFeed.processDataSet(sparkSession, args[0]);
			}			
		} catch (Exception e) {
			LogStack.batch.error(e);
		} finally{
			sparkSession.stop();
		}
	}	
}
