package com.dmcmedia.tracking.analytics.core.main.process.feed;

import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.factory.SparkMongodbConnectionFactory;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.process.feed.CategoriesPixel;

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
* 2018. 2. 27.   cshwang   Initial Release
*
*
************************************************************************************************/
public class RunCategoriesProcess {
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) {		
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();
		
		try{
			if(args.length == 0){
				CategoriesPixel processCategories = new CategoriesPixel();
				processCategories.processDataSet(sparkSession);				
			}else{
				CategoriesPixel processCategories = new CategoriesPixel();
				processCategories.processDataSet(sparkSession, args[0]);
			}			
		} catch (Exception e) {
			LogStack.batch.error(e);
		} finally{
			sparkSession.stop();
		}
	}	
}
