package com.dmcmedia.tracking.analytics.core.process.event;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.AbstractDataProcess;
import com.dmcmedia.tracking.analytics.common.factory.MongodbFactory;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.map.MongodbMap;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.dataset.event.EventAccumulation;
import com.dmcmedia.tracking.analytics.core.dataset.event.User;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

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
* 2018-12-25   cshwang  Initial Release
*
*
************************************************************************************************/
public class EventNewUserPixel  extends AbstractDataProcess{
	/**
	 * 생성자
	 */
	public EventNewUserPixel(){
		super();
	}
	
	/**
	 * 생성자 
	 * @param processType
	 * @param runType
	 */
	public EventNewUserPixel(String runType){
		super(runType);
	}
	
	@Override
	public boolean processDataSet(SparkSession sparkSession) {
		JavaSparkContext jsc = null;

		Dataset<EventAccumulation> previousUserDS = null;
		Dataset<User> evenetUserDS = null;
		Dataset<Row> resultNewUserDS = null;
		
		if(super.isValid() == false){
			return false;
		}

		try{			
			jsc = new JavaSparkContext(sparkSession.sparkContext());
			if(EACH.equals(this.getRunType().name())){
				//Empty
			}
			
			previousUserDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database", MongodbMap.DATABASE_PROCESSED_USER).withOption("collection", getPreviousProcessDate() +  MongodbMap.COLLECTION_PIXEL_USER)).toDS(EventAccumulation.class);
			evenetUserDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database",getProcessedDataBase()).withOption("collection", getProcessDate() +  MongodbMap.COLLECTION_PIXEL_EVENT_USER)).toDS(User.class);
			previousUserDS.createOrReplaceTempView("T_PIXEL_USER_PREVIOUS");
			evenetUserDS.createOrReplaceTempView("T_PIXEL_EVENT_USER");

			resultNewUserDS = sparkSession.sql("SELECT   A.pixelId"
											+ "			,A.catalogId " 
											+ "        	,A.pCid "
											+ "			,A.deviceTypeCode"
											+ "			,A.conversionRawdataTypeCode" 
											+ "        	,A.pageview " 
											+ "        	,A.viewcontent " 
											+ "        	,A.addtocart " 
											+ "        	,A.purchased " 
											+ "		   	,A.logDateTime "	
											+ "        	,A.processDate " 
											+ " FROM 	T_PIXEL_EVENT_USER A " 
											+ " WHERE 	NOT EXISTS (SELECT 	0 "
											+ "						FROM 	T_PIXEL_USER_PREVIOUS B "
											+ "						WHERE 	A.pixelId 	= B.px "
											+ "						AND 	A.catalogId = B.ct "
											+ "						AND 	A.pCid 		= B.pc ) "
											+ "");
			
			MongodbFactory.dropAndCreateAndShardingCollection(getProcessedDataBase(), getProcessDate() + MongodbMap.COLLECTION_PIXEL_NEWUSER );
			MongodbFactory.createCollectionCompoundIndex(getProcessedDataBase(), getProcessDate() + MongodbMap.COLLECTION_PIXEL_NEWUSER  ,new String[]{"pixelId","catalogId","pCid"});
			MongoSpark.write(resultNewUserDS).option("database", getProcessedDataBase())
										.option("collection",getProcessDate() + MongodbMap.COLLECTION_PIXEL_NEWUSER )
										.mode(DEFAULT_SAVEMODE)
										.save();
		}catch(Exception e){
			LogStack.process.error(ExceptionMap.PROCESS_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.process.error(e);
		}finally {
			
		}
		return true;
	}
}
