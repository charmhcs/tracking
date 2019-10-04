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
* 2019-07-01   cshwang  Initial Release
*
*
************************************************************************************************/
public class PurchaseAccumulation extends AbstractDataProcess{
	/**
	 * 생성자
	 */
	public PurchaseAccumulation(){
		super();
	}
	
	/**
	 * 생성자 
	 * @param processType
	 * @param runType
	 */
	public PurchaseAccumulation(String runType){
		super(runType);
	}
	@Override
	public boolean processDataSet(SparkSession sparkSession) {
		JavaSparkContext jsc = null;

		Dataset<EventAccumulation> userPurchaseDS = null;
		Dataset<User> evenetUserDS = null;
		Dataset<Row> resultUserDS = null;
		
		if(super.isValid() == false){
			return false;
		}

		try{			
			if(EACH.equals(this.getRunType().name())){
				//Empty
			}
			jsc = new JavaSparkContext(sparkSession.sparkContext());
			
			LogStack.process.debug(getPreviousProcessDate());
			
			userPurchaseDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database", MongodbMap.DATABASE_PROCESSED_USER).withOption("collection", getPreviousProcessDate() +  MongodbMap.COLLECTION_PIXEL_USER_PURCHASED)).toDS(EventAccumulation.class);
			evenetUserDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database",getProcessedDataBase()).withOption("collection", getProcessDate() +  MongodbMap.COLLECTION_PIXEL_EVENT_USER)).toDS(User.class);
			userPurchaseDS.createOrReplaceTempView("T_PIXEL_USER_PURCHASE");
			evenetUserDS.createOrReplaceTempView("T_PIXEL_EVENT_USER");
			
			resultUserDS = sparkSession.sql("SELECT  A._id"
					              + "				,A.pixelId 			AS px"
					              + "				,A.catalogId 		AS ct"
			                      + "             	,CAST(NVL(SUM(A.pageview), 0) AS BIGINT) 	AS pv "
			                      + "             	,CAST(NVL(SUM(A.viewcontent), 0) AS BIGINT) AS vc "
			                      + "             	,CAST(NVL(SUM(A.addtocart), 0) AS BIGINT) 	AS ac "
			                      + "             	,CAST(NVL(SUM(A.purchased), 0) AS BIGINT) 	AS pr "
			                      + "             	,CAST(NVL(SUM(A.purchasedValue), 0) AS DECIMAL(12,2)) 	AS ps "
			                      + "             	,MIN(A.processDate) AS pd "
			                      + "             	,MAX(A.updateDate) 	AS ud "
			                      + "        FROM ( SELECT   _id			"
			                      + "                       ,px 			AS pixelId"
			                      + "                       ,ct				AS catalogId "
			                      + "                       ,NVL(pv, 0)   	AS pageview "
			                      + "                       ,NVL(vc, 0)   	AS viewcontent "
			                      + "                       ,NVL(ac, 0)   	AS addtocart "
			                      + "                       ,NVL(pr, 0)   	AS purchased "
			                      + "                       ,NVL(ps, 0)   	AS purchasedValue "
			                      + "                       ,pd 			AS processDate "
			                      + "                       ,ud				AS updateDate "
			                      + "				FROM 	T_PIXEL_USER_PURCHASE T1 "
			                      + "				WHERE	ud > DATE_ADD(NOW(), -180) "
			                      + "         		UNION ALL"
			                      + "         		SELECT   pCid 					AS _id"
			                      + "               	    ,pixelId "
			                      + "                  		,catalogId "
			                      + "                   	,NVL(pageview, 0)		AS pageview "
			                      + "                   	,NVL(viewcontent, 0)	AS viewcontent "
			                      + "                   	,NVL(addtocart, 0)		AS addtocart "
			                      + "                   	,NVL(purchased, 0)    	AS purchased "
			                      + "                   	,NVL(purchasedValue, 0)    	AS purchasedValue "
			                      + "                   	,processDate 			AS processDate"
			                      + "                   	,processDate			AS updateDate "
			                      + "         		FROM 	T_PIXEL_EVENT_USER "
			                      + "		  		WHERE purchased > 0 "
			                      + "  ) A "
			                      + " GROUP BY A._id, A.pixelId, A.catalogId");

			MongodbFactory.dropAndCreateAndShardingCollection(MongodbMap.DATABASE_PROCESSED_USER, getProcessDate() + MongodbMap.COLLECTION_PIXEL_USER_PURCHASED );
			MongodbFactory.createCollectionCompoundIndex(MongodbMap.DATABASE_PROCESSED_USER, getProcessDate() + MongodbMap.COLLECTION_PIXEL_USER_PURCHASED ,new String[]{"px","ct","pc"});
			MongoSpark.write(resultUserDS).option("database", MongodbMap.DATABASE_PROCESSED_USER)
										.option("collection",getProcessDate() + MongodbMap.COLLECTION_PIXEL_USER_PURCHASED )
										.mode(DEFAULT_SAVEMODE)
										.save();
			// batchjob의 경우 7일전 콜랙션을 삭제한다.  
			if(BATCHJOB.equals(this.getRunType().name())){
				MongodbFactory.dropCollection(MongodbMap.DATABASE_PROCESSED_USER, getPrevious7DaysProcessedDate() + MongodbMap.COLLECTION_PIXEL_USER_PURCHASED);
			}
			
		}catch(Exception e){
			LogStack.process.error(ExceptionMap.PROCESS_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.process.error(e);
		}finally {
		}
		return true;
	}
}
