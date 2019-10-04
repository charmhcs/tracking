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
public class EventUserAccumulation  extends AbstractDataProcess{

	/**
	 * 생성자
	 */
	public EventUserAccumulation(){
		super();
	}
	
	/**
	 * 생성자 
	 * @param processType
	 * @param runType
	 */
	public EventUserAccumulation(String runType){
		super(runType);
	}
	@Override
	public boolean processDataSet(SparkSession sparkSession) {
		JavaSparkContext jsc = null;

		Dataset<EventAccumulation> previousUserDS = null;
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
			
			previousUserDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database", MongodbMap.DATABASE_PROCESSED_USER).withOption("collection", getPreviousProcessDate() +  MongodbMap.COLLECTION_PIXEL_USER)).toDS(EventAccumulation.class);
			evenetUserDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession).withOption("database",getProcessedDataBase()).withOption("collection", getProcessDate() +  MongodbMap.COLLECTION_PIXEL_EVENT_USER)).toDS(User.class);
			previousUserDS.createOrReplaceTempView("T_PIXEL_USER_PREVIOUS");
			evenetUserDS.createOrReplaceTempView("T_PIXEL_EVENT_USER");
			
			resultUserDS = sparkSession.sql("SELECT  A.pixelId 			AS px"
									+ "				,A.catalogId 		AS ct"
				                    + "             ,A.pCid 			AS pc"
				                    + "             ,SUM(A.pageview)    AS pv "
				                    + "             ,SUM(A.viewcontent) AS vc "
				                    + "             ,SUM(A.addtocart)   AS ac "
				                    + "             ,SUM(A.purchased)   AS pr "
				                    + "             ,MIN(A.processDate) AS pd "
				                    + "             ,MAX(A.processDate) AS ud "
				                    + "        FROM ( SELECT   'user'     	AS event "
				                    + "                       ,px 			AS pixelId"
				                    + "                       ,ct			AS catalogId "
				                    + "                       ,pc			AS pCid "
				                    + "                       ,NVL(pv, 0)   AS pageview "
				                    + "                       ,NVL(vc, 0)   AS viewcontent "
				                    + "                       ,NVL(ac, 0)   AS addtocart "
				                    + "                       ,NVL(pr, 0)   AS purchased "
				                    + "                       ,pd 			AS processDate "
				                    + "                       ,ud			AS updateDate "
				                    + "             FROM T_PIXEL_USER_PREVIOUS   "
				                    + "				WHERE (pd = ud  AND pr = 0 AND ac = 0 AND ud >  DATE_ADD(NOW(), -31)) OR ((pd != ud OR pr > 0 OR ac > 0)  AND ud > DATE_ADD(NOW(), -180))  "
				                    + "             UNION ALL"
				                    + "             SELECT   'eventUser' AS event "
				                    + "                       ,pixelId "
				                    + "                       ,catalogId "
				                    + "                       ,pCid "
				                    + "                       ,NVL(pageview, 0)		AS pageview "
				                    + "                       ,NVL(viewcontent, 0)	AS viewcontent "
				                    + "                       ,NVL(addtocart, 0)	AS addtocart "
				                    + "                       ,NVL(purchased, 0)    AS purchased "
				                    + "                       ,processDate "
				                    + "                       ,processDate			AS updateDate "
				                    + "               FROM 	T_PIXEL_EVENT_USER "
				                    + "  ) A "
				                    + " WHERE TRIM(A.pixelId) != ''  "
				                    + " GROUP BY A.pixelId, A.catalogId, A.pCid ");

			MongodbFactory.dropAndCreateAndShardingCollection(MongodbMap.DATABASE_PROCESSED_USER, getProcessDate() + MongodbMap.COLLECTION_PIXEL_USER );
			MongodbFactory.createCollectionCompoundIndex(MongodbMap.DATABASE_PROCESSED_USER, getProcessDate() + MongodbMap.COLLECTION_PIXEL_USER ,new String[]{"px","ct","pc"});
			MongoSpark.write(resultUserDS).option("database", MongodbMap.DATABASE_PROCESSED_USER)
										.option("collection",getProcessDate() + MongodbMap.COLLECTION_PIXEL_USER )
										.mode(DEFAULT_SAVEMODE)
										.save();
			// batchjob의 경우 7일전 콜랙션을 삭제한다.  
			if(BATCHJOB.equals(this.getRunType().name())){
				MongodbFactory.dropCollection(MongodbMap.DATABASE_PROCESSED_USER, getPrevious7DaysProcessedDate() + MongodbMap.COLLECTION_PIXEL_USER);
			}
			
		}catch(Exception e){
			LogStack.process.error(ExceptionMap.PROCESS_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.process.error(e);
		}finally {
		}
		return true;
	}
}
