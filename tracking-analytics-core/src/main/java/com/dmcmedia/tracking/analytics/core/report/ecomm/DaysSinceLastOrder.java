package com.dmcmedia.tracking.analytics.core.report.ecomm;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.AbstractDataReport;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.map.MongodbMap;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
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
* 2018. 5. 4.   cshwang   Initial Release
*
*
************************************************************************************************/
public class DaysSinceLastOrder extends AbstractDataReport {
	/**
	 * 생성자
	 */
	public DaysSinceLastOrder(){
		super();
	}
	
	/**
	 * 생성자
	 */
	public DaysSinceLastOrder(String runType){
		super(runType);
	}	

	@Override
	public boolean reportsDataSet(SparkSession sparkSession) {
		// TODO Auto-generated method stub
		JavaSparkContext jsc 	= null;
		if(super.isValid() == false){
			return false;
		}
		
		Dataset<Row> resultDS 	= null;
		Dataset<Row> newUserCatalogListDS 	= null;
		Dataset<Row> userDS 	= null;
		Dataset<Row> newUserDS 	= null;
		SaveMode svaeMode = SaveMode.Overwrite;

		try{			
			jsc = new JavaSparkContext(sparkSession.sparkContext());
			
			newUserDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession)
								.withOption("database", MongodbMap.DATABASE_PROCESSED_MONTH + getReportMonth())
								.withOption("collection",  this.getReportDate() + MongodbMap.COLLECTION_PIXEL_NEWUSER)).toDF(User.class);
			newUserDS.createOrReplaceTempView("T_PIXEL_NEWUSER");
			newUserCatalogListDS = sparkSession.sql("SELECT DISTINCT(catalogId) AS catalogId FROM T_PIXEL_NEWUSER");
			List<String> catalogList =  newUserCatalogListDS.as(Encoders.STRING()).collectAsList();

			for(String feedCatalog : catalogList){
				LogStack.report.debug("Catalog Id: " + feedCatalog);
				userDS = MongoSpark.load(jsc, ReadConfig.create(sparkSession)
								.withOption("database", MongodbMap.DATABASE_PROCESSED_USER)
								.withOption("collection",  feedCatalog + MongodbMap.COLLECTION_PIXEL_USER)).toDF(User.class);
				userDS.createOrReplaceTempView("T_PIXEL_USER"); 
				
				resultDS = sparkSession.sql(" SELECT  A.catalogId AS catalogId"
										+ "			,CAST(FLOOR(MONTHS_BETWEEN('"+getReportDate()+"',A.processDate)) AS INT) AS month "
										+ "			,CAST(A.purchased AS BIGINT) AS purchase" 
										+ "         ,CAST(COUNT(A.pCid) AS LONG)  AS customerCount" 
										+ "         ,CAST(CASE WHEN A.purchased = 0  THEN '0' " 
										+ "               WHEN A.purchased = 1  THEN '1' " 
										+ "               WHEN A.purchased = 2  THEN '2' " 
										+ "               WHEN A.purchased = 3  THEN '3' " 
										+ "               WHEN A.purchased BETWEEN 4 AND 5 THEN '4-5' " 
										+ "               WHEN A.purchased BETWEEN 6 AND 9 THEN '6-9' " 
										+ "               ELSE '10+' END  AS STRING) AS purchaseCount " 
										+ "         ,CAST(CASE WHEN FLOOR(MONTHS_BETWEEN('"+getReportDate()+"',A.processDate)) = 0  THEN '0' " 
										+ "               WHEN FLOOR(MONTHS_BETWEEN('"+getReportDate()+"',A.processDate)) = 1  THEN '1' " 
										+ "               WHEN FLOOR(MONTHS_BETWEEN('"+getReportDate()+"',A.processDate)) = 2  THEN '2' " 
										+ "               WHEN FLOOR(MONTHS_BETWEEN('"+getReportDate()+"',A.processDate)) = 3  THEN '3' " 
										+ "               WHEN FLOOR(MONTHS_BETWEEN('"+getReportDate()+"',A.processDate)) BETWEEN 4 AND 6 THEN '4-6' " 
										+ "               WHEN FLOOR(MONTHS_BETWEEN('"+getReportDate()+"',A.processDate)) BETWEEN 7 AND 12 THEN '7-12' " 
										+ "               ELSE '12+' END  AS STRING) AS lastOrderCount " 
										+ "			,'"+getReportDate()+"' AS analysisDate"
										+ " FROM    T_PIXEL_USER A " 
										+ " GROUP BY A.catalogId, FLOOR(MONTHS_BETWEEN('"+getReportDate()+"',A.processDate)), A.purchased");

			    MongoSpark.write(resultDS).option("database", MongodbMap.DATABASE_ANALYSIS)
											.option("collection", MongodbMap.COLLECTION_ANALYSIS_PIXEL_DAYS_SINCE_LAST_ORDER )
											.mode(svaeMode)
											.save();
			    svaeMode = SaveMode.Append;
			}						
		}catch(Exception e){
			LogStack.report.error(ExceptionMap.REPORT_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.report.error(e);
		}
		return true;
	}
}