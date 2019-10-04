package com.dmcmedia.tracking.analytics.core.report.event;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.AbstractDataReport;
import com.dmcmedia.tracking.analytics.common.factory.SparkJdbcConnectionFactory;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.config.RawDataSQLTempView;

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
public class EventPopularityRanking extends AbstractDataReport{ 
	
	/**
	 * 생성자 
	 */
	public EventPopularityRanking(){
		super(EACH, DAILY);
	}
	
	/**
	 * @param runType
	 */
	public EventPopularityRanking(String runType){
		super(runType, DAILY);
	}
	
	public boolean reportsDataSet(SparkSession sparkSession) {
		
		Dataset<Row> resultDS = null;
		String targetRdbTableName = "TS_TRK_WEB_EVENT_POPULARITY_RANKING";
		if(super.isValid() == false){
			return false;
		}
		try{
			if(EACH.equals(this.getRunType().name())){
				RawDataSQLTempView rawDataSQLTempView = new RawDataSQLTempView(getReportDate());
				rawDataSQLTempView.createPixelRawDataDailyTempView(sparkSession);
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
			}
			
			resultDS = sparkSession.sql("SELECT  reportDate 		AS REPORT_DATE "
									+ " 		,TW.webId			AS WEB_ID "
									+ " 		,trackingEventCode 	AS PIXEL_EVENT_CODE "
									+ " 		,rank				AS RANK "
									+ " 		,href				AS URL "
									+ " 		,count				AS TOTAL_COUNT "
									+ " 		,audienceSize		AS AUDIENCE_SIZE "
									+ "  FROM (SELECT reportDate "
									+ " 			,pixelId "
									+ " 			,trackingEventCode "
									+ "     	   	,href "
									+ "     	    ,audienceSize "
									+ "          	,count "
									+ "          	,rank "
									+ "  	FROM (SELECT reportDate "
									+ " 				,pixelId "
									+ " 				,trackingEventCode "
									+ "          	    ,href "
									+ "              	,audienceSize "
									+ "              	,count "
									+ "              	,RANK() OVER (PARTITION BY pixelId  ORDER BY count DESC, audienceSize DESC, ids ASC) AS rank "
									+ "      	FROM (SELECT reportDate "
									+ " 					,pixelId "
									+ " 					,trackingEventCode "
									+ "             	    ,href "
									+ "                  	,MIN(_id)               AS ids "
									+ "                  	,COUNT(DISTINCT pCid)   AS audienceSize "
									+ "                  	,COUNT(href)            AS count "
									+ "          	FROM (SELECT  _id "
									+ " 						,trackingEventCode "
									+ " 						,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')      AS reportDate "
									+ "                 	    ,pageView.pixel_id   	AS pixelId "
									+ "                  	    ,pageView.p_cid        	AS pCid "
									+ "                       	,pageView.href         	AS href "
									+ "               	FROM   T_PIXEL_PAGE_VIEW ) A "
									+ "         	 GROUP BY reportDate, pixelId, trackingEventCode, href "
									+ "      	) B "
									+ "  	) C "
									+ "  	WHERE rank <= 100 "
									+ "  ) V "
									+ " INNER JOIN T_TRK_WEB TW  ON	V.pixelId = TW.pixelId");
			resultDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), targetRdbTableName, SparkJdbcConnectionFactory.getDefaultJdbcOptions());
		}catch(Exception e){
			LogStack.report.error(ExceptionMap.REPORT_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.report.error(e);
		}finally {

		}
		return true;
	}
}
