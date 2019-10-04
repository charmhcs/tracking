package com.dmcmedia.tracking.analytics.core.report.ecomm;

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
* 2018. 10. 4.   cshwang   Initial Release
*
*
************************************************************************************************/
public class EcommReport extends AbstractDataReport  {
	
	/**
	 * 생성자 
	 */
	public EcommReport(){
		super();
	}
	
	/**
	 * 생성자
	 * @param runType
	 */
	public EcommReport(String runType){
		super(runType);
	}
	
	/**
	 * 생성자
	 * @param runType
	 * @param reportType
	 */
	public EcommReport(String runType, String reportType){
		super(runType, reportType);
	}
		
	public boolean reportsDataSet(SparkSession sparkSession) {
			
		Dataset<Row> resultHourlyDS 	= null;
		Dataset<Row> resultDailyDS 		= null;
		String targetHourlyTableName 	= "tracking.TS_TRK_WEB_ECOMM_HOURLY";
		String targetDailyTableName 	= "tracking.TS_TRK_WEB_ECOMM_DAILY";
			
		if(super.isValid() == false){
			return false;
		}
		try{
			if(EACH.equals(this.getRunType().name())){
				RawDataSQLTempView rawDataSQLTempView = new RawDataSQLTempView(getReportDate());
				rawDataSQLTempView.createPixelRawDataDailyTempView(sparkSession);
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
			}
			
			switch(getReportType().name()) {
				case HOURLY :
					resultHourlyDS = sparkSession.sql(getHourlyQuery());
					resultHourlyDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), targetHourlyTableName, SparkJdbcConnectionFactory.getDefaultJdbcOptions());
					break;
				case DAILY :
					resultDailyDS = sparkSession.sql(getDailyQuery());
					resultDailyDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), targetDailyTableName, SparkJdbcConnectionFactory.getDefaultJdbcOptions());
					break;
				case ALL :
					resultHourlyDS = sparkSession.sql(getHourlyQuery());
					resultHourlyDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), targetHourlyTableName, SparkJdbcConnectionFactory.getDefaultJdbcOptions());
					resultDailyDS = sparkSession.sql(getDailyQuery());
					resultDailyDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), targetDailyTableName, SparkJdbcConnectionFactory.getDefaultJdbcOptions());
					break;
				default :
					new Exception(ExceptionMap.INVALID_DATA_FORMAT_EXCEPTION);
					return false;
		    }	
			
		}catch(Exception e){
			LogStack.report.error(ExceptionMap.REPORT_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.report.error(e);
		}finally {

		}
		return true;
	}

	/**
	 * @param reportType
	 * @return
	 */
	private String getDailyQuery() {
		StringBuffer queryString = new StringBuffer();
		queryString.append("SELECT   V3.webId					AS WEB_ID "
						+ "         ,V1.reportDate 				AS REPORT_DATE"
						+ "			,V2.sessionPageview 		AS SESSION_PAGE_VIEW "
						+ "        	,V2.sessionViewcontent 		AS SESSION_VIEW_CONTENT "
						+ "        	,V2.sessionAddtocart 		AS SESSION_ADD_TO_CART "
						+ "        	,V2.sessionUniquePurchased 	AS SESSION_PURCHASE "
						+ "        	,V1.uniquePageview 			AS UNIQUE_PAGE_VIEW "
						+ "        	,V1.uniqueViewcontent 		AS UNIQUE_VIEW_CONTENT "
						+ "        	,V1.uniqueAddtocart 		AS UNIQUE_ADD_TO_CART "
						+ "        	,V1.uniquePurchased 		AS UNIQUE_PURCHASE "
						+ " FROM (SELECT A.catalogId "
						+ "             ,A.reportDate "
						+ "           	,COUNT(CASE WHEN A.eventType = 'PAGE_VIEW'     THEN A.pCid    END)	AS pageview "
						+ "             ,COUNT(CASE WHEN A.eventType = 'VIEW_CONTENT'  THEN A.pCid    END)	AS viewcontent "
						+ "             ,COUNT(CASE WHEN A.eventType = 'ADD_TO_CART'   THEN A.pCid    END)	AS addtocart "
						+ "        		,COUNT(CASE WHEN A.eventType = 'PURCHASED'	   THEN A.pCid    END)	AS purchased "
						+ "        		,COUNT(DISTINCT CASE WHEN A.eventType = 'PAGE_VIEW'     THEN A.pCid    END)	AS uniquePageview "
						+ "             ,COUNT(DISTINCT CASE WHEN A.eventType = 'VIEW_CONTENT'  THEN A.pCid    END)	AS uniqueViewcontent "
						+ "             ,COUNT(DISTINCT CASE WHEN A.eventType = 'ADD_TO_CART'   THEN A.pCid    END)	AS uniqueAddtocart "
						+ "        		,COUNT(DISTINCT CASE WHEN A.eventType = 'PURCHASED'	   THEN A.pCid    END)	AS uniquePurchased "
						+ "       FROM (	SELECT	'PAGE_VIEW'				AS	eventType "
						+ "           				,pageView.catalog_id	AS	catalogId "
						+ "           			    ,pageView.p_cid		    AS	pCid "
						+ "           			    ,logDateTime            AS  logDateTime "
						+ "           			    ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')   AS reportDate "
						+ "                   		,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')              AS reportHour "
						+ "           		FROM	T_PIXEL_PAGE_VIEW   "
						+ "             "
						+ "           		UNION ALL  "
						+ "             "
						+ "                SELECT	 'VIEW_CONTENT'         AS eventType "
						+ "           				,viewContent.catalog_id	AS catalogId "
						+ "           				,viewContent.p_cid      AS pCid "
						+ "           				,logDateTime "
						+ "           				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')   AS reportDate "
						+ "                   		,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')              AS reportHour "
						+ "           		FROM	T_PIXEL_VIEW_CONTENT "
						+ "             "
						+ "           		UNION ALL  "
						+ "             "
						+ "                SELECT	 'ADD_TO_CART'			AS	eventType "
						+ "           				,addToCart.catalog_id	AS	catalogId "
						+ "           				,addToCart.p_cid		AS	pCid "
						+ "           				,logDateTime "
						+ "           				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')   AS reportDate "
						+ "                   		,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')              AS reportHour "
						+ "           		FROM	T_PIXEL_ADD_TO_CART "
						+ "          "
						+ "           		UNION ALL  "
						+ "             "
						+ "           		SELECT	 'PURCHASED'            AS	eventType "
						+ "           				,catalogId "
						+ "           				,pCid "
						+ "           				,logDateTime "
						+ "           				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')   AS reportDate "
						+ "                   		,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')              AS reportHour "
						+ "           		FROM	T_PIXEL_PURCHASED "
						+ "           		) A  "
						+ " GROUP BY A.catalogId, A.reportDate ) V1 "
						+ " INNER JOIN  "
						+ "         "
						+ "        (SELECT   AA.catalogId "
						+ "                	,AA.reportDate "
						+ "          		,SUM(AA.uniquePageview)	    AS sessionPageview "
						+ "                	,SUM(AA.uniqueViewcontent)	AS sessionViewcontent "
						+ "                	,SUM(AA.uniqueAddtocart)		AS sessionAddtocart "
						+ "                	,SUM(AA.uniquePurchased)    	AS sessionUniquePurchased "
						+ "        FROM ( "
						+ "                SELECT	A.catalogId "
						+ "                        ,A.reportDate "
						+ "                        ,A.reportHour "
						+ "                        ,A.half "
						+ "                   		,COUNT(DISTINCT CASE WHEN A.eventType = 'PAGE_VIEW'     THEN A.pCid    END)	AS uniquePageview "
						+ "                        ,COUNT(DISTINCT CASE WHEN A.eventType = 'VIEW_CONTENT'  THEN A.pCid    END)	AS uniqueViewcontent "
						+ "                        ,COUNT(DISTINCT CASE WHEN A.eventType = 'ADD_TO_CART'   THEN A.pCid    END)	AS uniqueAddtocart "
						+ "                        ,COUNT(DISTINCT CASE WHEN A.eventType = 'PURCHASED'   THEN A.pCid    END)	AS uniquePurchased "
						+ "                FROM (	SELECT	'PAGE_VIEW'				AS	eventType "
						+ "                   				,pageView.catalog_id	AS	catalogId "
						+ "                   			    ,pageView.p_cid		    AS	pCid "
						+ "                   			    ,logDateTime            AS  logDateTime "
						+ "                   			    ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')   	AS reportDate "
						+ "                   			    ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')               	AS reportHour "
						+ "                                ,FLOOR(DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'mm')  / 30) 	AS half "
						+ "                   		FROM	T_PIXEL_PAGE_VIEW   "
						+ "                     "
						+ "                   		UNION ALL  "
						+ "                     "
						+ "                        SELECT	 'VIEW_CONTENT'         AS eventType "
						+ "                   				,viewContent.catalog_id	AS catalogId "
						+ "                   				,viewContent.p_cid      AS pCid "
						+ "                   				,logDateTime "
						+ "                   				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')   	AS reportDate "
						+ "                   				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS reportHour "
						+ "                                ,FLOOR(DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'mm')  / 30) 	AS half "
						+ "                   		FROM	T_PIXEL_VIEW_CONTENT "
						+ "                     "
						+ "                   		UNION ALL  "
						+ "                     "
						+ "                        SELECT	 'ADD_TO_CART'			AS	eventType "
						+ "                   				,addToCart.catalog_id	AS	catalogId "
						+ "                   				,addToCart.p_cid		AS	pCid "
						+ "                   				,logDateTime "
						+ "                   				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')   	AS reportDate "
						+ "                   				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS reportHour "
						+ "                                ,FLOOR(DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'mm')  / 30) 	AS half "
						+ "                   		FROM	T_PIXEL_ADD_TO_CART "
						+ "                  "
						+ "                   		UNION ALL  "
						+ "                     "
						+ "                   		SELECT	 'PURCHASED'            AS	eventType "
						+ "                   				,catalogId "
						+ "                   				,pCid "
						+ "                   				,logDateTime "
						+ "                   				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')   	AS reportDate "
						+ "                   				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS reportHour "
						+ "                                ,FLOOR(DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'mm')  / 30) 	AS half "
						+ "                   		FROM	T_PIXEL_PURCHASED "
						+ "                   		) A  "
				    	+ "                	GROUP BY A.catalogId, A.reportDate, A.reportHour, A.half "
						+ "        ) AA "
						+ "        GROUP BY AA.catalogId, AA.reportDate) V2 "
						+ "		ON 	   	V1.catalogId 	= V2.catalogId  AND    	V1.reportDate 	= V2.reportDate "
						+ "		INNER JOIN T_TRK_WEB V3   ON		V1.catalogId 	= V3.catalogId ");
		return queryString.toString();
	}
	
	private String getHourlyQuery() {
		StringBuffer queryString = new StringBuffer();
		queryString.append("SELECT   V3.webId					AS WEB_ID "
						+ "         ,V1.reportDate 				AS REPORT_DATE"
						+ "			,V1.reportHour 				AS REPORT_HOUR "
						+ "			,V1.pageview 				AS PAGE_VIEW "
						+ "        	,V1.viewContent 			AS VIEW_CONTENT "
						+ "        	,V1.addtocart 				AS ADD_TO_CART "
						+ "        	,V1.purchased 				AS PURCHASE "
						+ "			,V2.sessionPageview 		AS SESSION_PAGE_VIEW "
						+ "        	,V2.sessionViewcontent 		AS SESSION_VIEW_CONTENT "
						+ "        	,V2.sessionAddtocart 		AS SESSION_ADD_TO_CART "
						+ "        	,V2.sessionUniquePurchased 	AS SESSION_PURCHASE "
						+ "        	,V1.uniquePageview 			AS UNIQUE_PAGE_VIEW "
						+ "        	,V1.uniqueViewcontent 		AS UNIQUE_VIEW_CONTENT "
						+ "        	,V1.uniqueAddtocart 		AS UNIQUE_ADD_TO_CART "
						+ "        	,V1.uniquePurchased 		AS UNIQUE_PURCHASE "
						+ " FROM (SELECT A.catalogId "
						+ "             ,A.reportDate "
						+ "				,A.reportHour 	"
						+ "           	,COUNT(CASE WHEN A.eventType = 'PAGE_VIEW'     THEN A.pCid    END)	AS pageview "
						+ "             ,COUNT(CASE WHEN A.eventType = 'VIEW_CONTENT'  THEN A.pCid    END)	AS viewcontent "
						+ "             ,COUNT(CASE WHEN A.eventType = 'ADD_TO_CART'   THEN A.pCid    END)	AS addtocart "
						+ "        		,COUNT(CASE WHEN A.eventType = 'PURCHASED'	   THEN A.pCid    END)	AS purchased "
						+ "        		,COUNT(DISTINCT CASE WHEN A.eventType = 'PAGE_VIEW'     THEN A.pCid    END)	AS uniquePageview "
						+ "             ,COUNT(DISTINCT CASE WHEN A.eventType = 'VIEW_CONTENT'  THEN A.pCid    END)	AS uniqueViewcontent "
						+ "             ,COUNT(DISTINCT CASE WHEN A.eventType = 'ADD_TO_CART'   THEN A.pCid    END)	AS uniqueAddtocart "
						+ "        		,COUNT(DISTINCT CASE WHEN A.eventType = 'PURCHASED'	   THEN A.pCid    END)	AS uniquePurchased "
						+ "       FROM (	SELECT	'PAGE_VIEW'				AS	eventType "
						+ "           				,pageView.catalog_id	AS	catalogId "
						+ "           			    ,pageView.p_cid		    AS	pCid "
						+ "           			    ,logDateTime            AS  logDateTime "
						+ "           			    ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')   AS reportDate "
						+ "                   		,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')              AS reportHour "
						+ "           		FROM	T_PIXEL_PAGE_VIEW   "
						+ "             "
						+ "           		UNION ALL  "
						+ "             "
						+ "                SELECT	 'VIEW_CONTENT'         AS eventType "
						+ "           				,viewContent.catalog_id	AS catalogId "
						+ "           				,viewContent.p_cid      AS pCid "
						+ "           				,logDateTime "
						+ "           				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')   AS reportDate "
						+ "                   		,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')              AS reportHour "
						+ "           		FROM	T_PIXEL_VIEW_CONTENT "
						+ "             "
						+ "           		UNION ALL  "
						+ "             "
						+ "                SELECT	 'ADD_TO_CART'			AS	eventType "
						+ "           				,addToCart.catalog_id	AS	catalogId "
						+ "           				,addToCart.p_cid		AS	pCid "
						+ "           				,logDateTime "
						+ "           				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')   AS reportDate "
						+ "                   		,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')              AS reportHour "
						+ "           		FROM	T_PIXEL_ADD_TO_CART "
						+ "          "
						+ "           		UNION ALL  "
						+ "             "
						+ "           		SELECT	 'PURCHASED'            AS	eventType "
						+ "           				,catalogId "
						+ "           				,pCid "
						+ "           				,logDateTime "
						+ "           				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')   AS reportDate "
						+ "                   		,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')              AS reportHour "
						+ "           		FROM	T_PIXEL_PURCHASED "
						+ "           		) A  "
						+ " 		WHERE A.logDateTime LIKE '"+getReportDateHour()+"%' "
						+ "			GROUP BY A.catalogId, A.reportDate, A.reportHour ) V1 "
						+ " INNER JOIN  "
						+ "         "
						+ "        (SELECT   AA.catalogId "
						+ "                	,AA.reportDate "
						+ "					,AA.reportHour "
						+ "          		,SUM(AA.uniquePageview)	    AS sessionPageview "
						+ "                	,SUM(AA.uniqueViewcontent)	AS sessionViewcontent "
						+ "                	,SUM(AA.uniqueAddtocart)		AS sessionAddtocart "
						+ "                	,SUM(AA.uniquePurchased)    	AS sessionUniquePurchased "
						+ "        FROM ( "
						+ "                SELECT	A.catalogId "
						+ "                        ,A.reportDate "
						+ "                        ,A.reportHour "
						+ "                        ,A.half "
						+ "                   		,COUNT(DISTINCT CASE WHEN A.eventType = 'PAGE_VIEW'     THEN A.pCid    END)	AS uniquePageview "
						+ "                        ,COUNT(DISTINCT CASE WHEN A.eventType = 'VIEW_CONTENT'  THEN A.pCid    END)	AS uniqueViewcontent "
						+ "                        ,COUNT(DISTINCT CASE WHEN A.eventType = 'ADD_TO_CART'   THEN A.pCid    END)	AS uniqueAddtocart "
						+ "                        ,COUNT(DISTINCT CASE WHEN A.eventType = 'PURCHASED'   THEN A.pCid    END)	AS uniquePurchased "
						+ "                FROM (	SELECT	'PAGE_VIEW'				AS	eventType "
						+ "                   				,pageView.catalog_id	AS	catalogId "
						+ "                   			    ,pageView.p_cid		    AS	pCid "
						+ "                   			    ,logDateTime            AS  logDateTime "
						+ "                   			    ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')   	AS reportDate "
						+ "                   			    ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')               	AS reportHour "
						+ "                                ,FLOOR(DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'mm')  / 30) 	AS half "
						+ "                   		FROM	T_PIXEL_PAGE_VIEW   "
						+ "                     "
						+ "                   		UNION ALL  "
						+ "                     "
						+ "                        SELECT	 'VIEW_CONTENT'         AS eventType "
						+ "                   				,viewContent.catalog_id	AS catalogId "
						+ "                   				,viewContent.p_cid      AS pCid "
						+ "                   				,logDateTime "
						+ "                   				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')   	AS reportDate "
						+ "                   				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS reportHour "
						+ "                                ,FLOOR(DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'mm')  / 30) 	AS half "
						+ "                   		FROM	T_PIXEL_VIEW_CONTENT "
						+ "                     "
						+ "                   		UNION ALL  "
						+ "                     "
						+ "                        SELECT	 'ADD_TO_CART'			AS	eventType "
						+ "                   				,addToCart.catalog_id	AS	catalogId "
						+ "                   				,addToCart.p_cid		AS	pCid "
						+ "                   				,logDateTime "
						+ "                   				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')   	AS reportDate "
						+ "                   				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS reportHour "
						+ "                                ,FLOOR(DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'mm')  / 30) 	AS half "
						+ "                   		FROM	T_PIXEL_ADD_TO_CART "
						+ "                  "
						+ "                   		UNION ALL  "
						+ "                     "
						+ "                   		SELECT	 'PURCHASED'            AS	eventType "
						+ "                   				,catalogId "
						+ "                   				,pCid "
						+ "                   				,logDateTime "
						+ "                   				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')   	AS reportDate "
						+ "                   				,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS reportHour "
						+ "                                ,FLOOR(DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'mm')  / 30) 	AS half "
						+ "                   		FROM	T_PIXEL_PURCHASED "
						+ "                   		) A  "
						+ "              	WHERE A.logDateTime LIKE '"+getReportDateHour()+"%'  "
						+ "                	GROUP BY A.catalogId, A.reportDate, A.reportHour, A.half "
						+ "        ) AA "
						+ "        GROUP BY AA.catalogId, AA.reportDate, AA.reportHour) V2 "
						+ "		ON 	   	V1.catalogId 	= V2.catalogId  AND    	V1.reportDate 	= V2.reportDate   AND    V1.reportHour 	= V2.reportHour "
						+ "		INNER JOIN T_TRK_WEB V3   ON		V1.catalogId 	= V3.catalogId");
		return queryString.toString();
	}
}
