package com.dmcmedia.tracking.analytics.core.report.event;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.AbstractDataReport;
import com.dmcmedia.tracking.analytics.common.factory.SparkJdbcConnectionFactory;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.map.MongodbMap;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.config.RawDataSQLTempView;
import com.dmcmedia.tracking.analytics.core.dataset.event.ConversionRawData;
import com.dmcmedia.tracking.analytics.core.dataset.event.NewUser;
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
* 2018. 10. 11.   cshwang   Initial Release
*
*
************************************************************************************************/
public class EventNewUserChannels extends AbstractDataReport{
	/**
	 * 생성자 
	 * 	 
	 * */
	public EventNewUserChannels(){
		super();
	}
	
	/**
	 * 생성자 
	 * @param runType
	 */
	public EventNewUserChannels(String runType){
		super(runType);
	}
	
	public boolean reportsDataSet(SparkSession sparkSession) {
		
		Dataset<Row> resultDS 	= null;
		String targetRdbTableName = "tracking.TS_TRK_WEB_LINK_NEWUSER";
		
		if(super.isValid() == false){
			return false;
		}
		
		try{
			if(EACH.equals(this.getRunType().name())){
				RawDataSQLTempView rawDataSQLTempView = new RawDataSQLTempView(getReportDate());
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
			}
			MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", getProcessedDataBase())
					.withOption("collection", this.getReportDate() + MongodbMap.COLLECTION_PIXEL_CONVERSION_RAWDATA), ConversionRawData.class)
					.createOrReplaceTempView("T_PIXEL_CONVERSION_RAWDATA");
			MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", getProcessedDataBase())
					.withOption("collection", this.getReportDate() + MongodbMap.COLLECTION_PIXEL_NEWUSER), NewUser.class)
					.createOrReplaceTempView("T_PIXEL_NEWUSER");
		
			resultDS = sparkSession.sql("SELECT  V.reportDate 		    AS REPORT_DATE "
									+ " 		,V.reportHour		    AS REPORT_HOUR "
									+ " 		,TW.webId			    AS WEB_ID "
									+ " 		,NVL(V.mediaId,'')	    AS MEDIA_ID "
									+ " 		,NVL(V.channelId,'')	AS CHANNEL_ID "
									+ "         ,NVL(V.productsId,'')	AS PRODUCTS_ID "
									+ " 		,NVL(V.campaignId,'')	AS CAMPAIGN_ID "
									+ " 		,NVL(V.linkId,'')		AS LINK_ID "
									+ " 		,NVL(V.newUser, 0)	    AS NEW_USER "
									+ " FROM	(SELECT  A.reportDate "
									+ "                 ,A.reportHour "
									+ "                 ,A.pixelId "
									+ "                 ,A.deviceTypeCode"
									+ "                 ,D.conversionRawdataTypeCode"
									+ "                 ,D.mediaId"
									+ "                 ,D.channelId"
									+ "                 ,D.productsId"
									+ "                 ,D.campaignId"
									+ "                 ,D.linkId"
									+ "                 ,COUNT(DISTINCT A.pCid) AS newUser "
									+ "         FROM  "
									+ "             (SELECT  reportDate"
									+ "                     ,reportHour"
									+ "                     ,pixelId"
									+ "                     ,deviceTypeCode"
									+ "                     ,pCid"
									+ "             FROM (SELECT  DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')      AS reportDate "
									+ "                     ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS reportHour "
									+ "                     ,pixelId "
									+ "                     ,deviceTypeCode "
									+ "                     ,pCid "
									+ "                     ,RANK() OVER (PARTITION BY pixelId, pCid  ORDER BY logDateTime ASC, _id ASC) AS ranks"
									+ "                 FROM    T_PIXEL_NEWUSER "
									+ "                 )"
									+ "             WHERE ranks = 1"
									+ "             ) A "
									+ "             LEFT OUTER JOIN "
									+ "             (SELECT  C.reportDate "
									+ "         			,C.reportHour "
									+ "                     ,C.pixelId "
									+ "                     ,C.mediaId "
									+ "         			,C.channelId "
									+ "                     ,C.productsId "
									+ "                     ,C.campaignId "
									+ "                     ,C.linkId "
									+ "                     ,C.deviceTypeCode "
									+ "                     ,C.conversionRawdataTypeCode "
									+ "                     ,C.pCid "
									+ "         	FROM (SELECT B.pixelId "
									+ "                         ,B.mediaId "
									+ "                         ,B.channelId "
									+ "                         ,B.productsId "
									+ "                         ,B.campaignId "
									+ "                         ,B.linkId "
									+ "                         ,B.deviceTypeCode "
									+ "                         ,B.conversionRawdataTypeCode "
									+ "                         ,B.pCid "
									+ "                         ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(B.logDateTime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd')      AS reportDate"
									+ "                         ,DATE_FORMAT(CAST(UNIX_TIMESTAMP(B.logDateTime, 'yyyy-MM-dd HH') AS TIMESTAMP), 'HH')                 AS reportHour"
									+ "                         ,RANK() OVER (PARTITION BY B.pixelId, B.pCid  ORDER BY  B.isoDate ASC, B._id ASC) AS ranks"
									+ "                 FROM    T_PIXEL_CONVERSION_RAWDATA B) C "
									+ "                 WHERE   C.ranks = 1 "
									+ "              ) D "
									+ "         ON  A.reportDate = D.reportDate AND A.reportHour = D.reportHour AND A.pixelId = D.pixelId AND A.deviceTypeCode = D.deviceTypeCode AND  A.pCid = D.pCid "
									+ "         GROUP BY A.reportDate, A.reportHour, A.pixelId, A.deviceTypeCode , D.conversionRawdataTypeCode, D.mediaId, D.channelId , D.productsId , D.campaignId , D.linkId"
									+ " 	) V "
									+ " INNER JOIN T_TRK_WEB TW  ON	V.pixelId = TW.pixelId " 
									+ "");
			resultDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), targetRdbTableName, SparkJdbcConnectionFactory.getDefaultJdbcOptions());
		}catch(Exception e){
			LogStack.report.error(e);
			new Exception(ExceptionMap.REPORT_EXCEPTION);
		}finally {

		}
		return true;
	}
}