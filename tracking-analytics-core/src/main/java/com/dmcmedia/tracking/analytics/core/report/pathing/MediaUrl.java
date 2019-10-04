package com.dmcmedia.tracking.analytics.core.report.pathing;

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
* 2018. 6. 3.   cshwang   Initial Release
*
*
************************************************************************************************/
public class MediaUrl extends AbstractDataReport{

	/**
	 * 생성자
	 */
	public MediaUrl(){
		super();
	}	
	
	/**
	 * @param runType
	 */
	public MediaUrl(String runType){
		super(runType);
	}
	
	@Override
	public boolean reportsDataSet(SparkSession sparkSession) {
		// TODO Auto-generated method stub
		if(super.isValid() == false){
			return false;
		}
		
		Dataset<Row> resultDS 	= null;

		try{			
			if(EACH.equals(this.getRunType().name())){
				RawDataSQLTempView rawDataSQLTempView = new RawDataSQLTempView(getReportDate());
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
			}

			MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", getProcessedDataBase())
					.withOption("collection", this.getReportDate() + MongodbMap.COLLECTION_PIXEL_CONVERSION_RAWDATA), ConversionRawData.class)
					.createOrReplaceTempView("T_PIXEL_CONVERSION_RAWDATA");
			
			resultDS = sparkSession.sql("SELECT  V.reportDate           AS REPORT_DATE"
									+ "         ,TW.webId				AS WEB_ID"
									+ "         ,V.mediaId              AS MEDIA_ID"
									+ "         ,V.channelId            AS CHANNEL_ID"
									+ "         ,V.productsId           AS PRODUCTS_ID"
									+ "         ,V.campaignId           AS CAMPAIGN_ID"
									+ "         ,V.linkId           	AS LINK_ID"
									+ "         ,V.referrerUrl		    AS REFERRER_URL"
									+ "         ,V.pathingTypeCode 	    AS PATHING_TYPE_CODE"
									+ "         ,V.totalCount 			AS TOTAL_COUNT"
									+ "         ,V.audienceSize 		AS AUDIENCE_SIZE"
									+ " FROM (SELECT     A.reportDate"
									+ "                 ,A.pixelId"
									+ "                 ,A.mediaId"
									+ "                 ,A.channelId"
									+ "                 ,A.productsId"
									+ "                 ,A.campaignId"
									+ "                 ,A.linkId"
									+ "           		,A.referrerUrl "
									+ "                 ,A.pathingTypeCode "
									+ "         		,COUNT(pCid)            AS totalCount "
									+ "          		,COUNT(DISTINCT pCid)   AS audienceSize "
									+ "                 ,RANK() OVER (PARTITION BY A.pixelId ORDER BY COUNT(pCid) DESC, COUNT(DISTINCT pCid) DESC, A.referrerUrl DESC ) AS pt"
									+ "         FROM    (SELECT  processDate AS reportDate"
									+ "                         ,pixelId"
									+ "                         ,mediaId"
									+ "                         ,channelId"
									+ "                         ,productsId"
									+ "                         ,campaignId"
									+ "                         ,linkId"
									+ "          				,CASE WHEN channelId = 'chn_direct' THEN 'Direct' ELSE PARSE_URL(referrer, 'HOST')  END  AS referrerUrl "
									+ "              			,CASE WHEN channelId = 'chn_direct' THEN 'PTC001' ELSE 'PTC999' END                      AS pathingTypeCode"
									+ "              			,pCid"
									+ "           	    FROM    T_PIXEL_CONVERSION_RAWDATA"
									+ "                 WHERE   conversionRawdataTypeCode IN ('CRT002','CRT003', 'CRT005')"
									+ "          	) A "
									+ "         GROUP BY reportDate, pixelId,  mediaId, channelId, productsId,  productsId, campaignId,linkId, referrerUrl, pathingTypeCode"
									+ "     ) V"
									+ " INNER JOIN T_TRK_WEB TW  ON V.pixelId = TW.pixelId  "
									+ " WHERE   V.pt <= 100 ");
			
			resultDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), "tracking.TS_TRK_WEB_MEDIA_PATHING", SparkJdbcConnectionFactory.getDefaultJdbcOptions());
			
		}catch(Exception e){
			LogStack.report.error(ExceptionMap.REPORT_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.report.error(e);
		}
		return true;				
	}
}
