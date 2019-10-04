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
public class MediaKeyword extends AbstractDataReport{
	/**
	 * 생성자
	 */
	public MediaKeyword(){
		super();
	}
	
	/**
	 * 생성
	 * @param runType
	 */
	public MediaKeyword(String runType) {
		super(runType);
	}

	/**
	 * @param sparkSession
	 * @return
	 */
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
			
			resultDS = sparkSession.sql("SELECT  A.processDate          AS REPORT_DATE"
									+ "         ,TW.webId               AS WEB_ID "
									+ " 	    ,A.mediaId				AS MEDIA_ID "
									+ "         ,A.channelId			AS CHANNEL_ID  "
									+ "         ,A.productsId           AS PRODUCTS_ID"
									+ "         ,A.campaignId           AS CAMPAIGN_ID"
									+ "         ,A.linkId               AS LINK_ID"
									+ "         ,A.searchMedia          AS SEARCH_MEDIA"
									+ "         ,A.deviceTypeCode		AS DEVICE_TYPE_CODE  "
									+ "         ,A.keyword				AS KEYWORD  "
									+ "         ,A.totalCount       	AS TOTAL_COUNT  "
									+ "         ,A.audienceSize			AS AUDIENCE_SIZE"
									+ " FROM (SELECT processDate"
									+ "             ,pixelId"
									+ "     		,mediaId "
									+ "     		,channelId  "
									+ "      	  	,productsId"
									+ "      	  	,campaignId"
									+ "      	  	,linkId"
									+ "      	  	,deviceTypeCode"
									+ "      	  	,PARSE_URL(referrer, 'HOST')                AS searchMedia"
									+ "      	  	,TRIM(LOWER(SUBSTRING(utmTerm, 0 , 127))) 	AS keyword  "
									+ "      	  	,COUNT(pCid)            	AS totalCount  "
									+ "      	  	,COUNT(DISTINCT(pCid))  	AS audienceSize  "
									+ "      	  	,RANK() OVER (PARTITION BY pixelId  ORDER BY COUNT(pCid) DESC, COUNT(DISTINCT(pCid)) DESC, TRIM(LOWER(SUBSTRING(utmTerm, 0 , 127))) ASC) AS pt  "
									+ "     FROM    T_PIXEL_CONVERSION_RAWDATA"
									+ "     WHERE   conversionRawdataTypeCode = 'CRT005'"
									+ "     AND     TRIM(utmTerm) != ''"
									+ "     GROUP BY processDate, pixelId, mediaId, channelId, productsId,  productsId, campaignId,linkId, deviceTypeCode, PARSE_URL(referrer, 'HOST') ,  TRIM(LOWER(SUBSTRING(utmTerm, 0 , 127))) "
									+ "     ) A"
									+ " INNER JOIN T_TRK_WEB TW  ON	A.pixelId = TW.pixelId  "
									+ " WHERE A.pt <= 100 ");
			resultDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), "tracking.TS_TRK_WEB_MEDIA_SEARCH_KEYWORD", SparkJdbcConnectionFactory.getDefaultJdbcOptions());
			
		}catch(Exception e){
			LogStack.report.error(ExceptionMap.REPORT_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.report.error(e);
		}
		return true;
	}	
}
