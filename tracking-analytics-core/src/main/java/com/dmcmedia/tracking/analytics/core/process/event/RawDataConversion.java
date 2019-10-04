package com.dmcmedia.tracking.analytics.core.process.event;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import com.dmcmedia.tracking.analytics.common.AbstractDataProcess;
import com.dmcmedia.tracking.analytics.common.factory.MongodbFactory;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.map.MongodbMap;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.config.RawDataSQLTempView;
import com.mongodb.spark.MongoSpark;

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
* 2018. 9. 10.   cshwang   Initial Release
*
*
************************************************************************************************/
public class RawDataConversion extends AbstractDataProcess{
	/**
	 * 생성자
	 */
	public RawDataConversion(){
		super();
	}
	
	/**
	 * 생성자
	 * 
	 * @param processType
	 */
	public RawDataConversion(String runType){
		super(runType);
	}
	
	/**
	 * 생성자 
	 * @param processType
	 * @param runType
	 */
	public RawDataConversion(String runType, String processType){
		super(runType, processType);
	}
		
	/* (non-Javadoc)
	 * @see com.dmcmedia.tracking.analysis.common.AbstractSparkSessionAnalysis#analysisDataSet(org.apache.spark.sql.SparkSession)
	 */
	public boolean processDataSet(SparkSession sparkSession){

		Dataset<Row> pixelConversionRawDataDS = null;
		String queryString = "";
		sparkSession.udf().register("URL_DECODE", urldecode, DataTypes.StringType);
	
		if(super.isValid() == false){
			return false;
		}

		try{
			if(EACH.equals(this.getRunType().name())){
		    	RawDataSQLTempView rawDataSQLTempView = null;
				if(HOURLY.equals(getProcessType().name())) {
					rawDataSQLTempView = new RawDataSQLTempView(getProcessDateHour());
					rawDataSQLTempView.createPixelRawDataHourlyTempView(sparkSession);
				}else {
					rawDataSQLTempView = new RawDataSQLTempView(getProcessDate());
					rawDataSQLTempView.createPixelRawDataDailyTempView(sparkSession);
				}
				rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
			}
			
			switch(getProcessType().name()) {
				case HOURLY :
					if(!MongodbFactory.collectionExists(getProcessedDataBase(), getProcessDate() + MongodbMap.COLLECTION_PIXEL_CONVERSION_RAWDATA)){
						MongodbFactory.dropAndCreateAndShardingCollection(getProcessedDataBase(), getProcessDate() + MongodbMap.COLLECTION_PIXEL_CONVERSION_RAWDATA);
						MongodbFactory.createCollectionIndex(getProcessedDataBase(), getProcessDate() + MongodbMap.COLLECTION_PIXEL_CONVERSION_RAWDATA, "logDateTime");
					}
					queryString = " AND logDateTime LIKE '"+getProcessDateHour()+"%' "; 
					break;
				default :
					MongodbFactory.dropAndCreateAndShardingCollection(getProcessedDataBase(), getProcessDate() + MongodbMap.COLLECTION_PIXEL_CONVERSION_RAWDATA );
					MongodbFactory.createCollectionIndex(getProcessedDataBase(), getProcessDate() + MongodbMap.COLLECTION_PIXEL_CONVERSION_RAWDATA, "logDateTime");
					queryString = "";
					break;
				}
			/*WEB Pixel*/
			pixelConversionRawDataDS = sparkSession.sql("SELECT  DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd') AS processDate "
													+ "         ,pageView.pixel_id      AS pixelId "
													+ "         ,pageView.catalog_id    AS catalogId "
													+ "         ,pageView.p_cid         AS pCid "
													+ "         ,'CRT001'               AS conversionRawdataTypeCode "
													+ "         ,(CASE WHEN pageView.user_agent LIKE '%iPhone%' OR pageView.user_agent LIKE '%Android%' OR pageView.user_agent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode "
													+ "         ,IFNULL(PARSE_URL(pageView.href, 'QUERY', 'mediaId'),'')       	AS mediaId "
													+ "         ,IFNULL(PARSE_URL(pageView.href, 'QUERY', 'channelId'),'')     	AS channelId "
													+ "         ,IFNULL(PARSE_URL(pageView.href, 'QUERY', 'productsId'),'')    	AS productsId "
													+ "         ,IFNULL(PARSE_URL(pageView.href, 'QUERY', 'campaignId'),'')    	AS campaignId "
													+ "         ,IFNULL(PARSE_URL(pageView.href, 'QUERY', 'linkId'),'')    		AS linkId "
													+ "         ,''                     AS utmMedium "
													+ "         ,''                     AS utmSource "
													+ "         ,''                     AS utmCampaign "
													+ "         ,''                     AS utmTerm "
													+ "         ,IFNULL(PARSE_URL(pageView.href, 'QUERY', 'uuId'),'')			AS utmContent "
													+ "         ,_id                    AS ids "
													+ "         ,''                     AS href "
													+ "         ,pageView.referrer      AS referrer "
													+ "         ,logDateTime "
													+ "         ,isoDate "
													+ " FROM    T_PIXEL_PAGE_VIEW "
													+ " WHERE   pageView.href LIKE '%linkId=lnk_%' " + queryString
													+ " UNION ALL"
													+ " SELECT   DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd') AS processDate "
													+ "         ,pageView.pixel_id      AS pixelId "
													+ " 		,pageView.catalog_id    AS catalogId "
													+ " 		,pageView.p_cid         AS pCid "
													+ "         ,'CRT002'               AS conversionRawdataTypeCode "
													+ "         ,(CASE WHEN pageView.user_agent LIKE '%iPhone%' OR pageView.user_agent LIKE '%Android%' OR pageView.user_agent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode "
													+ " 		,''                     AS mediaId "
													+ " 		,'chn_direct'           AS channelId "
													+ " 		,''           			AS productsId "
													+ " 		,''                     AS campaignId "
													+ " 		,''                     AS linkId "
													+ " 		,''                     AS utmMedium "
													+ " 		,''                     AS utmSource "
													+ " 		,''                     AS utmCampaign "
													+ " 		,''                     AS utmTerm "
													+ " 		,''                     AS utmContent "
													+ " 		,_id                    AS ids "
													+ " 		,pageView.href          AS href "
													+ " 		,pageView.referrer      AS referrer "
													+ " 		,logDateTime "
													+ " 		,isoDate "
													+ " FROM   	T_PIXEL_PAGE_VIEW "
													+ " WHERE  	TRIM(pageView.referrer) = '' "
													+ " AND     pageView.href NOT LIKE '%linkId=lnk%' " + queryString
													+ " UNION ALL"
													+ " SELECT   DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd') AS processDate "
													+ "         ,pageView.pixel_id      AS pixelId "
													+ " 		,pageView.catalog_id    AS catalogId "
													+ " 		,pageView.p_cid         AS pCid "
													+ "         ,'CRT003'               AS conversionRawdataTypeCode "
													+ "         ,(CASE WHEN pageView.user_agent LIKE '%iPhone%' OR pageView.user_agent LIKE '%Android%' OR pageView.user_agent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode "
													+ " 		,(CASE  WHEN pageView.referrer LIKE '%naver.c%'  THEN 'mda_naver' "
													+ "                 WHEN pageView.referrer LIKE '%daum.net%' THEN 'mda_daum'  "
													+ "                 WHEN pageView.referrer LIKE '%facebook.c%'  THEN 'mda_facebook' "
													+ "                 WHEN pageView.referrer LIKE '%instagram.c%' THEN 'mda_instagram' "
													+ "                 WHEN pageView.referrer LIKE '%twitter.c%'   THEN 'mda_twitter' ELSE '' END) AS mediaId "
													+ " 		,'chn_referral'         AS channelId "
													+ " 		,''           			AS productsId "
													+ " 		,''                     AS campaignId "
													+ " 		,''                     AS linkId "
													+ " 		,''                     AS utmMedium "
													+ " 		,''                     AS utmSource "
													+ " 		,''                     AS utmCampaign "
													+ " 		,''                     AS utmTerm "
													+ " 		,''                     AS utmContent "
													+ " 		,_id                    AS ids "
													+ " 		,pageView.href          AS href "
													+ " 		,pageView.referrer      AS referrer "
													+ " 		,logDateTime "
													+ " 		,isoDate "
													+ " FROM    T_PIXEL_PAGE_VIEW A "
													+ " WHERE   SUBSTRING_INDEX(PARSE_URL(pageView.referrer, 'HOST'), '.', -2) != SUBSTRING_INDEX(PARSE_URL(pageView.href, 'HOST'), '.', -2) "
													+ " AND     TRIM(pageView.referrer) != ''  "
													+ " AND     pageView.referrer NOT LIKE '%utm%'  "
													+ " AND     pageView.href NOT LIKE '%linkId=lnk%' "
													+ " AND     NOT EXISTS (SELECT 0 "
													+ "                     FROM T_PIXEL_PAGE_VIEW B "
													+ "                     WHERE A._id = B._id  "
													+ "                     AND (PARSE_URL(pageView.referrer, 'HOST')  LIKE '%naver.c%'  "
													+ "                         OR PARSE_URL(pageView.referrer, 'HOST')   LIKE '%daum.net%'  "
													+ "                         OR PARSE_URL(pageView.referrer, 'HOST')   LIKE '%google.c%') "
													+ "                     AND PARSE_URL(pageView.referrer, 'HOST') LIKE '%search%') " + queryString
													+ "  "
													+ "  "
													+ " UNION ALL"
													+ " SELECT   DATE_FORMAT(CAST(UNIX_TIMESTAMP(A.logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd') AS processDate "
													+ "         ,A.pixelId "
													+ "         ,A.catalogId "
													+ "         ,A.pCid "
													+ "         ,'CRT004'               AS conversionRawdataTypeCode "
													+ "         ,A.deviceTypeCode "
													+ "         ,IFNULL(A.mediaId, '')     AS mediaId "
													+ "         ,IFNULL(B.channelId, '')   AS channelId "
													+ " 		,IFNULL(A.productsId, '')  AS productsId "
													+ "         ,IFNULL(A.campaignId, '')  AS campaignId "
													+ "         ,IFNULL(A.linkId, '')      AS linkId "
													+ "         ,A.utmMedium "
													+ "         ,A.utmSource "
													+ "         ,A.utmCampaign "
													+ "         ,A.utmTerm "
													+ "         ,A.utmContent "
													+ "         ,A.ids "
													+ "         ,A.href "
													+ "         ,A.referrer "
													+ "         ,A.logDateTime "
													+ "         ,A.isoDate "
													+ " FROM (  SELECT	 ''						AS mediaId "
													+ " 				,''						AS channelId "
													+ " 				,''						AS productsId "
													+ " 				,''						AS campaignId "
													+ " 				,''						AS linkId "
													+ " 				,(CASE WHEN pageView.user_agent LIKE '%iPhone%' OR pageView.user_agent LIKE '%Android%' OR pageView.user_agent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode "
													+ " 				,IFNULL(PARSE_URL(pageView.referrer, 'QUERY', 'utm_medium'),'') 	AS utmMedium "
													+ " 				,IFNULL(PARSE_URL(pageView.referrer, 'QUERY', 'utm_source'),'') 	AS utmSource "
													+ " 				,IFNULL(PARSE_URL(pageView.referrer, 'QUERY', 'utm_campaign'),'') 	AS utmCampaign "
													+ " 				,IFNULL(PARSE_URL(pageView.referrer, 'QUERY', 'utm_term'),'') 		AS utmTerm "
													+ " 				,IFNULL(PARSE_URL(pageView.referrer, 'QUERY', 'utm_content'),'') 	AS utmContent "
													+ " 				,_id                  	AS ids "
													+ " 				,pageView.href        	AS href "
													+ " 				,pageView.referrer    	AS referrer "
													+ " 				,pageView.p_cid       	AS pCid "
													+ " 				,pageView.catalog_id  	AS catalogId "
													+ " 				,pageView.pixel_id      AS pixelId "
													+ " 				,logDateTime "
													+ " 				,isoDate "
													+ "         FROM   T_PIXEL_PAGE_VIEW A "
													+ "         WHERE  pageView.referrer LIKE '%utm%') A "
													+ "         LEFT OUTER JOIN T_TRK_CHANNEL B "
													+ "         ON  A.utmMedium = B.utmMedium "
													+ "         AND A.utmSource = B.utmSource "
													+ "         AND A.utmTerm = B.utmTerm "
													+ "         AND A.utmContent = B.utmContent "
													+ "         AND A.utmCampaign = B.utmCampaign " + queryString
													+ " UNION ALL"
													+ " SELECT	 DATE_FORMAT(CAST(UNIX_TIMESTAMP(logdatetime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd') AS processDate "
													+ "         ,pageView.pixel_id      AS pixelId "
													+ "         ,pageView.catalog_id    AS catalogId "
													+ "         ,pageView.p_cid         AS pCid "
													+ "         ,'CRT005'               AS conversionRawdataTypeCode "
													+ "         ,(CASE WHEN pageView.user_agent LIKE '%iPhone%' OR pageView.user_agent LIKE '%Android%' OR pageView.user_agent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode "
													+ "         ,(CASE  WHEN pageView.referrer LIKE '%naver.c%'     THEN 'mda_naver' "
													+ "                 WHEN pageView.referrer LIKE '%daum.net%'    THEN 'mda_daum'  "
													+ "                 WHEN pageView.referrer LIKE '%google.c%'    THEN 'mda_google' ELSE '' END) AS mediaId "
													+ "         ,(CASE  WHEN pageView.referrer LIKE '%naver.c%'     THEN 'chn_search' "
													+ " 			    WHEN pageView.referrer LIKE '%daum.net%'    THEN 'chn_search'  "
													+ " 			    WHEN pageView.referrer LIKE '%google.c%'    THEN 'chn_search'  ELSE '' END) AS channelId "
													+ "         ,''                     AS campaignId "
													+ " 		,''           			AS productsId "
													+ "         ,''                     AS linkId "
													+ "         ,''                     AS utmMedium "
													+ "         ,''                     AS utmSource "
													+ "         ,''                     AS utmCampaign "
													+ "         ,(CASE  WHEN pageView.referrer LIKE '%naver.c%'   THEN URL_DECODE(SUBSTRING_INDEX(SUBSTRING_INDEX(pageView.referrer , 'query=', -1), '&', 1)) "
													+ "                 WHEN pageView.referrer LIKE '%daum.net%'   THEN URL_DECODE(SUBSTRING_INDEX(SUBSTRING_INDEX(pageView.referrer , 'q=', -1), '&', 1)) "
													+ " 			    ELSE '' END)                     AS utmTerm "
													+ "         ,''                     AS utmContent "
													+ "         ,_id                    AS ids "
													+ "         ,pageView.href          AS href "
													+ "         ,pageView.referrer      AS referrer "
													+ "         ,logDateTime "
													+ "         ,isoDate "
													+ " FROM    T_PIXEL_PAGE_VIEW "
													+ " WHERE   SUBSTRING_INDEX(PARSE_URL(pageView.referrer, 'HOST'), '.', -2) != SUBSTRING_INDEX(PARSE_URL(pageView.href, 'HOST'), '.', -2) "
													+ " AND     pageView.referrer NOT LIKE '%utm%'  "
													+ " AND     pageView.href NOT LIKE '%linkId=lnk%' "
													+ " AND     TRIM(pageView.referrer) != '' "
													+ " AND     ((PARSE_URL(pageView.referrer, 'HOST')  LIKE '%naver.c%' AND PARSE_URL(pageView.referrer, 'HOST') LIKE '%search%')"
													+ "         OR (PARSE_URL(pageView.referrer, 'HOST')   LIKE '%daum.net%' AND PARSE_URL(pageView.referrer, 'HOST') LIKE '%search%')"
													+ "         OR PARSE_URL(pageView.referrer, 'HOST')   LIKE '%google.c%')"
													+ " " + queryString
									                + " UNION ALL"
									                + " SELECT   DATE_FORMAT(CAST(UNIX_TIMESTAMP(logDateTime, 'yyyy-MM-dd HH:mm') AS TIMESTAMP), 'yyyy-MM-dd') AS processDate "
									                + "         ,pixelId "
									                + "  		,catalogId "
									                + "  		,pCid "
									                + "         ,'CRT006'       			AS conversionRawdataTypeCode "
									                + "         ,(CASE WHEN userAgent LIKE '%iPhone%' OR userAgent LIKE '%Android%' OR userAgent LIKE '%Windows Phone%' THEN 'PMC001' ELSE 'PMC002' END) AS deviceTypeCode "
									                + "         ,IFNULL(mediaId, '')     	AS mediaId "
													+ "         ,IFNULL(channelId, '')   	AS channelId "
													+ " 		,IFNULL(productsId, '')  	AS productsId "
													+ "         ,IFNULL(campaignId, '')  	AS campaignId "
													+ "         ,IFNULL(linkId, '')      	AS linkId "
									                + "  		,''       		AS utmMedium "
									                + "  		,''       		AS utmSource "
									                + "  		,''       		AS utmCampaign "
									                + "  		,''       		AS utmTerm "
									                + "  		,uuId	     	AS utmContent "
									                + "  		,_id      		AS ids "
									                + "  		,''       		AS href "
									                + "  		,''       		AS referrer "
									                + "  		,logDateTime "
									                + "  		,isoDateTime "
									                + " FROM    T_WEB_CLICK "
									                + "	WHERE 	1=1 " + queryString );
			// MongodbFactory.dropAndCreateAndShardingCollection(getProcessedDataBase(), getProcessDate() + MongodbMap.COLLECTION_PIXEL_CONVERSION_RAWDATA );
			MongoSpark.write(pixelConversionRawDataDS).option("database", getProcessedDataBase())
					.option("collection",getProcessDate() + MongodbMap.COLLECTION_PIXEL_CONVERSION_RAWDATA )
					.mode(DEFAULT_SAVEMODE)
					.save();

		}catch(Exception e){
			LogStack.process.error(ExceptionMap.PROCESS_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.process.error(e);
		}
		return true;
	}	 
}
