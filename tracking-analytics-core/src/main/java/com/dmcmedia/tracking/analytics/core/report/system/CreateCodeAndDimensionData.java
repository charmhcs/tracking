package com.dmcmedia.tracking.analytics.core.report.system;

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
* 2018. 8. 23.   cshwang   Initial Release
*
*
************************************************************************************************/
public class CreateCodeAndDimensionData extends AbstractDataReport {
	
	/**
	 * 생성자 
	 */
	public CreateCodeAndDimensionData() {
		super();
	}
	
	/**
	 * 생성
	 * @param runType
	 */
	public CreateCodeAndDimensionData(String runType) {
		super(runType, DAILY);
	}

	/**
	 * 생성
	 * @param runType
	 * @param reportType
	 */
	public CreateCodeAndDimensionData(String runType, String reportType) {
		super(runType, reportType);
	}

	/* (non-Javadoc)
	 * @see com.dmcmedia.tracking.analytics.common.DataReport#reportsDataSet(org.apache.spark.sql.SparkSession)
	 */
	@Override
	public boolean reportsDataSet(SparkSession sparkSession) {

		Dataset<Row> resultDS 	= null;
		Dataset<Row> pixelListDS = null;
		
		if(super.isValid() == false){
			return false;
		}
		try{
		    
			if(EACH.equals(this.getRunType().name())){
				RawDataSQLTempView rawDataSQLTempView = new RawDataSQLTempView(getReportDate());
				rawDataSQLTempView.createPixelRawDataDailyTempView(sparkSession);
			}
			String trkPixelTable = "(SELECT PIXEL_ID AS pixelId, CATALOG_ID AS catalogId, PIXEL_NAME AS pixelName FROM tracking.T_TRK_PIXEL) AS T_TRK_PIXEL"; 
		    pixelListDS = SparkJdbcConnectionFactory.getSelectJdbcDfLoad(sparkSession, trkPixelTable);
		    pixelListDS.createOrReplaceTempView("T_TRK_PIXEL");
		    
		    resultDS = sparkSession.sql("SELECT  pageView.pixel_id      AS PIXEL_ID "
						    		+ "         ,pageView.catalog_id	AS CATALOG_ID "
						    		+ "			,'"+SYSTEM_USER+"'		AS REGIST_ID "
						    		+ " 		,NOW()					AS REGIST_DATETIME "
						    		+ " FROM 	T_PIXEL_PAGE_VIEW A "
						    		+ " WHERE	pageView.pixel_id IS NOT NULL AND TRIM(pageView.pixel_id) != '' "
						    		+ " AND NOT EXISTS (SELECT 0 FROM T_TRK_PIXEL B WHERE A.pageView.pixel_id = B.pixelId AND pageView.catalog_id = B.catalogId) "
						    		+ " GROUP BY pageView.pixel_id , pageView.catalog_id "
						    		+ "");
		    
		    resultDS.write().mode(DEFAULT_SAVEMODE).jdbc(SparkJdbcConnectionFactory.getDefaultJdbcUrl(), "tracking.T_TRK_PIXEL", SparkJdbcConnectionFactory.getDefaultJdbcOptions());
		    
		}catch(Exception e){
			LogStack.report.error(ExceptionMap.REPORT_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.report.error(e);
		}
		return true;
	}
}
