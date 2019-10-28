package com.charmhcs.tracking.analytics.common;

import org.apache.spark.sql.SparkSession;

/**    
*
*   날  짜      성 명                     수정  내용
* ----------  ---------  ------------------------------------------------------------------------
* 2018. 4. 6.   cshwang   Initial Release
*
*
************************************************************************************************/
public interface DataAnalysis {
	/**
	 * @param sparkSession
	 * @return
	 */
	public boolean analyzeDataSet(SparkSession sparkSession);

	/**
	 * @param sparkSession
	 * @param analysisDateTime
	 * @return
	 */
	public boolean analyzeDataSet(SparkSession sparkSession,  String analysisDateTime);
}
