package com.charmhcs.tracking.analytics.common;

import org.apache.spark.sql.SparkSession;

/**  
*
*   날  짜      성 명                     수정  내용
* ----------  ---------  ------------------------------------------------------------------------
* 2018. 8. 23.   cshwang   Initial Release
*
*
************************************************************************************************/
public interface DataReport {
	/**
	 * @param sparkSession
	 * @return
	 */
	public boolean reportsDataSet(SparkSession sparkSession);

	/**
	 * @param sparkSession
	 * @param reportDateTime
	 * @return
	 */
	public boolean reportsDataSet(SparkSession sparkSession,  String reportDateTime);
}
