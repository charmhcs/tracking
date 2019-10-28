package com.charmhcs.tracking.analytics.common;

import org.apache.spark.sql.SparkSession;

/**  
*
*   날  짜      성 명                     수정  내용
* ----------  ---------  ------------------------------------------------------------------------
* 2017. 10. 10.   cshwang   Initial Release
*
*
************************************************************************************************/
public interface DataProcess {

	/**
	 * @param sparkSession
	 * @return
	 */
	public boolean processDataSet(SparkSession sparkSession);


	/**
	 * @param sparkSession
//	 * @param processDateTime
	 * @return
	 */
	public boolean processDataSet(SparkSession sparkSession, String processDateTime);
}
