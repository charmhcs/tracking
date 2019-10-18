package com.charmhcs.tracking.analytics.common;

import org.apache.spark.sql.SparkSession;

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
