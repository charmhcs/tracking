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