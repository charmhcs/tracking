package com.dmcmedia.tracking.analytics.core.report.system;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import com.dmcmedia.tracking.analytics.common.factory.SparkMongodbConnectionFactory;
import com.dmcmedia.tracking.analytics.common.test.CommonJunitTest;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.report.system.CreateCodeAndDimensionData;
import com.dmcmedia.tracking.analytics.core.report.trackinglink.TrackingLinkReport;

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
public class CreateCodeAndDimensionDataTest extends CommonJunitTest {
	

	@Before
	public void setUp() {
		super.atomicInteger = new AtomicInteger(0);
	}
	
	@Test
	public void testCreateCodeAndDimensionDataDaily() throws Exception {
		String reportDateTime = "2017-08-01";
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();		
		try{
			new CreateCodeAndDimensionData().reportsDataSet(sparkSession, reportDateTime);

		} catch (Exception e) {
			LogStack.batch.error("Tracking Test CreateCodeAndDimensionDataTest ERROR");	
			e.printStackTrace();
		} finally{
			sparkSession.stop();
		}
	}
	
	@Test
	public void testCreateCodeAndDimensionDataHourly() throws Exception {
		String reportDateTime = "2017-08-04 00";
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();		
		try{
			new CreateCodeAndDimensionData(TrackingLinkReport.EACH, CreateCodeAndDimensionData.HOURLY).reportsDataSet(sparkSession, reportDateTime);

		} catch (Exception e) {
			LogStack.batch.error("Tracking Test CreateCodeAndDimensionDataTest ERROR");	
			e.printStackTrace();
		} finally{
			sparkSession.stop();
		}
	}
}
