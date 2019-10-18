package com.dmcmedia.tracking.analytics.core.report.ecomm;

import java.util.concurrent.atomic.AtomicInteger;

import com.charmhcs.tracking.analytics.core.report.ecomm.EcommLastClickConversionChannels;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import com.dmcmedia.tracking.analytics.common.factory.SparkMongodbConnectionFactory;
import com.dmcmedia.tracking.analytics.common.test.CommonJunitTest;
import com.dmcmedia.tracking.analytics.common.util.LogStack;


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
* 2018-11-11  cshwang   Initial Release
*
*
************************************************************************************************/
public class EcommLastClickConversionChannelsTest extends CommonJunitTest {
	@Before
	public void setUp() {
		super.atomicInteger = new AtomicInteger(0);
	}

	@Test
	public void testEcommChannelsWebDaily() throws Exception {
		String reportDateTime = "2017-08-01";
		
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();		
		try{
			new EcommLastClickConversionChannels(EcommLastClickConversionChannels.EACH, EcommLastClickConversionChannels.DAILY).reportsDataSet(sparkSession, reportDateTime);

		} catch (Exception e) {
			LogStack.batch.error("Tracking Test EcommChannels ERROR");	
			e.printStackTrace();
		} finally{
			sparkSession.stop();
		}
	}
}
