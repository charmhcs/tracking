package com.dmcmedia.tracking.analytics.core.report.event;

import java.util.concurrent.atomic.AtomicInteger;

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
* 2018. 10. 8.   cshwang   Initial Release
*
*
************************************************************************************************/
public class EventUserTest  extends CommonJunitTest {
	
	private String reportDateTime = "2017-08-01 00";
	
	@Before
	public void setUp() {
		super.atomicInteger = new AtomicInteger(0);
	}
	
	/**
	 * @throws Exception
	 */
	@Test
	public void testEventUser() throws Exception {

		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();		
		try{
			new EventUser(EventUser.EACH, EventUser.HOURLY).reportsDataSet(sparkSession, reportDateTime);

		} catch (Exception e) {
			LogStack.batch.error("Tracking EventUserTest ERROR");	
			e.printStackTrace();
		} finally{
			sparkSession.stop();
		}
	}
	
	@Test
	public void testEventUserChannel() throws Exception {
		
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();		
		try{
			new EventUserChannels(EventUserChannels.EACH, EventUserChannels.HOURLY).reportsDataSet(sparkSession, reportDateTime);

		} catch (Exception e) {
			LogStack.batch.error("Tracking EventUserChannelsTest ERROR");	
			e.printStackTrace();
		} finally{
			sparkSession.stop();
		}
	}
}