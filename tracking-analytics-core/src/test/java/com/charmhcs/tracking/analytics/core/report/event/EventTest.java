package com.dmcmedia.tracking.analytics.core.report.event;

import java.util.concurrent.atomic.AtomicInteger;

import com.charmhcs.tracking.analytics.core.report.event.EventPopularityRanking;
import com.charmhcs.tracking.analytics.core.report.event.EventUserDuration;
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
* 2018-12-26   cshwang  Initial Release
*
*
************************************************************************************************/
public class EventTest  extends CommonJunitTest {
	
	private String reportDateTime = "2017-08-01";
	
	@Before
	public void setUp() {
		super.atomicInteger = new AtomicInteger(0);
	}
	
	/**
	 * @throws Exception
	 */
	@Test
	public void testEventDuration() throws Exception {

		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();		
		try{

			new EventUserDuration().reportsDataSet(sparkSession, reportDateTime);

		} catch (Exception e) {
			LogStack.batch.error("Tracking Test EventStayTime ERROR");	
			e.printStackTrace();
		} finally{
			sparkSession.stop();
		}
	}
	
	/**
	 * @throws Exception
	 */
	@Test
	public void testEventPopularityRanking() throws Exception {

		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();		
		try{
			new EventPopularityRanking().reportsDataSet(sparkSession, reportDateTime);
		} catch (Exception e) {
			LogStack.batch.error("Tracking EventPopularityRankingTest ERROR");	
			e.printStackTrace();
		} finally{
			sparkSession.stop();
		}
	}
}