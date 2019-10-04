package com.dmcmedia.tracking.analytics.core.process.event;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.dmcmedia.tracking.analytics.common.factory.SparkMongodbConnectionFactory;
import com.dmcmedia.tracking.analytics.common.test.CommonJunitTest;
import com.dmcmedia.tracking.analytics.common.util.LogStack;                      	
/**
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
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AccumulateEventUserTest extends CommonJunitTest{
	private String processDateTime = "2017-08-01";
	
	@Before
	public void setUp() {
		super.atomicInteger = new AtomicInteger(0);
	}
	
	@Test
	public void testAudiencHourlyPixel() throws Exception {
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();		
		
		try{
			new EventAudience().processDataSet(sparkSession, processDateTime);
		} catch (Exception e) {
			LogStack.batch.error("TrackingHourlyBatch ERROR");	
			e.printStackTrace();
		} finally{
			sparkSession.close();
		}
	}
	
	@Test
	public void testAudiencDailyPixel() throws Exception {
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();		
		
		try{
			EventAudience audiencePixel = new EventAudience(EventAudience.DAILY);
//			audiencePixelProcess.setDays(3);
			audiencePixel.processDataSet(sparkSession, processDateTime);
		} catch (Exception e) {
			LogStack.batch.error("TrackingHourlyBatch ERROR");	
			e.printStackTrace();
		} finally{
			sparkSession.close();
		}
	}
	
	@Test
	public void testAudiencDaysPixel() throws Exception {
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();		
		
		try{
			EventAudience audiencePixel = new EventAudience(EventAudience.DAYS);
			audiencePixel.setDays(7);
			audiencePixel.processDataSet(sparkSession, processDateTime);
		} catch (Exception e) {
			LogStack.batch.error("TrackingHourlyBatch ERROR");	
			e.printStackTrace();
		} finally{
			sparkSession.close();
		}
	}
}