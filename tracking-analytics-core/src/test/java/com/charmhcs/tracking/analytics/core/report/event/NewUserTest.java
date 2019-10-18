package com.dmcmedia.tracking.analytics.core.report.event;

import java.util.concurrent.atomic.AtomicInteger;

import com.charmhcs.tracking.analytics.core.report.event.EventNewUser;
import com.charmhcs.tracking.analytics.core.report.event.EventNewUserChannels;
import com.charmhcs.tracking.analytics.core.report.event.EventNewUserRetention;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import com.dmcmedia.tracking.analytics.common.factory.SparkMongodbConnectionFactory;
import com.dmcmedia.tracking.analytics.common.test.CommonJunitTest;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.charmhcs.tracking.analytics.core.config.RawDataSQLTempView;

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
public class NewUserTest  extends CommonJunitTest{
	
	private String reportDateTime = "2017-08-01";
	@Before
	public void setUp() {
		super.atomicInteger = new AtomicInteger(0);
	}
	
	@Test
	public void testNewUser() throws Exception {

		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();		
		try{
			new EventNewUser().reportsDataSet(sparkSession, reportDateTime);

		} catch (Exception e) {
			LogStack.batch.error("Tracking NewUserChannelsTest ERROR");	
			e.printStackTrace();
		} finally{
			sparkSession.stop();
		}
	}
	
	@Test
	public void testNewUserChannels() throws Exception {

		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();		
		try{
			RawDataSQLTempView rawDataSQLTempView = new RawDataSQLTempView(reportDateTime);
			rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
			new EventNewUserChannels(EventNewUserChannels.BATCHJOB).reportsDataSet(sparkSession, reportDateTime);

		} catch (Exception e) {
			LogStack.batch.error("Tracking NewUserChannelsTest ERROR");	
			e.printStackTrace();
		} finally{
			sparkSession.stop();
		}
	}
	
	@Test
	public void testNewUserRetention() throws Exception {

		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();		
		try{
//			RawDataSQLTempView rawDataSQLTempView = new RawDataSQLTempView(reportDateTime);
//			rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);
//			
			new EventNewUserRetention().reportsDataSet(sparkSession, reportDateTime);

		} catch (Exception e) {
			LogStack.batch.error("Tracking  NewUserRetentionTest ERROR");	
			e.printStackTrace();
		} finally{
			sparkSession.stop();
		}
	}

}
