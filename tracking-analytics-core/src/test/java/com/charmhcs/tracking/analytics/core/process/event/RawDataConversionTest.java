package com.dmcmedia.tracking.analytics.core.process.event;

import java.util.concurrent.atomic.AtomicInteger;

import com.charmhcs.tracking.analytics.core.process.event.RawDataConversion;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import com.dmcmedia.tracking.analytics.common.factory.SparkMongodbConnectionFactory;
import com.dmcmedia.tracking.analytics.common.test.CommonJunitTest;
import com.dmcmedia.tracking.analytics.common.util.LogStack;

/**
*
*   날  짜      성 명                     수정  내용
* ----------  ---------  ------------------------------------------------------------------------
* 2018. 9. 10.   cshwang   Initial Release
*
*
************************************************************************************************/
public class RawDataConversionTest  extends CommonJunitTest{

	@Before
	public void setUp() {
		super.atomicInteger = new AtomicInteger(0);
	}


	@Test
	public void testConversionRawDataPixelDaily() throws Exception {
		String processDateTime = "2017-08-02";
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();

		try{
			new RawDataConversion(RawDataConversion.EACH, RawDataConversion.DAILY).processDataSet(sparkSession, processDateTime);
		} catch (Exception e) {
			LogStack.batch.error("TrackigTestConversionRawDataDailyPixelBatch ERROR");
			e.printStackTrace();
		} finally{
			sparkSession.close();
		}
	}
}
