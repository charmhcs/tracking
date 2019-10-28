package com.dmcmedia.tracking.analytics.core.report.trackinglink;

import java.util.concurrent.atomic.AtomicInteger;

import com.charmhcs.tracking.analytics.core.report.trackinglink.TrackingLinkReport;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import com.dmcmedia.tracking.analytics.common.factory.SparkMongodbConnectionFactory;
import com.dmcmedia.tracking.analytics.common.test.CommonJunitTest;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.charmhcs.tracking.analytics.core.config.RawDataSQLTempView;

/** 
*
*   날  짜      성 명                     수정  내용
* ----------  ---------  ------------------------------------------------------------------------
* 2018. 9. 16.   cshwang   Initial Release
*
*
************************************************************************************************/
public class TrackingReportWebTest extends CommonJunitTest {

	@Before
	public void setUp() {
		super.atomicInteger = new AtomicInteger(0);
	}

	@Test
	public void testTrackingReportWebReport() throws Exception {


		String reportDateTime = "2017-08-01";
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();
		try{
			RawDataSQLTempView rawDataSQLTempView = new RawDataSQLTempView(reportDateTime);
			rawDataSQLTempView.createWebReportCodeAndDimensionTempView(sparkSession);

			new TrackingLinkReport(TrackingLinkReport.BATCHJOB, TrackingLinkReport.DAILY).reportsDataSet(sparkSession, reportDateTime);

		} catch (Exception e) {
			LogStack.batch.error("Tracking Test CreateCodeAndDimensionDataTest ERROR");
			e.printStackTrace();
		} finally{
			sparkSession.stop();
		}
	}
}
