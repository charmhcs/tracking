package com.charmhcs.tracking.analytics.core.config;

import com.charmhcs.tracking.analytics.core.dataset.TrackingPixelDataSet;
import com.charmhcs.tracking.analytics.core.dataset.entity.web.Click;
import com.charmhcs.tracking.analytics.core.dataset.event.User;
import com.charmhcs.tracking.analytics.core.dataset.feed.Purchased;
import org.apache.spark.sql.SparkSession;

import com.charmhcs.tracking.analytics.common.CommonDataAnalytics;
import com.charmhcs.tracking.analytics.common.factory.SparkJdbcConnectionFactory;
import com.charmhcs.tracking.analytics.common.map.CodeMap;
import com.charmhcs.tracking.analytics.common.map.ExceptionMap;
import com.charmhcs.tracking.analytics.common.map.MongodbMap;
import com.charmhcs.tracking.analytics.common.util.DateUtil;
import com.charmhcs.tracking.analytics.common.util.LogStack;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

/**
*
*   날  짜      성 명                     수정  내용
* ----------  ---------  ------------------------------------------------------------------------
* 2018. 10. 4.   cshwang   Initial Release
*
*
************************************************************************************************/
public class RawDataSQLTempView extends CommonDataAnalytics{

	public final static int DEFAULT_INTERVAL_DAY = CodeMap.NG_ONE;
	public final static int DEFAULT_INTERVAL_HOUR = CodeMap.NG_ONE;
	private String analyticsDailyDate;
	private String analyticsHourlyDate;
	private String analyticsHourlyDateTime;
	private String rawDataBaseDaily;
	private String processedDataBaseDaily;
	private String rawDataBaseHourly;
	private String processedDataBaseHourly;

	/**
	 * 생성자
	 */
	public RawDataSQLTempView(){
		setRawDataBaseDaily(MongodbMap.DATABASE_RAW_MONTH + DateUtil.getIntervalDate(CodeMap.DATE_MONTH_PATTERN, DEFAULT_INTERVAL_DAY));
		setProcessedDataBaseDaily(MongodbMap.DATABASE_PROCESSED_MONTH + DateUtil.getIntervalDate(CodeMap.DATE_MONTH_PATTERN, DEFAULT_INTERVAL_DAY));
		setAnalyticsDailyDate(DateUtil.getIntervalDate(CodeMap.DATE_YMD_PATTERN_FOR_ANYS, DEFAULT_INTERVAL_DAY));

		setRawDataBaseHourly(MongodbMap.DATABASE_RAW_MONTH + DateUtil.getIntervalHour(CodeMap.DATE_MONTH_PATTERN, DEFAULT_INTERVAL_DAY));
		setProcessedDataBaseHourly(MongodbMap.DATABASE_PROCESSED_MONTH + DateUtil.getIntervalHour(CodeMap.DATE_MONTH_PATTERN, DEFAULT_INTERVAL_DAY));
		setAnalyticsHourlyDate(DateUtil.getIntervalHour(CodeMap.DATE_YMD_PATTERN_FOR_ANYS, DEFAULT_INTERVAL_HOUR));
		setAnalyticsHourlyDateTime(DateUtil.getIntervalHour(CodeMap.DATE_YMDH_PATTERN_FOR_ANYS, DEFAULT_INTERVAL_HOUR));
	}

	/**
	 * @param analyticsDateTime
	 * @throws Exception
	 */
	public RawDataSQLTempView(String analyticsDateTime) throws Exception  {
		initRawDataSQLTempView(analyticsDateTime);
	}

	/**
	 * @param analyticsDateTime
	 * @param rawDataBase
	 * @param processedDataBase
	 */
	public RawDataSQLTempView(String analyticsDateTime, String rawDataBase, String processedDataBase) throws Exception  {
		setRawDataBase(rawDataBase);
		setProcessedDataBase(processedDataBase);
		initRawDataSQLTempView(analyticsDateTime);
	}

	/**
	 * @param analyticsDateTime
	 */
	private void initRawDataSQLTempView(String analyticsDateTime) throws Exception {
		if(DateUtil.checkDateFormat(analyticsDateTime, CodeMap.DATE_YMDH_PATTERN_FOR_ANYS)){ // Hourly
			setRawDataBaseHourly(MongodbMap.DATABASE_RAW_MONTH + DateUtil.getChangeDateFormat(analyticsDateTime, CodeMap.DATE_YMDH_PATTERN_FOR_ANYS, CodeMap.DATE_MONTH_PATTERN));
			setProcessedDataBaseHourly(MongodbMap.DATABASE_PROCESSED_MONTH + DateUtil.getChangeDateFormat(analyticsDateTime, CodeMap.DATE_YMDH_PATTERN_FOR_ANYS, CodeMap.DATE_MONTH_PATTERN));
			setAnalyticsHourlyDate(DateUtil.getChangeDateFormat(analyticsDateTime, CodeMap.DATE_YMDH_PATTERN_FOR_ANYS, CodeMap.DATE_YMD_PATTERN_FOR_ANYS));
			setAnalyticsHourlyDateTime(analyticsDateTime);
		}else if(DateUtil.checkDateFormat(analyticsDateTime, CodeMap.DATE_YMD_PATTERN_FOR_ANYS)){ // Daily
			setRawDataBaseDaily(MongodbMap.DATABASE_RAW_MONTH + DateUtil.getChangeDateFormat(analyticsDateTime, CodeMap.DATE_YMD_PATTERN_FOR_ANYS, CodeMap.DATE_MONTH_PATTERN));
			setProcessedDataBaseDaily(MongodbMap.DATABASE_PROCESSED_MONTH + DateUtil.getChangeDateFormat(analyticsDateTime, CodeMap.DATE_YMD_PATTERN_FOR_ANYS, CodeMap.DATE_MONTH_PATTERN));
			setAnalyticsDailyDate(analyticsDateTime);
		}else{
			throw new Exception(ExceptionMap.INVALID_DATA_FORMAT_EXCEPTION);
		}
	}

	/**
	 * @param sparkSession
	 * @param analyticsDate
	 * @param rawDatabase
	 * @param processedDataBase
	 */
	public void createPixelRawDataDailyTempView(SparkSession sparkSession) {
		LogStack.coreSys.info("rawDataBase :" + rawDataBaseDaily);
		LogStack.coreSys.info("processedDataBase :" + processedDataBaseDaily);
		LogStack.coreSys.info("analyticsDate :" + analyticsDailyDate);
		MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", rawDataBaseDaily).withOption("collection", analyticsDailyDate + MongodbMap.COLLECTION_PIXEL_PAGE_VIEW), TrackingPixelDataSet.class).persist().createOrReplaceTempView("T_PIXEL_PAGE_VIEW");
		MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", rawDataBaseDaily).withOption("collection", analyticsDailyDate + MongodbMap.COLLECTION_PIXEL_VIEW_CONTENT), TrackingPixelDataSet.class).persist().createOrReplaceTempView("T_PIXEL_VIEW_CONTENT");
		MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", rawDataBaseDaily).withOption("collection", analyticsDailyDate + MongodbMap.COLLECTION_PIXEL_ADD_TO_CART), TrackingPixelDataSet.class).persist().createOrReplaceTempView("T_PIXEL_ADD_TO_CART");
		MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", rawDataBaseDaily).withOption("collection", analyticsDailyDate + MongodbMap.COLLECTION_PIXEL_PURCHASED), TrackingPixelDataSet.class).persist().createOrReplaceTempView("T_PIXEL_PURCHASED_RAW");
		MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", processedDataBaseDaily).withOption("collection", analyticsDailyDate + MongodbMap.COLLECTION_PIXEL_PURCHASED), Purchased.class).persist().createOrReplaceTempView("T_PIXEL_PURCHASED");
		MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", rawDataBaseDaily).withOption("collection", analyticsDailyDate + MongodbMap.COLLECTION_WEB_CLICK), Click.class).createOrReplaceTempView("T_WEB_CLICK");
	}

	/**
	 * 시간별로 RAW 데이터를 가져온다.
	 *
	 * @param sparkSession
	 * @param dateTimeHourHH
	 */
	public void createPixelRawDataHourlyTempView(SparkSession sparkSession) {
		LogStack.coreSys.info("rawDataBase :" + rawDataBaseHourly);
		LogStack.coreSys.info("processedDataBase :" + processedDataBaseHourly);
		LogStack.coreSys.info("analyticsDate :" + analyticsHourlyDate);
		LogStack.coreSys.info("dateTimeHourHHmm :" + analyticsHourlyDateTime);
		MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", rawDataBaseHourly).withOption("collection", analyticsHourlyDate + MongodbMap.COLLECTION_PIXEL_PAGE_VIEW), TrackingPixelDataSet.class).where("logDateTime LIKE '"+analyticsHourlyDateTime+"%'").persist().createOrReplaceTempView("T_PIXEL_PAGE_VIEW");
		MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", rawDataBaseHourly).withOption("collection", analyticsHourlyDate + MongodbMap.COLLECTION_PIXEL_VIEW_CONTENT), TrackingPixelDataSet.class).where("logDateTime LIKE '"+analyticsHourlyDateTime+"%'").persist().createOrReplaceTempView("T_PIXEL_VIEW_CONTENT");
		MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", rawDataBaseHourly).withOption("collection", analyticsHourlyDate + MongodbMap.COLLECTION_PIXEL_ADD_TO_CART), TrackingPixelDataSet.class).where("logDateTime LIKE '"+analyticsHourlyDateTime+"%'").persist().createOrReplaceTempView("T_PIXEL_ADD_TO_CART");
		MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", rawDataBaseHourly).withOption("collection", analyticsHourlyDate + MongodbMap.COLLECTION_PIXEL_PURCHASED), TrackingPixelDataSet.class).where("logDateTime LIKE '"+analyticsHourlyDateTime+"%'").persist().createOrReplaceTempView("T_PIXEL_PURCHASED_RAW");
		MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", processedDataBaseHourly).withOption("collection", analyticsHourlyDate + MongodbMap.COLLECTION_PIXEL_PURCHASED), Purchased.class).where("logDateTime LIKE '"+analyticsHourlyDateTime+"%'").persist().createOrReplaceTempView("T_PIXEL_PURCHASED");
		MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", rawDataBaseHourly).withOption("collection", analyticsHourlyDate + MongodbMap.COLLECTION_WEB_CLICK), Click.class).where("logDateTime LIKE '"+analyticsHourlyDateTime+"%'").persist().createOrReplaceTempView("T_WEB_CLICK");
	}


	/**
	 * 일별로 Processed 데이터를 로드한다.
	 * @param sparkSession
	 */
	public void createPixelProcessDataDailyTempView(SparkSession sparkSession) {
//		MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", processedDataBaseDaily)
//					.withOption("collection", analyticsDailyDate + MongodbMap.COLLECTION_PIXEL_CONVERSION_RAWDATA),ConversionRawData.class)
//					.persist().createOrReplaceTempView("T_PIXEL_CONVERSION_RAWDATA");
		MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", processedDataBaseDaily)
				.withOption("collection", analyticsDailyDate + MongodbMap.COLLECTION_PIXEL_EVENT_USER), User.class)
				.persist().createOrReplaceTempView("T_PIXEL_EVENT_USER");

	}

	/**
	 * 시간별로 Processed 데이터를 로드한다.
	 * @param sparkSession
	 */
	public void createPixelProcessDataHourlyTempView(SparkSession sparkSession) {
		LogStack.coreSys.info("processedDataBase :" + processedDataBaseHourly);
		LogStack.coreSys.info("analyticsDate :" + analyticsHourlyDate);
		LogStack.coreSys.info("dateTimeHourHHmm :" + analyticsHourlyDateTime);
//		MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", processedDataBaseHourly)
//					.withOption("collection", analyticsHourlyDate + MongodbMap.COLLECTION_PIXEL_CONVERSION_RAWDATA),ConversionRawData.class)
//					.persist().where("logDateTime LIKE '"+analyticsHourlyDateTime+"%'").createOrReplaceTempView("T_PIXEL_CONVERSION_RAWDATA");
		MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", processedDataBaseHourly)
				.withOption("collection", analyticsHourlyDate + MongodbMap.COLLECTION_PIXEL_EVENT_USER_HOURLY),User.class)
				.persist().where("logDateTime LIKE '"+analyticsHourlyDateTime+"%'").createOrReplaceTempView("T_PIXEL_EVENT_USER_HOURLY");
	}

	/**
	 * @param sparkSession
	 *
	 */
	public void createWebReportCodeAndDimensionTempView(SparkSession sparkSession) {

		String trkPixelTable = "(SELECT PIXEL_ID AS pixelId, CATALOG_ID AS catalogId, PIXEL_NAME AS pixelName FROM tracking.T_TRK_PIXEL) AS T_TRK_PIXEL" ;
		String trkWebTable = "(SELECT WEB_ID AS webId, PIXEL_ID AS pixelId,  CATALOG_ID AS catalogId  FROM tracking.T_TRK_WEB WHERE USE_YN = 'Y' AND DEL_YN = 'N' AND PIXEL_ID IS NOT NULL AND TRIM(PIXEL_ID) != '') AS T_TRK_WEB";
		String trkChannelTable = "(SELECT A.WEB_ID AS webId, A.CATALOG_ID	AS catalogId, C.CHANNEL_ID AS channelId, IFNULL(C.UTM_MEDIUM,'') AS utmMedium, IFNULL(C.UTM_SOURCE, '') 	AS utmSource, IFNULL(C.UTM_TERM, '') 	AS utmTerm, IFNULL(C.UTM_CONTENT,'') 	AS utmContent , IFNULL(D.UTM_CAMPAIGN	,'') AS utmCampaign FROM 	T_TRK_WEB A INNER JOIN  T_TRK_USER_WEB B ON 	A.WEB_ID = B.WEB_ID	AND A.USE_YN = 'Y' AND A.DEL_YN = 'N' AND B.USE_YN = 'Y' AND B.DEL_YN = 'N' INNER JOIN  T_TRK_CHANNEL C ON 	B.USER_ID = C.REGIST_ID AND SAVE_TYPE_CODE = 'STC001' 	AND C.USE_YN = 'Y' AND C.DEL_YN = 'N' INNER JOIN 	T_TRK_CHANNEL_UTM D ON 	C.CHANNEL_ID = D.CHANNEL_ID 	AND D.USE_YN = 'Y' AND D.DEL_YN = 'N' ) AS T_TRK_CHANNEL";
		String trkMediaTable = "(SELECT MEDIA_ID AS mediaId, MEDIA_NAME AS mediaName FROM tracking.T_TRK_MEDIA WHERE USE_YN = 'Y' AND DEL_YN = 'N') AS T_TRK_MEDIA";
		String trkCampaignTable = "(SELECT CAMPAIGN_ID  AS campaignId, CAMPAIGN_NAME  AS campaignName FROM tracking.T_TRK_CAMPAIGN WHERE USE_YN = 'Y' AND DEL_YN = 'N') AS T_TRK_CAMPAIGN";

		SparkJdbcConnectionFactory.getSelectJdbcDfLoad(sparkSession, trkPixelTable).persist().createOrReplaceTempView("T_TRK_PIXEL");
		SparkJdbcConnectionFactory.getSelectJdbcDfLoad(sparkSession, trkWebTable).persist().createOrReplaceTempView("T_TRK_WEB");
	    SparkJdbcConnectionFactory.getSelectJdbcDfLoad(sparkSession, trkChannelTable).persist().createOrReplaceTempView("T_TRK_CHANNEL");
	    SparkJdbcConnectionFactory.getSelectJdbcDfLoad(sparkSession, trkMediaTable).persist().createOrReplaceTempView("T_TRK_MEDIA");
	    SparkJdbcConnectionFactory.getSelectJdbcDfLoad(sparkSession, trkCampaignTable).persist().createOrReplaceTempView("T_TRK_CAMPAIGN");
	}

	public String getAnalyticsDailyDate() {
		return analyticsDailyDate;
	}

	public void setAnalyticsDailyDate(String analyticsDailyDate) {
		this.analyticsDailyDate = analyticsDailyDate;
	}

	public String getAnalyticsHourlyDate() {
		return analyticsHourlyDate;
	}

	public void setAnalyticsHourlyDate(String analyticsHourlyDate) {
		this.analyticsHourlyDate = analyticsHourlyDate;
	}

	public String getAnalyticsHourlyDateTime() {
		return analyticsHourlyDateTime;
	}

	public void setAnalyticsHourlyDateTime(String analyticsHourlyDateTime) {
		this.analyticsHourlyDateTime = analyticsHourlyDateTime;
	}

	public String getRawDataBaseDaily() {
		return rawDataBaseDaily;
	}

	public void setRawDataBaseDaily(String rawDataBaseDaily) {
		this.rawDataBaseDaily = rawDataBaseDaily;
	}

	public String getProcessedDataBaseDaily() {
		return processedDataBaseDaily;
	}

	public void setProcessedDataBaseDaily(String processedDataBaseDaily) {
		this.processedDataBaseDaily = processedDataBaseDaily;
	}

	public String getRawDataBaseHourly() {
		return rawDataBaseHourly;
	}

	public void setRawDataBaseHourly(String rawDataBaseHourly) {
		this.rawDataBaseHourly = rawDataBaseHourly;
	}

	public String getProcessedDataBaseHourly() {
		return processedDataBaseHourly;
	}

	public void setProcessedDataBaseHourly(String processedDataBaseHourly) {
		this.processedDataBaseHourly = processedDataBaseHourly;
	}
}
