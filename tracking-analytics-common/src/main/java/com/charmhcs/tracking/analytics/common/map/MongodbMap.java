package com.charmhcs.tracking.analytics.common.map;

/**  
*
*	Mongo DB Collection
*
*   날  짜      성 명                     수정  내용
* ----------  ---------  ------------------------------------------------------------------------
* 2015. 11. 4.   cshwang   Initial Release
*
*
************************************************************************************************/
public class MongodbMap {

	public static final int DEFAULT_LIMIT_SIZE = 10;

	public static final String DATABASE_TRACKING 				= "tracking";
	public static final String DATABASE_ANALYSIS 				= "tracking_analysis";
	public static final String DATABASE_PROCESSED_FEED 			= "tracking_processed_feed";
	public static final String DATABASE_PROCESSED_USER			= "tracking_processed_user";
	public static final String DATABASE_PROCESSED_AUDIENCE		= "tracking_processed_audience";
	public static final String DATABASE_PROCESSED_MONTH			= "tracking_processed_m";
	public static final String DATABASE_RAW_MONTH				= "tracking_raw_m";
	public static final String DATABASE_3RDPARTY_TRACKING		= "11st";


	public static final String COLLECTION_TRACKING						= "tracking";
	public static final String COLLECTION_APP 							= "app";
	public static final String COLLECTION_PIXEL							= "pixel";
	public static final String COLLECTION_ADPOOL						= "dsp";
	public static final String COLLECTION_NEXTVIP						= "network";
	public static final String COLLECTION_ALL							= "all";

	public static final String COLLECTION_DOT							= ".";
	public static final String COLLECTION_TRACKING_DEFAULT				= "tracking";
	public static final String COLLECTION_PIXEL_PAGE_VIEW 				= ".pixel.pageview";
    public static final String COLLECTION_PIXEL_VIEW_CONTENT 			= ".pixel.viewcontent";
    public static final String COLLECTION_PIXEL_CATALOG 				= ".pixel.catalog";
    public static final String COLLECTION_PIXEL_ADD_TO_CART 			= ".pixel.cart";
    public static final String COLLECTION_PIXEL_COMPLETE_REGISTRATION 	= ".pixel.registration";
    public static final String COLLECTION_PIXEL_PURCHASED 				= ".pixel.purchased";
    public static final String COLLECTION_PIXEL_ERROR 					= ".pixel.error";
    public static final String COLLECTION_PIXEL_ETC 					= ".pixel.etc";

    public static final String COLLECTION_APP_IMPRESSION				= ".app.impression";
    public static final String COLLECTION_APP_CLICK						= ".app.click";
    public static final String COLLECTION_APP_EXECUTE					= ".app.execute";
    public static final String COLLECTION_APP_DEVICE_INFO				= ".app.deviceinfo";
    public static final String COLLECTION_APP_REFERRER 					= ".app.referrer";
    public static final String COLLECTION_APP_INSTALL 					= ".app.install";
    public static final String COLLECTION_APP_PAGE_VIEW 				= ".app.pageview";
    public static final String COLLECTION_APP_CATALOG 					= ".app.catalog";
    public static final String COLLECTION_APP_VIEW_CONTENT 				= ".app.viewcontent";
    public static final String COLLECTION_APP_ADD_TO_CART 				= ".app.addtocart";
    public static final String COLLECTION_APP_COMPLETE_REGISTRATION 	= ".app.registration";
    public static final String COLLECTION_APP_PURCHASED 				= ".app.purchased";
    public static final String COLLECTION_APP_ERROR 					= ".app.error";
    public static final String COLLECTION_APP_ETC 						= ".app.etc";

    public static final String COLLECTION_WEB_IMPRESSION				= ".web.impression";
    public static final String COLLECTION_WEB_CLICK						= ".web.click";

    public static final String COLLECTION_DAYS							= "days";
	public static final String COLLECTION_PIXEL_FEED					= ".pixel.feed";
	public static final String COLLECTION_PIXEL_FEED_DAILY 				= ".pixel.feed.daily";
	public static final String COLLECTION_PIXEL_FEED_HOURLY 			= ".pixel.feed.hourly";
	public static final String COLLECTION_PIXEL_PROCESSED_PURCHASED 	= ".pixel.purchased";
	public static final String COLLECTION_PIXEL_CATEGORY 				= ".pixel.category";
	public static final String COLLECTION_PIXEL_CATEGORY_LV1			= ".pixel.category.lv1";
	public static final String COLLECTION_PIXEL_CATEGORY_LV2			= ".pixel.category.lv2";
	public static final String COLLECTION_PIXEL_AUDIENCE 				= ".pixel.audience";
	public static final String COLLECTION_PIXEL_AUDIENCE_DAILY 			= ".pixel.audience.daily";
	public static final String COLLECTION_PIXEL_AUDIENCE_HOURLY 		= ".pixel.audience.hourly";
	public static final String COLLECTION_PIXEL_EVENT 					= ".pixel.event";
	public static final String COLLECTION_PIXEL_EVENT_PRODUCTS			= ".pixel.event.products";
	public static final String COLLECTION_PIXEL_EVENT_AMOUNT			= ".pixel.event.amount";
	public static final String COLLECTION_PIXEL_EVENT_USER				= ".pixel.event.user";
	public static final String COLLECTION_PIXEL_EVENT_USER_HOURLY		= ".pixel.event.user.hourly";
	public static final String COLLECTION_PIXEL_NEWUSER					= ".pixel.newuser";
	public static final String COLLECTION_PIXEL_USER					= ".pixel.user";
	public static final String COLLECTION_PIXEL_USER_PURCHASED			= ".pixel.user.purchased";
	public static final String COLLECTION_PIXEL_PATHING_DOMAIN			= ".pixel.pathing.domain";
	public static final String COLLECTION_PIXEL_PATHING_KEYWORD			= ".pixel.pathing.keyword";
	public static final String COLLECTION_PIXEL_CONVERSION_RAWDATA		= ".pixel.conversion.rawdata";

	public static final String COLLECTION_APP_FEED					= ".app.feed";
	public static final String COLLECTION_APP_FEED_DAILY 			= ".app.feed.daily";
	public static final String COLLECTION_APP_FEED_HOURLY 			= ".app.feed.hourly";
	public static final String COLLECTION_APP_PROCESSED_PURCHASED 	= ".app.purchased";
	public static final String COLLECTION_APP_CATEGORY 				= ".app.category";
	public static final String COLLECTION_APP_CATEGORY_LV1			= ".app.category.lv1";
	public static final String COLLECTION_APP_CATEGORY_LV2			= ".app.category.lv2";
	public static final String COLLECTION_APP_AUDIENCE 				= ".app.audience";
	public static final String COLLECTION_APP_AUDIENCE_DAILY 		= ".app.audience.daily";
	public static final String COLLECTION_APP_AUDIENCE_HOURLY 		= ".app.audience.hourly";
	public static final String COLLECTION_APP_EVENT 				= ".app.event";
	public static final String COLLECTION_APP_EVENT_PRODUCTS		= ".app.event.products";
	public static final String COLLECTION_APP_EVENT_AMOUNT			= ".app.event.amount";
	public static final String COLLECTION_APP_EVENT_USER			= ".app.event.user";
	public static final String COLLECTION_APP_EVENT_USER_HOURLY		= ".app.event.user.hourly";
	public static final String COLLECTION_APP_EVENT_INSTALL			= ".app.event.install";
	public static final String COLLECTION_APP_USER					= ".app.user";
	public static final String COLLECTION_APP_CONVERSION_RAWDATA	= ".app.conversion.rawdata";

	public static final String COLLECTION_3RDPT						= "zh_track_";
	public static final String COLLECTION_3RDPT_RAWDATA				= ".3rdpt.rawdata";
	public static final String COLLECTION_3RDPT_ADBRIX				= ".3rdpt.adbrix";
	public static final String COLLECTION_3RDPT_SINGULAR			= ".3rdpt.singular";
	public static final String COLLECTION_3RDPT_TUNE				= ".3rdpt.trun";
	public static final String COLLECTION_3RDPT_CONVERSION_RAWDATA	= ".3rdpt.conversion.rawdata";
	public static final String COLLECTION_3RDPT_EVENT_USER			= ".3rdpt.event.user";

	public static final String COLLECTION_ANALYSIS_PIXEL_DAYS_SINCE_LAST_ORDER 	= "pixel.days.since.last.order";
	public static final String COLLECTION_ANALYSIS_PIXEL_TOP_REVENUE_CHANNELS 	= "pixel.top.revenue.channels";
	public static final String COLLECTION_ANALYSIS_PIXEL_PATHING_URL 			= "pixel.pathing.url";
	public static final String COLLECTION_ANALYSIS_PIXEL_PATHING_KEYWORD		= "pixel.pathing.keyword";
	public static final String COLLECTION_ANALYSIS_APP_TOP_REVENUE_CHANNELS		= "app.top.revenue.channels";

	public static final String FIELD_ID = "_id";
	public static final String FIELD_LOGDATETIME = "logDateTime";
	public static final String FIELD_TRACKING_TYPE_CODE = "trackingTypeCode";
	public static final String FIELD_TRACKING_EVENT_CODE = "trackingEventCode";
	public static final String FIELD_NAME = "name";
	public static final String FIELD_EC = "ec";
	public static final String FIELD_PIXEL_ID = "pixelId";
	public static final String FIELD_CATALOG_ID = "catalogId";
	public static final String FIELD_EVENT_TYPE = "event_type";
	public static final String INDEX_NAME_ID_DT = "idx_dt_id";
	public static final String INDEX_NAME_DT = "idx_dt";
	public static final String INDEX_NAME_DK_CT = "idx_dk_ct";
	public static final String INDEX_NAME_LOGDATE_ID = "idx_logdatetime_id";
	public static final String INDEX_NAME_LOGDATE = "idx_logdatetime";
	public static final String INDEX_NAME_TRACKING = "idx_tracking";
	public static final String FIELD_PX = "px";
	public static final String FIELD_PC = "pc";
}
