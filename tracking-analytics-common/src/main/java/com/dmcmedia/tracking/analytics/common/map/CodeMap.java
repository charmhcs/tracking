package com.dmcmedia.tracking.analytics.common.map;

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
* 2017. 5. 11.   cshwang   Initial Release
*
*
************************************************************************************************/
public class CodeMap {

	public static final String PROPERTY_SPARK_MASTER 				= "spark.master";
	public static final String PROPERTY_SPARK_APP_ID 				= "spark.app.id";
	public static final String PROPERTY_SPARK_APP_NAME 				= "spark.app.name";
	public static final String PROPERTY_SPARK_MONGODB_URI 			= "spark.mongodb.uri";
    public static final String PROPERTY_SPARK_MONGODB_INPUT_URI 	= "spark.mongodb.input.uri";
    public static final String PROPERTY_SPARK_MONGODB_OUTPUT_URI 	= "spark.mongodb.output.uri";    
    public static final String PROPERTY_JDBC_DRIVER 				= "jdbc.driver"; 
    public static final String PROPERTY_JDBC_URL					= "jdbc.url"; 
    public static final String PROPERTY_JDBC_USER 					= "jdbc.user"; 
    public static final String PROPERTY_JDBC_PASSWORD 				= "jdbc.password";     
     
    public static final String MONTH_01 = "01";
    public static final String MONTH_02 = "02";
    public static final String MONTH_03 = "03";
    public static final String MONTH_04 = "04";
    public static final String MONTH_05 = "05";
    public static final String MONTH_06 = "06";
    public static final String MONTH_07 = "07";
    public static final String MONTH_08 = "08";
    public static final String MONTH_09 = "09";
    public static final String MONTH_10 = "10";
    public static final String MONTH_11 = "11";
    public static final String MONTH_12 = "12";
    
	public static final String BLANK = "";
	public static final String TRUE = "Y";
	public static final String FALSE = "N";	
	public static final String DELETE = "D";	
	public static final String UNKOWN = "UNKNOWN";
	public static final String NONE = "None";
	public static final String ETC_STR = "etc";
	public static final String ZERO_STR = "0";
	public static final String ONE_STR = "1";
	public static final int NG_ONE = -1;
	public static final int NG_FIVE = -5;
	public static final int NG_SEVEN = -7;
    public static final int ZERO = 0;
    public static final int ONE = 1; 
    public static final int FIVE = 5;
    public static final int DATE_TIME_LENGTH = 10; 
    public static final int DATE_LENGTH = 8; 
    public static final String DATE_TIME_STR = "T";
    public static final int HUNDRED = 100;
    public static final int THOUSAND = 1000;    
    public static final int SMALL_DATA_SIZE = 1000;    
    public static final int MID_DATA_SIZE = 5000;   
    public static final int LARGE_DATA_SIZE = 10000;       
    public static final String ENTER = "\n";       
    public static final String CHARSET_UTF8 = "UTF-8";
    
    public static final String DATE_YMDH_PATTERN = "yyyyMMddHH";
    public static final String DATE_YMD_PATTERN = "yyyyMMdd";
    public static final String DATE_YMDHMS_PATTERN = "yyyyMMddHHmmss";
    public static final String DATE_YMDHMSS_PATTERN = "yyyyMMddHHmmssSSS";
    public static final String DATE_ISODATE = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    public static final String DATE_MONTH_PATTERN = "MM";
    public static final String DATE_MMSS_PATTERN = "mmss";
    public static final String DATE_MM_PATTERN = "mm";
    public static final String DATE_YMD_PATTERN_FOR_ANYS = "yyyy-MM-dd";
    public static final String DATE_YMDH_PATTERN_FOR_ANYS = "yyyy-MM-dd HH";
    public static final String DATE_YMDHM_PATTERN_FOR_ANYS = "yyyy-MM-dd HH:mm";
    public static final String DATE_YMDHMS_PATTERN_FOR_USER = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_DAY_OF_WEEK_PATTERN = "e";
    public static final String DATE_DAY_OF_HOUR_PATTERN = "HH";	
    public static final String DATE_HOUR_PATTERN = "H";	
}
