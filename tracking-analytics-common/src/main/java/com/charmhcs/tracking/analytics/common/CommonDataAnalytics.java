package com.charmhcs.tracking.analytics.common;

import java.net.URLDecoder;
import java.sql.Timestamp;

import com.charmhcs.tracking.analytics.common.map.CodeMap;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;

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
* 2018. 8. 25.   cshwang   Initial Release
*
*
************************************************************************************************/
public abstract class CommonDataAnalytics {	
	
	public enum RunType {batchjob, each}; 
	public final static String HOURLY = "hourly";
	public final static String DAILY = "daily";	
	public final static String ALL = "all";
	public final static String SYSTEM_USER = "analytics_system";
	public final static String BATCHJOB = "batchjob";
	public final static String EACH = "each";
	public final static SaveMode DEFAULT_SAVEMODE = SaveMode.Append;
	private String rawDataBase;
	private String processedDataBase;
	private boolean isValid = false;
	//Batch Job 으로 실행하는지, 개별적으 실행하는지
	private RunType runType = RunType.each;
	
	private String hourFormat = CodeMap.DATE_DAY_OF_HOUR_PATTERN;
	private String dateFormat = CodeMap.DATE_YMD_PATTERN_FOR_ANYS;	
	private String dateHourFormat = CodeMap.DATE_YMDH_PATTERN_FOR_ANYS;
	private String monthFormat = CodeMap.DATE_MONTH_PATTERN;
	
	
	/**
	 * Spark Sql URL_DECODE function 
	 */
	public static final UDF1<String, String> urldecode = new UDF1<String, String>() {
		private static final long serialVersionUID = -1L;
		public String call(final String str)  {
			try {
				return URLDecoder.decode(str, "utf-8");
			}catch(Exception e) {
				return "";
			}
	    }
	};
	
	/**
	 * spark sql add_hours
	 */
	public static final UDF2<Timestamp, Integer, Timestamp> addhours = new UDF2<Timestamp, Integer, Timestamp>() {
		private static final long serialVersionUID = -1L;
		public Timestamp call(final Timestamp datetime, final Integer hours){
			try {
				return new Timestamp(datetime.getTime() + hours * 60 * 60 * 1000 );
			}catch(Exception e) {
				return null;
			}
		}
	};


	public boolean isValid() {
		return isValid;
	}

	public void setValid(boolean isValid) {
		this.isValid = isValid;
	}

	public RunType getRunType() {
		return runType;
	}

	public void setRunType(RunType runType) {
		this.runType = runType;
	}
	

	public String getProcessedDataBase() {
		return processedDataBase;
	}

	public void setProcessedDataBase(String processedDataBase) {
		this.processedDataBase = processedDataBase;
	}

	public String getRawDataBase() {
		return rawDataBase;
	}

	public void setRawDataBase(String rawDataBase) {
		this.rawDataBase = rawDataBase;
	}

	public String getHourFormat() {
		return hourFormat;
	}

	public void setHourFormat(String hourFormat) {
		this.hourFormat = hourFormat;
	}

	public String getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

	public String getDateHourFormat() {
		return dateHourFormat;
	}

	public void setDateHourFormat(String dateHourFormat) {
		this.dateHourFormat = dateHourFormat;
	}

	public String getMonthFormat() {
		return monthFormat;
	}

	public void setMonthFormat(String monthFormat) {
		this.monthFormat = monthFormat;
	}
}