package com.charmhcs.tracking.analytics.common;

import com.charmhcs.tracking.analytics.common.map.CodeMap;
import com.charmhcs.tracking.analytics.common.map.ExceptionMap;
import com.charmhcs.tracking.analytics.common.map.MongodbMap;
import com.charmhcs.tracking.analytics.common.util.DateUtil;
import com.charmhcs.tracking.analytics.common.util.LogStack;
import org.apache.spark.sql.SparkSession;

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
* 2018-12-29   cshwang  Initial Release
*
*	1/5분 연속배치 동작에 대한 기본 정의 
*
************************************************************************************************/
public abstract class AbstractDataMinutelyAnalysis extends CommonDataAnalytics implements DataAnalysis{
	
	public enum AnalysisType {minutely};
	public final static String MINUTE = "minute";
	public final static int DEFAULT_INTERVAL_MINUTE = CodeMap.NG_FIVE;
	private int intervalMinute;
	private String analysisDate;
	private String analysisMonth;
	private String analysisDateHourMinute;
	private String currentAnalysisDateHourMinute; // 동작 시
	
	/**
	 * 기본 생성자
	 */
	public AbstractDataMinutelyAnalysis() {
		initDataMinutelyAnalysis(DEFAULT_INTERVAL_MINUTE);
	}
	/**
	 * RawData의 Analysis 범위를 분 단위로 설정  
	 * @param intervalMinute
	 */
	public AbstractDataMinutelyAnalysis(int intervalMinute) {
		initDataMinutelyAnalysis(intervalMinute);
	}
	
	/**
	 * 생성자 공통 동작 메소드 
	 * @param intervalMinute
	 */
	private void initDataMinutelyAnalysis(int intervalMinute) {
		
		LogStack.analysis.info("Running " + this.getClass().getSimpleName());
		try{
			setIntervalMinute(intervalMinute);
			this.setAnalysisDate(DateUtil.getCurrentDate(CodeMap.DATE_YMD_PATTERN_FOR_ANYS));
			this.setAnalysisMonth(DateUtil.getCurrentDate(CodeMap.DATE_MONTH_PATTERN));
			this.setAnalysisDateHourMinute(DateUtil.getIntervalMinute(CodeMap.DATE_YMDHM_PATTERN_FOR_ANYS, getIntervalMinute()));
			this.setCurrentAnalysisDateHourMinute(DateUtil.getCurrentDate(CodeMap.DATE_YMDHM_PATTERN_FOR_ANYS));
			setRawDataBase(MongodbMap.DATABASE_RAW_MONTH + getAnalysisMonth());
			setProcessedDataBase(MongodbMap.DATABASE_PROCESSED_MONTH + getAnalysisMonth());
			this.setValid(true);
		}catch(Exception e){
			setValid(false);
			LogStack.analysis.error(ExceptionMap.ANALYSIS_EXCEPTION);
			LogStack.analysis.error(e);
		}
	}

	/* (non-Javadoc)
	 * @see com.dmcmedia.tracking.analytics.common.DataAnalysis#AnalysissDataSet(org.apache.spark.sql.SparkSession, java.lang.String)
	 * 
	 * 커스텀으로 날짜를 가져오는 경우는 다시 셋팅한다.
	 */
	@Override
	public boolean analyzeDataSet(SparkSession sparkSession, String analysisDateTime) {
		try{
			this.setAnalysisDate(DateUtil.getChangeDateFormat(analysisDateTime, CodeMap.DATE_YMDHM_PATTERN_FOR_ANYS, CodeMap.DATE_YMD_PATTERN_FOR_ANYS));
			this.setAnalysisMonth(DateUtil.getChangeDateFormat(analysisDateTime, CodeMap.DATE_YMDHM_PATTERN_FOR_ANYS, CodeMap.DATE_MONTH_PATTERN));
			this.setAnalysisDateHourMinute(DateUtil.getChangeDateFormatIntervalMinute(analysisDateTime, CodeMap.DATE_YMDHM_PATTERN_FOR_ANYS, CodeMap.DATE_YMDHM_PATTERN_FOR_ANYS, getIntervalMinute() ));
			this.setCurrentAnalysisDateHourMinute(DateUtil.getChangeDateFormat(analysisDateTime, CodeMap.DATE_YMDHM_PATTERN_FOR_ANYS, CodeMap.DATE_YMDHM_PATTERN_FOR_ANYS));
			setRawDataBase(MongodbMap.DATABASE_RAW_MONTH + getAnalysisMonth());
			setProcessedDataBase(MongodbMap.DATABASE_PROCESSED_MONTH + getAnalysisMonth());
			setValid(true);
			LogStack.analysis.debug(getAnalysisDate());
			LogStack.analysis.debug(getAnalysisDateHourMinute());
			LogStack.analysis.debug(getCurrentAnalysisDateHourMinute());
		}catch(Exception e){
			setValid(false);
			LogStack.analysis.error(ExceptionMap.ANALYSIS_EXCEPTION);
			LogStack.analysis.error(e);
		}
		return analyzeDataSet(sparkSession);
	}

	public String getAnalysisDate() {
		return analysisDate;
	}

	public void setAnalysisDate(String analysisDate) {
		this.analysisDate = analysisDate;
	}

	public String getAnalysisDateHourMinute() {
		return analysisDateHourMinute;
	}

	public void setAnalysisDateHourMinute(String analysisDateHourMinute) {
		this.analysisDateHourMinute = analysisDateHourMinute;
	}

	public String getCurrentAnalysisDateHourMinute() {
		return currentAnalysisDateHourMinute;
	}

	public void setCurrentAnalysisDateHourMinute(String currentAnalysisDateHourMinute) {
		this.currentAnalysisDateHourMinute = currentAnalysisDateHourMinute;
	}

	public String getAnalysisMonth() {
		return analysisMonth;
	}

	public void setAnalysisMonth(String analysisMonth) {
		this.analysisMonth = analysisMonth;
	}

	public int getIntervalMinute() {
		return intervalMinute;
	}

	public void setIntervalMinute(int intervalMinute) {
		this.intervalMinute = intervalMinute;
	}
}
