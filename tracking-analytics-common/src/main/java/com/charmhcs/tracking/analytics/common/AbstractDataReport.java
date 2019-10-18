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
* 2018. 8. 23.   cshwang   Initial Release
*
* 	Raw Data 혹은 Processed Data를 이용하여, 분석/가공 데이터를 처리함 
* 
* 		- Mongodb Processed Data  -> RDBMS report Data 
* 
************************************************************************************************/
public abstract class AbstractDataReport extends CommonDataAnalytics implements DataReport{
	
	/* 
	 * all은 hourly + daily
	 */
	public enum ReportType {hourly, daily, all};
	public final static int DEFAULT_INTERVAL_DAY = CodeMap.NG_ONE;
	public final static int DEFAULT_INTERVAL_HOUR = CodeMap.NG_ONE;
	
	private String reportMonth;
	private String reportDate;	
	private String reportHour;
	private String reportDateHour;	
	
	/**
	 * report 처리 상태
	 */
	private ReportType reportType;
	
	/**
	 * 기본 생성자
	 */
	public AbstractDataReport() {
		initDateReport(ALL);
	} 
	
	/**
	 * 기본 생성자 (처리 타입)
	 * @param inputRunType
	 */
	public AbstractDataReport(String inputRunType) {
		if(RunType.batchjob.name().equals(inputRunType)){
			setRunType(RunType.batchjob);
		}else {
			setRunType(RunType.each);
		}
		initDateReport(ALL);
	}
	
	/**
	 * 기본 생성자 (처리 타입)
	 * @param inputRunType
	 * @param ReportsType
	 */
	public AbstractDataReport(String inputRunType, String inputReportType) {
		if(RunType.batchjob.name().equals(inputRunType)){
			setRunType(RunType.batchjob);
		}else {
			setRunType(RunType.each);
		}
		initDateReport(inputReportType);
	}
	
	/**
	 * @param inputReportType
	 */
	private void initDateReport(String inputReportType) {
		
		LogStack.report.info("Running " + this.getClass().getSimpleName());
		try{		
			switch(inputReportType) {
				case HOURLY :
					setReportDate(DateUtil.getIntervalHour(this.getDateFormat(), DEFAULT_INTERVAL_HOUR));
					setReportDateHour(DateUtil.getIntervalHour(this.getDateHourFormat(),DEFAULT_INTERVAL_HOUR));
					setReportMonth(DateUtil.getIntervalHour(this.getMonthFormat(), DEFAULT_INTERVAL_HOUR));
					setReportHour(DateUtil.getIntervalHour(this.getHourFormat(), DEFAULT_INTERVAL_HOUR));
					setReportType(ReportType.hourly);
					break;
				default :  // all
					setReportDate(DateUtil.getIntervalDate(this.getDateFormat(), DEFAULT_INTERVAL_DAY));
					setReportDateHour(DateUtil.getIntervalDate(this.getDateFormat(), DEFAULT_INTERVAL_DAY)); // all 인 경우 사용할 수 있음. 
					setReportMonth(DateUtil.getIntervalDate(this.getMonthFormat(), DEFAULT_INTERVAL_DAY));				
					if(ReportType.daily.name().equals(inputReportType)){
						setReportType(ReportType.daily);
					}else {
						setReportType(ReportType.all);
					}
			}
			setRawDataBase(MongodbMap.DATABASE_RAW_MONTH + getReportMonth());
			setProcessedDataBase(MongodbMap.DATABASE_PROCESSED_MONTH + getReportMonth());
			this.setValid(true);
		}catch(Exception e){
			setValid(false);
			LogStack.report.error(ExceptionMap.INVALID_DATA_FORMAT_EXCEPTION);
		}
	}
	
	/**
	 * @param sparkSession
	 * @param reportDateTime
	 * @return
	 */
	public boolean reportsDataSet(SparkSession sparkSession, String reportDateTime) {
		try{
			 switch(getReportType().name()) {
				case HOURLY :					
					if(DateUtil.checkDateFormat(reportDateTime, this.getDateHourFormat())){
						this.setReportDate(DateUtil.getChangeDateFormat(reportDateTime, this.getDateHourFormat(), this.getDateFormat()));			
						this.setReportMonth(DateUtil.getChangeDateFormat(reportDateTime, this.getDateHourFormat(), this.getMonthFormat()));
						this.setReportDateHour(reportDateTime);			
						this.setReportHour(DateUtil.getChangeDateFormat(reportDateTime, this.getDateHourFormat(), this.getHourFormat()));
						
					}else if(DateUtil.checkDateFormat(reportDateTime, this.getDateFormat())){
						this.setReportDate(DateUtil.getChangeDateFormat(reportDateTime, this.getDateFormat(), this.getDateFormat()));	
						this.setReportDateHour(DateUtil.getChangeDateFormat(reportDateTime, this.getDateFormat(), this.getDateFormat()));	
						this.setReportMonth(DateUtil.getChangeDateFormat(reportDateTime, this.getDateFormat(), this.getMonthFormat()));
						this.setReportHour(CodeMap.BLANK);
					}else{
						throw new Exception(ExceptionMap.INVALID_DATA_FORMAT_EXCEPTION);
					}
					break;
				default :
					this.setReportDate(DateUtil.getChangeDateFormat(reportDateTime, this.getDateFormat(), this.getDateFormat()));			
					this.setReportDateHour(DateUtil.getChangeDateFormat(reportDateTime, this.getDateFormat(), this.getDateFormat()));
					this.setReportMonth(DateUtil.getChangeDateFormat(reportDateTime, this.getDateFormat(), this.getMonthFormat()));
		    }
			setRawDataBase(MongodbMap.DATABASE_RAW_MONTH + getReportMonth());
			setProcessedDataBase(MongodbMap.DATABASE_PROCESSED_MONTH + getReportMonth());
			
			LogStack.report.info("reportDateHour :" +getReportDateHour());	
			LogStack.report.info("reportDate :" +getReportDate());	
			LogStack.report.info("reportMonth :" +getReportMonth());	
			LogStack.report.info("reportHour :" +getReportHour());	
			LogStack.report.info("rawDataBase :" +getRawDataBase());	
			LogStack.report.info("processedDataBase :" +getProcessedDataBase());	
			
			setValid(true);
		}catch(Exception e){
			setValid(false);
			LogStack.report.error(ExceptionMap.INVALID_DATA_FORMAT_EXCEPTION);			
		}
		return reportsDataSet(sparkSession);
	}

	public String getReportDate() {
		return reportDate;
	}

	public void setReportDate(String reportDate) {
		this.reportDate = reportDate;
	}

	public String getReportDateHour() {
		return reportDateHour;
	}

	public void setReportDateHour(String reportDateHour) {
		this.reportDateHour = reportDateHour;
	}

	public String getReportMonth() {
		return reportMonth;
	}

	public void setReportMonth(String reportMonth) {
		this.reportMonth = reportMonth;
	}

	public ReportType getReportType() {
		return reportType;
	}

	public void setReportType(ReportType reportStatus) {
		this.reportType = reportStatus;
	}

	public String getReportHour() {
		return reportHour;
	}

	public void setReportHour(String reportHour) {
		this.reportHour = reportHour;
	}
}
