package com.dmcmedia.tracking.analytics.common;

import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.map.CodeMap;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.map.MongodbMap;
import com.dmcmedia.tracking.analytics.common.util.DateUtil;
import com.dmcmedia.tracking.analytics.common.util.LogStack;

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
* 2017. 5. 22.   cshwang   Initial Release
* 
* 	대량 이벤트 및 선처리 위한 데이터 가공 기능을 정의함 
* 		- Raw Data Mongodb 	-> Processed Data Mongodb 
* 		- Raw Data Logfile 	-> Processed Data Mongodb
* 		- Raw Data Mongodb 	-> Processed Data Logfile
*
************************************************************************************************/
public abstract class AbstractDataProcess extends CommonDataAnalytics implements DataProcess {
	
	public final static int DEFAULT_INTERVAL_DAY = CodeMap.NG_ONE;
	public final static int DEFAULT_INTERVAL_HOUR = CodeMap.NG_ONE;	
	
	public enum ProcessType {all, hourly, daily, days}
	public final static String 	DAYS = "days";
	public final static String 	ALL = "all";
	public final static String 	MAX_HOUR_STR = "23";	
	public final static int    	DAYS_3 = 3;	
	public final static int    	DAYS_7 = 7;	
	public final static int    	DAYS_14 = 14;
	public final static int    	DAYS_28 = 28;
	
	//Process 처리 상태
	private ProcessType processType;
	// 	process 처리 시간 설정 
	private int days = 0;
	private String processDate;	
	private String processDateHour;	
	private String processMonth;
	private String processWeek;
	private String processHour;
	private String previousProcessDate;
	private String previous7DaysProcessedDate;	
	
	/**
	 * 기본 생성자 ()
	 */
	public AbstractDataProcess(){		
		initDataProcess(ALL);
	}
	
	/**
	 * 기본 생성자 (처리 타입)
	 * @param inputRunType
	 */
	public AbstractDataProcess(String inputRunType){		
		if(RunType.batchjob.name().equals(inputRunType)){
			setRunType(RunType.batchjob);
		}else {
			setRunType(RunType.each);
		}
		initDataProcess(ALL);
	}
	
	/**
	 * 기본 생성자 (실행 타입)
	 * @param processType
	 * @param inputRunType
	 */
	public AbstractDataProcess(String inputRunType, String processType) {
		if(RunType.batchjob.name().equals(inputRunType)){
			setRunType(RunType.batchjob);
		}else {
			setRunType(RunType.each);
		}
		initDataProcess(processType);
	}
	
	/**
	 * @param processType
	 * 실행 시간 기준으로 일자가 셋팅됨 
	 */
	private void initDataProcess(String processType) {
		LogStack.process.info("Running " + this.getClass().getSimpleName());
		try{
			if(HOURLY.equals(processType)){
				setProcessDateHour(DateUtil.getIntervalHour(this.getDateHourFormat(), DEFAULT_INTERVAL_HOUR));
				setProcessDate(DateUtil.getIntervalHour(this.getDateFormat(), DEFAULT_INTERVAL_HOUR));
				setProcessMonth(DateUtil.getIntervalHour(this.getMonthFormat(), DEFAULT_INTERVAL_HOUR));
				setProcessHour(DateUtil.getIntervalHour(this.getHourFormat(), DEFAULT_INTERVAL_HOUR));
			}else {
				setProcessDate(DateUtil.getIntervalDate(this.getDateFormat(), DEFAULT_INTERVAL_DAY));
				setProcessMonth(DateUtil.getIntervalDate(this.getMonthFormat(), DEFAULT_INTERVAL_DAY));
				setProcessHour(MAX_HOUR_STR); // 23:59:55.999 
			}
			
			switch(processType) {
				case HOURLY :
					setProcessType(ProcessType.hourly);
					break;
				case DAILY :
					setProcessType(ProcessType.daily);
					break;
				case DAYS :
					setDays(DAYS_28);
					setProcessType(ProcessType.days);
					break;
				case ALL :
					setDays(DAYS_28);
					setProcessType(ProcessType.all);
					break;
				default :	
					LogStack.process.error(ExceptionMap.INVALID_DATA_FORMAT_EXCEPTION);
			}
			setPreviousProcessDate(DateUtil.getChangeDateFormatIntervalDay(getProcessDate(), this.getDateFormat(), this.getDateFormat(), CodeMap.NG_ONE));
			setPrevious7DaysProcessedDate(DateUtil.getChangeDateFormatIntervalDay(getProcessDate(), this.getDateFormat(), this.getDateFormat(), CodeMap.NG_SEVEN));
			setRawDataBase(MongodbMap.DATABASE_RAW_MONTH + getProcessMonth());
			setProcessedDataBase(MongodbMap.DATABASE_PROCESSED_MONTH + getProcessMonth());
			setValid(true);
		}catch(Exception e){
			setValid(false);
			LogStack.process.error(ExceptionMap.INVALID_DATA_FORMAT_EXCEPTION);
		}
	}
	
	
	/* (non-Javadoc)
	 * @see com.dmcmedia.tracking.analytics.common.DataProcess#processDataSet(org.apache.spark.sql.SparkSession, java.lang.String)
	 * 
	 * 임의의 시간이 입력되면 그에 설정됨 
	 */
	@Override
	public boolean processDataSet(SparkSession sparkSession, String processDateTime) {
		// TODO Auto-generated method stub
		try {
			if(HOURLY.equals(getProcessType().name()) && DateUtil.checkDateFormat(processDateTime, this.getDateHourFormat())){ 
				setProcessDateHour(DateUtil.getChangeDateFormat(processDateTime, this.getDateHourFormat(), this.getDateHourFormat()));			
				setProcessDate(DateUtil.getChangeDateFormat(processDateTime, this.getDateHourFormat(), this.getDateFormat()));				
				setProcessMonth(DateUtil.getChangeDateFormat(processDateTime, this.getDateHourFormat(), this.getMonthFormat()));
				setProcessHour(DateUtil.getChangeDateFormat(processDateTime, this.getDateHourFormat(), this.getHourFormat()));
			}else if(DateUtil.checkDateFormat(processDateTime, this.getDateFormat())) {
				setProcessDateHour(processDateTime);
				setProcessDate(DateUtil.getChangeDateFormat(processDateTime, this.getDateFormat(), this.getDateFormat()));				
				setProcessMonth(DateUtil.getChangeDateFormat(processDateTime, this.getDateFormat(), this.getMonthFormat()));
				setProcessHour(MAX_HOUR_STR); // 23:59:55.999 
			}else {
				throw new Exception(ExceptionMap.INVALID_DATA_FORMAT_EXCEPTION);
			}
			setPreviousProcessDate(DateUtil.getChangeDateFormatIntervalDay(getProcessDate(), this.getDateFormat(), this.getDateFormat(), CodeMap.NG_ONE));
			setPrevious7DaysProcessedDate(DateUtil.getChangeDateFormatIntervalDay(getProcessDate(), this.getDateFormat(), this.getDateFormat(), CodeMap.NG_SEVEN));
			setRawDataBase(MongodbMap.DATABASE_RAW_MONTH + getProcessMonth());
			setProcessedDataBase(MongodbMap.DATABASE_PROCESSED_MONTH + getProcessMonth());
			
			LogStack.process.info("processDateHour :" +getProcessDateHour());	
			LogStack.process.info("processDate :" +getProcessDate());	
			LogStack.process.info("processMonth :" +getProcessMonth());	
			LogStack.process.info("processHour :" +getProcessHour());	
			LogStack.process.info("previousProcessDate :" +getPreviousProcessDate());	
			LogStack.process.info("previous7DaysProcessedDate :" +getPrevious7DaysProcessedDate());	
			LogStack.process.info("rawDataBase :" +getRawDataBase());	
			LogStack.process.info("processedDataBase :" +getProcessedDataBase());		
			
			setValid(true);
		}catch(Exception e){
			setValid(false);
			LogStack.process.error(ExceptionMap.INVALID_DATA_FORMAT_EXCEPTION);			
		}
		return processDataSet(sparkSession);
	}
	
	public String getProcessDate() {
		return processDate;
	}

	public void setProcessDate(String processDate) {
		this.processDate = processDate;
	}

	public String getProcessDateHour() {
		return processDateHour;
	}

	public void setProcessDateHour(String processDateTime) {
		this.processDateHour = processDateTime;
	}

	public String getProcessMonth() {
		return processMonth;
	}

	public void setProcessMonth(String processMonth) {
		this.processMonth = processMonth;
	}

	public String getProcessWeek() {
		return processWeek;
	}

	public void setProcessWeek(String processWeek) {
		this.processWeek = processWeek;
	}

	public String getProcessHour() {
		return processHour;
	}

	public void setProcessHour(String processHour) {
		this.processHour = processHour;
	}

	public ProcessType getProcessType() {
		return processType;
	}

	public void setProcessType(ProcessType processStatus) {
		this.processType = processStatus;
	}

	public int getDays() {
		return days;
	}

	public void setDays(int days) {
		this.days = days;
	}

	public String getPreviousProcessDate() {
		return previousProcessDate;
	}

	public void setPreviousProcessDate(String previousProcessDate) {
		this.previousProcessDate = previousProcessDate;
	}

	public String getPrevious7DaysProcessedDate() {
		return previous7DaysProcessedDate;
	}

	public void setPrevious7DaysProcessedDate(String previous7DaysProcessedDate) {
		this.previous7DaysProcessedDate = previous7DaysProcessedDate;
	}
}
