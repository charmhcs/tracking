package com.charmhcs.tracking.analytics.common;

import com.charmhcs.tracking.analytics.common.map.CodeMap;
import com.charmhcs.tracking.analytics.common.map.ExceptionMap;
import com.charmhcs.tracking.analytics.common.util.DateUtil;
import com.charmhcs.tracking.analytics.common.util.LogStack;
import org.apache.spark.sql.SparkSession;


/**    
*
*   날  짜      성 명                     수정  내용
* ----------  ---------  ------------------------------------------------------------------------
* 2018. 4. 6.   cshwang   Initial Release
*
* 	Raw Data 혹은 Processed Data를 이용하여, 데이터를 분석함
*
* 		- Mongodb Raw Data  	-> RDBMS analysis Data OR Mongodb Analysis Data
* 		- Mongodb Processed Data  -> RDBMS analysis Data
*
************************************************************************************************/
public abstract class AbstractDataAnalysis extends CommonDataAnalytics implements DataAnalysis {

	public enum analysisType {hourly, daily, Month};
	private String analysisDate;
	private String analysisHour;
	private String analysisMonth;

	/**
	 * 기본 생성자 (시간별)
	 */
	public AbstractDataAnalysis(){
		try{
			this.setAnalysisDate(DateUtil.getIntervalHour(CodeMap.DATE_YMD_PATTERN_FOR_ANYS, -1));
			this.setAnalysisMonth(DateUtil.getIntervalHour(CodeMap.DATE_MONTH_PATTERN, -1));
			this.setValid(true);
		}catch(Exception e){
			setValid(false);
			LogStack.analysis.error(ExceptionMap.INVALID_DATA_FORMAT_EXCEPTION);
		}
	}

	/* (non-Javadoc)
	 * @see com.dmcmedia.tracking.analytics.common.DataAnalysis#analyzeDataSet(org.apache.spark.sql.SparkSession, java.lang.String)
	 */
	public boolean analyzeDataSet(SparkSession sparkSession,  String analysisDateTime) {
		try{
			this.setAnalysisDate(DateUtil.getChangeDateFormat(analysisDateTime, CodeMap.DATE_YMD_PATTERN_FOR_ANYS, CodeMap.DATE_YMD_PATTERN_FOR_ANYS));
			this.setAnalysisMonth(DateUtil.getChangeDateFormat(analysisDateTime, CodeMap.DATE_YMD_PATTERN_FOR_ANYS, CodeMap.DATE_MONTH_PATTERN));
		}catch(Exception e){
			setValid(false);
			LogStack.analysis.error(ExceptionMap.INVALID_DATA_FORMAT_EXCEPTION);
		}
		return analyzeDataSet(sparkSession);
	}

	public String getAnalysisDate() {
		return analysisDate;
	}

	public void setAnalysisDate(String analysisDate) {
		this.analysisDate = analysisDate;
	}

	public String getAnalysisHour() {
		return analysisHour;
	}

	public void setAnalysisHour(String analysisHour) {
		this.analysisHour = analysisHour;
	}

	public String getAnalysisMonth() {
		return analysisMonth;
	}

	public void setAnalysisMonth(String analysisMonth) {
		this.analysisMonth = analysisMonth;
	}

}
