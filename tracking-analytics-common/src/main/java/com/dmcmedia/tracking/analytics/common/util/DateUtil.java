package com.dmcmedia.tracking.analytics.common.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.joda.time.format.DateTimeFormat;

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
* 2015. 11. 25.   cshwang  Initial Release // 이거.. JAVA 1.8용으로 바꿔주세요.. 
*
*
************************************************************************************************/
public class DateUtil {

	/**
	 * 시간 반환 (날짜 전날 반환)
	 * @param pattern
	 * @return
	 */
	public static String getCurrentDate(String pattern){
		return getIntervalDate(pattern, 0);
	}
	
	/**
	 * 시간 반환 (전일자 반환)
	 * @param pattern
	 * @return
	 */
	public static String getIntervalDate(String pattern, int intervalDay) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, intervalDay);
		return DateTimeFormat.forPattern(pattern).print(cal.getTimeInMillis());
	}
	
	/**
	 * 시간 반환 (시간단위 반환)
	 * @param pattern
	 * @return
	 */
	public static String getIntervalHour(String pattern, int intervalHour) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.HOUR, intervalHour);
		return DateTimeFormat.forPattern(pattern).print(cal.getTimeInMillis());
	}
	
	/**
	 * 시간 반환 (분단위 반환)
	 * @param pattern
	 * @return
	 */
	public static String getIntervalMinute(String pattern, int intervalMin) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MINUTE, intervalMin);
		return DateTimeFormat.forPattern(pattern).print(cal.getTimeInMillis());
	}
	
	/**
	 * 시간 반환 (전일자 반환)
	 * @param pattern
	 * @return
	 */
	public static String getChangeDateFormat(String inputDate, String inputPatten, String outputPattern) {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat (inputPatten);
		try {
			cal.setTime(simpleDateFormat.parse(inputDate));
		} catch (ParseException e) {
			e.printStackTrace();
		}		
		return DateTimeFormat.forPattern(outputPattern).print(cal.getTimeInMillis());
		
	}
	
	/**
	 * 시간 반환 (이전/이후 일자 반환)
	 * @param pattern
	 * @return
	 */
	public static String getChangeDateFormatIntervalDay(String inputDate, String inputPatten, String outputPattern, int intervalDay) {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat (inputPatten);
		try {
			cal.setTime(simpleDateFormat.parse(inputDate));
			cal.add(Calendar.DATE, intervalDay);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return DateTimeFormat.forPattern(outputPattern).print(cal.getTimeInMillis());
	}
	
	
	/**
	 * 분 반환 (이전/이후 일자 반환)
	 * @param pattern
	 * @return
	 */
	public static String getChangeDateFormatIntervalMinute(String inputDate, String inputPatten, String outputPattern, int intervalMinute) {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat (inputPatten);
		try {
			cal.setTime(simpleDateFormat.parse(inputDate));
			cal.add(Calendar.MINUTE, intervalMinute);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return DateTimeFormat.forPattern(outputPattern).print(cal.getTimeInMillis());
	}
	
	/**
	 * 날짜 format(pattern)
	 * 
	 * @param dateTimes
	 * @param checkPattern
	 * @return
	 */
	public static boolean checkDateFormat(String dateTime, String checkPattern){
		SimpleDateFormat dateFormatParser = new SimpleDateFormat(checkPattern);
		try{
			dateFormatParser.parse(dateTime);
		}catch(Exception Ex){
			return false;
		}
		return true;
	}
}
