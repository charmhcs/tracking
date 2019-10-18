package com.charmhcs.tracking.analytics.common.util;

import java.io.File;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
* 2015. 11. 25.   dmcmedia   Initial Release
*
*
************************************************************************************************/
public class StringUtil {
	
	public static final String START_TIME = "000000000";
	public static final String END_TIME = "235959999";
    
    public static String makeUUID(){
        return UUID.randomUUID().toString();
    }
    
    public static String getCurTime(){
        DateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        return format.format(new Date());       
    }
    
    public static String getCurDate(){
    	DateFormat format = new SimpleDateFormat("yyyyMMdd");
        return format.format(new Date());       
    }
    
    public static String getNextDay(String strDate, int nDay){
    	String ret = "";
    	try{
    		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
    		Date date = format.parse(strDate);
    		Calendar cal = Calendar.getInstance();
    		cal.setTime(date);
    		cal.add(Calendar.DATE, nDay);
    		Date retDate = cal.getTime();
    		ret = format.format(retDate);
    	}catch(Exception e){e.printStackTrace();}
    	return ret;
    }
    
    public static String getCurDayOfWeek(){
        String[] weeks = {"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"};
        Calendar calendar = Calendar.getInstance();
        return weeks[calendar.get(Calendar.DAY_OF_WEEK) -1];        
    }
    
    public static int parseInt(String str){
    	try{
    		return Integer.parseInt(str);
    	}catch(Exception e){
    		return 0;
    	}
    }
    
    public static double parseDouble(String str){
    	try{
    		return Double.parseDouble(str);
    	}catch(Exception e){
    		return 0;
    	}
    }

    public static String getDayOfWeek(String strDate){
    	String[] weeks = {"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"};
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    	Date date = new Date();
    	try {
    		date = sdf.parse(strDate);
    	} catch (ParseException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}		
    	Calendar cal = Calendar.getInstance();
    	cal.setTime(date);
    	return weeks[cal.get(Calendar.DAY_OF_WEEK) -1];
    }
    
    public static int getCurWeekOfYear(){    	
		Date date = new Date();			
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		int week = cal.get(Calendar.WEEK_OF_YEAR);
		return week;    	
    }
    
    public static int getWeekOfYear(String strDate){    	
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date date = new Date();
		try {
			date = sdf.parse(strDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		int week = cal.get(Calendar.WEEK_OF_YEAR);
		return week;
    }
    
    public static String getAddTime(String strDate){
    	DateFormat format = new SimpleDateFormat("HHmmssSSS");
    	return strDate + format.format(new Date());
    }
    
    public static String getPathWithoutName(File file){
    	if(!file.exists())
    		return "";
    	return file.getAbsolutePath().
	    	     substring(0,file.getAbsolutePath().lastIndexOf(File.separator));
    }
    
    /**
     * @param str
     * @return
     */
    public static String StringReplace(String str){       
        String match = "[^\uAC00-\uD7A3xfe0-9a-zA-Z\\s]";
        str =str.replaceAll(match, " ");
        return str;
     }
     /**
     * @param email
     * @return
     */
    public static boolean isEmailPattern(String email){
      Pattern pattern=Pattern.compile("\\w+[@]\\w+\\.\\w+");
      Matcher match=pattern.matcher(email);
      return match.find();
     }

     public static String continueSpaceRemove(String str){
      String match2 = "\\s{2,}";
      str = str.replaceAll(match2, " ");
      return str;
     }
}