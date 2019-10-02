package com.dmcmedia.tracking.analytics.common.util;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


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
*	Log Stack
*
*   날  짜      성 명                     수정  내용
* ----------  ---------  ------------------------------------------------------------------------
* 2015. 11. 5.   cshwang   Initial Release
*
*
************************************************************************************************/
public class LogStack {
	
	public static final Log process = LogFactory.getLog("log.process");	
	public static final Log analysis = LogFactory.getLog("log.analysis");
	public static final Log report = LogFactory.getLog("log.report");
	public static final Log batch = LogFactory.getLog("log.batch");	
	public static final Log factory = LogFactory.getLog("log.factory");	
	public static final Log exception = LogFactory.getLog("log.exception");	
	public static final Log coreSys = LogFactory.getLog("log.coreSys");
	public static final Log junitTest = LogFactory.getLog("log.junitTest");
	public static final Log cacheException = LogFactory.getLog("log.cacheException");
	public static final Log coreSysException = LogFactory.getLog("log.coreSysException");
	public static final Log processMongoDB = LogFactory.getLog("log.process.mongodb");
	
	/**
	 * @param t
	 * @return
	 */
	public static String getStackTrace(Throwable t) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw, true);
		t.printStackTrace(pw);
		pw.flush();
		sw.flush();
		return sw.toString();
	}
}