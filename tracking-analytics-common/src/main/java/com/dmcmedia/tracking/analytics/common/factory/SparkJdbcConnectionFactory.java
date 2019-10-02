package com.dmcmedia.tracking.analytics.common.factory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.common.util.PropertyLoader;

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
* 2018. 10. 18.   cshwang   Initial Release
*
*
************************************************************************************************/
public class SparkJdbcConnectionFactory {

	private final static String JDBC = "jdbc";
	private final static String DBTABLE = "dbtable";
	private final static String JDBC_URL = PropertyLoader.getProperty("tracking.jdbc.url");
	private final static String JDBC_USER = PropertyLoader.getProperty("tracking.jdbc.user");
	private final static String JDBC_PASSWORD = PropertyLoader.getProperty("tracking.jdbc.password");
	private final static String JDBC_DRIVERS = PropertyLoader.getProperty("tracking.jdbc.driver");

	/**
	 * @param url
	 * @param driver
	 */
	public static Dataset<Row> getSelectJdbcDfLoad(SparkSession sparkSession, Map<String, String> options,
			String dbtable) {
		return sparkSession.read().format(JDBC).options(options).option(DBTABLE, dbtable).load();
	}

	/**
	 * @param sparkSession
	 * @param dbtable
	 * @return
	 */
	public static Dataset<Row> getSelectJdbcDfLoad(SparkSession sparkSession, String dbtable) {
		Map<String, String> jdbcOption = new HashMap<String, String>();
		jdbcOption.put("url", JDBC_URL);
		jdbcOption.put("user", JDBC_USER);
		jdbcOption.put("password", JDBC_PASSWORD);
		jdbcOption.put("driver", JDBC_DRIVERS);
		return getSelectJdbcDfLoad(sparkSession, jdbcOption, dbtable);
	}

	public static String getDefaultJdbcUrl() {
		return JDBC_URL;
	}
	
	public static String getJdbcUser() {
		return JDBC_USER;
	}
	
	public static String getJdbcPassword() {
		return JDBC_PASSWORD;
	}

	/**
	 * @return
	 */
	public static Properties getDefaultJdbcOptions() {
		return getJdbcOptions(null);
	}

	/**
	 * @param url
	 * @param driver
	 */
	public static Properties getJdbcOptions(final String[] args) {
		Properties connectionProperties = new Properties();
		if (args == null || args.length == 0) {
			LogStack.batch.debug("Default JDBC Setting!!");
			LogStack.batch.debug("user :" + JDBC_USER);
			LogStack.batch.debug("password :" + JDBC_PASSWORD);

			connectionProperties.put("user", JDBC_USER);
			connectionProperties.put("password", JDBC_PASSWORD);
//			connectionProperties.put("lowerBound", "1000");
//			connectionProperties.put("upperBound", "1000");
//			connectionProperties.put("batchsize", "500");
		} else {
			LogStack.batch.debug("Arguments JDBC Setting!!");
			connectionProperties.put("user", args[1]);
			connectionProperties.put("password", args[2]);
//			connectionProperties.put("lowerBound", "1000");
//			connectionProperties.put("upperBound", "1000");
//			connectionProperties.put("batchsize", "500");
		}
		connectionProperties.put("driver", JDBC_DRIVERS);
		return connectionProperties;
	}
}
