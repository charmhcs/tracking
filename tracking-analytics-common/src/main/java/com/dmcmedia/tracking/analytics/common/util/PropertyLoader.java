package com.dmcmedia.tracking.analytics.common.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;


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
* 2016. 7. 21.   cshwang   Initial Release
*
*
************************************************************************************************/
public class PropertyLoader {

	public static Properties properties;
	public static String DEFAILT_PROPRETIES = "tracking.properties";

	public static Properties setInstance(Properties propertiesArgs) {
		properties = propertiesArgs;
		return properties;
	}

	/**
	 * @param key
	 * @return
	 */
	public static String getProperty(String key) {
		Properties properties = new Properties();
        try {
            properties.load(PropertyLoader.class.getClassLoader().getResourceAsStream(DEFAILT_PROPRETIES));
 
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } 
        return properties.getProperty(key);
	}
	
	/**
	 * @param key
	 * @param propertisFileName
	 * @return
	 */
	public static String getProperty(String key, String propertisFileName) {
		Properties properties = new Properties();
        try {
            properties.load(PropertyLoader.class.getClassLoader().getResourceAsStream(propertisFileName));
 
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } 
        return properties.getProperty(key);
	}	
}
