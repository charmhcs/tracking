package com.charmhcs.tracking.analytics.common.util;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

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
* 2017. 1. 16.   cshwang   Initial Release
*	단방향 암호화
*
************************************************************************************************/
public class StringEncryptUtil {
	private final static String SHA_512 = "SHA-512";
	private final static String CHARSET = "UTF-8";

	public static String encrypt(String plainString, String salt) {
		String encryptString = null;
		try {
			MessageDigest md = MessageDigest.getInstance(SHA_512);
			md.update(salt.getBytes(CHARSET));
			byte[] bytes = md.digest(plainString.getBytes(CHARSET));
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < bytes.length; i++) {
				sb.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			encryptString = sb.toString();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return encryptString;
	}
}
