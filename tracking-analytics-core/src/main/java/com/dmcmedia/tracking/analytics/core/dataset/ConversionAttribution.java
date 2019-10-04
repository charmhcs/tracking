package com.dmcmedia.tracking.analytics.core.dataset;

import java.io.Serializable;

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
* 2018. 9. 29.   cshwang   Initial Release
*
*
************************************************************************************************/
public class ConversionAttribution implements Serializable {

	private static final long serialVersionUID = 2534780603525703382L;
	private String attribution;
	private String attributionCode;
	public String getAttribution() {
		return attribution;
	}
	public void setAttribution(String attribution) {
		this.attribution = attribution;
	}
	public String getAttributionCode() {
		return attributionCode;
	}
	public void setAttributionCode(String attributionCode) {
		this.attributionCode = attributionCode;
	}
}
