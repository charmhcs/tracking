package com.charmhcs.tracking.analytics.core.dataset;

import java.io.Serializable;

/**  
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
