package com.charmhcs.tracking.analytics.core.dataset.event;

import java.io.Serializable;

import com.charmhcs.tracking.analytics.core.dataset.entity.AbstractProcess;

/**
*
*   날  짜      성 명                     수정  내용
* ----------  ---------  ------------------------------------------------------------------------
* 2018. 5. 15.   cshwang   Initial Release
*
*
************************************************************************************************/
public class NewUser extends AbstractProcess implements Serializable{

	private static final long serialVersionUID = -5886193540882098511L;

	private String deviceTypeCode;
	private Long pageview;
	private Long viewcontent;
	private Long addtocart;
	private Long purchased;
	private String updateDate;

	public Long getPageview() {
		return pageview;
	}
	public void setPageview(Long pageview) {
		this.pageview = pageview;
	}
	public Long getViewcontent() {
		return viewcontent;
	}
	public void setViewcontent(Long viewcontent) {
		this.viewcontent = viewcontent;
	}
	public Long getAddtocart() {
		return addtocart;
	}
	public void setAddtocart(Long addtocart) {
		this.addtocart = addtocart;
	}
	public Long getPurchased() {
		return purchased;
	}
	public void setPurchased(Long purchased) {
		this.purchased = purchased;
	}
	public String getUpdateDate() {
		return updateDate;
	}
	public void setUpdateDate(String updateDate) {
		this.updateDate = updateDate;
	}
	public String getDeviceTypeCode() {
		return deviceTypeCode;
	}
	public void setDeviceTypeCode(String deviceTypeCode) {
		this.deviceTypeCode = deviceTypeCode;
	}
}
