package com.charmhcs.tracking.analytics.core.dataset.feed;

import java.io.Serializable;

import com.charmhcs.tracking.analytics.core.dataset.entity.AbstractProcess;

/**
*
*   날  짜      성 명                     수정  내용
* ----------  ---------  ------------------------------------------------------------------------
* 2017. 7. 4.   cshwang   Initial Release
*
*
************************************************************************************************/
public class Purchased extends AbstractProcess implements Serializable{

	private static final long serialVersionUID = 4688902470595248170L;
	private String contentIds;
	private String contentType;
	private String value;
	private String currency;
	private String userAgent;

	public String getContentIds() {
		return contentIds;
	}
	public void setContentIds(String contentIds) {
		this.contentIds = contentIds;
	}
	public String getContentType() {
		return contentType;
	}
	public void setContentType(String contentType) {
		this.contentType = contentType;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getCurrency() {
		return currency;
	}
	public void setCurrency(String currency) {
		this.currency = currency;
	}
	public String getUserAgent() {
		return userAgent;
	}
	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}
}
