package com.charmhcs.tracking.analytics.core.dataset.entity.web;

import java.io.Serializable;
import java.sql.Timestamp;

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
* 2018-11-26  cshwang   Initial Release
*
*
************************************************************************************************/
public class Click extends Impression implements Serializable{

	private static final long serialVersionUID = 4327221557770184103L;
	
	private String adset;
	private String ad;
	private String period;
	private String pixelId;
	private String catalogId;
	private String landingUrl;
	private String referrer;
	private String isRetargeting;
	private String logDateTime;
	private Timestamp isoDateTime;
	private String regionSid;

	public String getAdset() {
		return adset;
	}
	public void setAdset(String adset) {
		this.adset = adset;
	}
	public String getAd() {
		return ad;
	}
	public void setAd(String ad) {
		this.ad = ad;
	}
	public String getPeriod() {
		return period;
	}
	public void setPeriod(String period) {
		this.period = period;
	}
	public String getCatalogId() {
		return catalogId;
	}
	public void setCatalogId(String catalogId) {
		this.catalogId = catalogId;
	}
	public String getLandingUrl() {
		return landingUrl;
	}
	public void setLandingUrl(String landingUrl) {
		this.landingUrl = landingUrl;
	}
	public String getReferrer() {
		return referrer;
	}
	public void setReferrer(String referrer) {
		this.referrer = referrer;
	}
	public String getIsRetargeting() {
		return isRetargeting;
	}
	public void setIsRetargeting(String isRetargeting) {
		this.isRetargeting = isRetargeting;
	}
	public String getLogDateTime() {
		return logDateTime;
	}
	public void setLogDateTime(String logDateTime) {
		this.logDateTime = logDateTime;
	}
	public Timestamp getIsoDateTime() {
		return isoDateTime;
	}
	public void setIsoDateTime(Timestamp isoDateTime) {
		this.isoDateTime = isoDateTime;
	}
	public String getRegionSid() {
		return regionSid;
	}
	public void setRegionSid(String regionSid) {
		this.regionSid = regionSid;
	}
	public String getPixelId() {
		return pixelId;
	}
	public void setPixelId(String pixelId) {
		this.pixelId = pixelId;
	}
}
