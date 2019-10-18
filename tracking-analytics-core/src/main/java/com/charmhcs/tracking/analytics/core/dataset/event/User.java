package com.charmhcs.tracking.analytics.core.dataset.event;

import java.io.Serializable;
import java.math.BigDecimal;

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
* 2018. 5. 15.   cshwang   Initial Release
*
*
************************************************************************************************/
public class User extends NewUser implements Serializable{

	private static final long serialVersionUID = -1884345252278598649L;
		
	private String conversionRawdataTypeCode;
	private String mediaId;
	private String channelId;
	private String productsId;
	private String campaignId;
	private String linkId;
	private int	session;
	private Long bounce;
	private Long duration;
	private BigDecimal viewContentValue;
	private BigDecimal addtocartValue;
	private BigDecimal purchasedValue;

	public Long getBounce() {
		return bounce;
	}
	public void setBounce(Long bounce) {
		this.bounce = bounce;
	}
	public Long getDuration() {
		return duration;
	}
	public void setDuration(Long duration) {
		this.duration = duration;
	}
	public String getConversionRawdataTypeCode() {
		return conversionRawdataTypeCode;
	}
	public void setConversionRawdataTypeCode(String conversionRawdataTypeCode) {
		this.conversionRawdataTypeCode = conversionRawdataTypeCode;
	}
	public String getMediaId() {
		return mediaId;
	}
	public void setMediaId(String mediaId) {
		this.mediaId = mediaId;
	}
	public String getChannelId() {
		return channelId;
	}
	public void setChannelId(String channelId) {
		this.channelId = channelId;
	}
	public String getProductsId() {
		return productsId;
	}
	public void setProductsId(String productsId) {
		this.productsId = productsId;
	}
	public String getCampaignId() {
		return campaignId;
	}
	public void setCampaignId(String campaignId) {
		this.campaignId = campaignId;
	}
	public String getLinkId() {
		return linkId;
	}
	public void setLinkId(String linkId) {
		this.linkId = linkId;
	}
	public int getSession() {
		return session;
	}
	public void setSession(int session) {
		this.session = session;
	}
	public BigDecimal getViewContentValue() {
		return viewContentValue;
	}
	public void setViewContentValue(BigDecimal viewContentValue) {
		this.viewContentValue = viewContentValue;
	}
	public BigDecimal getAddtocartValue() {
		return addtocartValue;
	}
	public void setAddtocartValue(BigDecimal addtocartValue) {
		this.addtocartValue = addtocartValue;
	}
	public BigDecimal getPurchasedValue() {
		return purchasedValue;
	}
	public void setPurchasedValue(BigDecimal purchasedValue) {
		this.purchasedValue = purchasedValue;
	}
}
