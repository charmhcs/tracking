package com.dmcmedia.tracking.analytics.core.dataset.entity.web;

import java.io.Serializable;

import com.dmcmedia.tracking.analytics.common.dataset.entity.MongodbEntity;

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
public class Impression extends MongodbEntity implements Serializable{

	private static final long serialVersionUID = 9101658487618181546L;
	private String webId;
	private String mediaId;
	private String channelId;
	private String productsId; 
	private String campaignId;
	private String linkId;
	private String pCid;
	private String uuId;
	private String userAgent;
	private String httpRemoteAddress;
	
	public String getUuId() {
		return uuId;
	}
	public void setUuId(String uuId) {
		this.uuId = uuId;
	}
	public String getWebId() {
		return webId;
	}
	public void setWebId(String webId) {
		this.webId = webId;
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
	public String getpCid() {
		return pCid;
	}
	public void setpCid(String pCid) {
		this.pCid = pCid;
	}
	public String getProductsId() {
		return productsId;
	}
	public void setProductsId(String productsId) {
		this.productsId = productsId;
	}
	public String getUserAgent() {
		return userAgent;
	}
	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}
	public String getHttpRemoteAddress() {
		return httpRemoteAddress;
	}
	public void setHttpRemoteAddress(String httpRemoteAddress) {
		this.httpRemoteAddress = httpRemoteAddress;
	}
}
