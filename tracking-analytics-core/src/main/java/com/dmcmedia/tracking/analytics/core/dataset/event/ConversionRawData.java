package com.dmcmedia.tracking.analytics.core.dataset.event;

import java.io.Serializable;

import com.dmcmedia.tracking.analytics.core.dataset.entity.AbstractProcess;

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
* 2018. 9. 15.   cshwang   Initial Release
*
*
************************************************************************************************/
public class ConversionRawData extends AbstractProcess implements Serializable{

	private static final long serialVersionUID = -8227547663882932106L;
	private String ids;
	private String conversionRawdataTypeCode;
	private String deviceTypeCode;
	private String mediaId;
	private String productsId;
	private String channelId;
	private String campaignId;
	private String linkId;
	private String utmMedium;
	private String utmSource;
	private String utmCampaign;
	private String utmTerm;
	private String utmContent;
	private String href;
	private String referrer;
	
	public String getIds() {
		return ids;
	}
	public void setIds(String ids) {
		this.ids = ids;
	}
	public String getConversionRawdataTypeCode() {
		return conversionRawdataTypeCode;
	}
	public void setConversionRawdataTypeCode(String conversionRawdataTypeCode) {
		this.conversionRawdataTypeCode = conversionRawdataTypeCode;
	}
	public String getDeviceTypeCode() {
		return deviceTypeCode;
	}
	public void setDeviceTypeCode(String deviceTypeCode) {
		this.deviceTypeCode = deviceTypeCode;
	}
	public String getMediaId() {
		return mediaId;
	}
	public void setMediaId(String mediaId) {
		this.mediaId = mediaId;
	}
	public String getProductsId() {
		return productsId;
	}
	public void setProductsId(String productsId) {
		this.productsId = productsId;
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
	public String getUtmMedium() {
		return utmMedium;
	}
	public void setUtmMedium(String utmMedium) {
		this.utmMedium = utmMedium;
	}
	public String getUtmSource() {
		return utmSource;
	}
	public void setUtmSource(String utmSource) {
		this.utmSource = utmSource;
	}
	public String getUtmCampaign() {
		return utmCampaign;
	}
	public void setUtmCampaign(String utmCampaign) {
		this.utmCampaign = utmCampaign;
	}
	public String getUtmTerm() {
		return utmTerm;
	}
	public void setUtmTerm(String utmTerm) {
		this.utmTerm = utmTerm;
	}
	public String getUtmContent() {
		return utmContent;
	}
	public void setUtmContent(String utmContent) {
		this.utmContent = utmContent;
	}
	public String getReferrer() {
		return referrer;
	}
	public void setReferrer(String referrer) {
		this.referrer = referrer;
	}
	public String getHref() {
		return href;
	}
	public void setHref(String href) {
		this.href = href;
	}
}
