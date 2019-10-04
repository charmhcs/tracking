package com.dmcmedia.tracking.analytics.core.dataset.feed;

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
* 2018. 4. 30.   cshwang   Initial Release
*
*
************************************************************************************************/
public class ProductsFeed extends AbstractProcess implements Serializable{

	private static final long serialVersionUID = 2227014073049050434L;
	private String hour;	
	private String catalogId;
	private String contentIds;
	private String contentCategory;
	private String contentType;
	private String contentName;	
	private String productType;
	private String brand;
	private String link;
	private String imageLink;
	private String clickLink;
	private String description;
	private String availability;
	private String currency;
	private String value;

	public String getHour() {
		return hour;
	}
	public void setHour(String hour) {
		this.hour = hour;
	}
	public String getCatalogId() {
		return catalogId;
	}
	public void setCatalogId(String catalogId) {
		this.catalogId = catalogId;
	}
	public String getContentIds() {
		return contentIds;
	}
	public void setContentIds(String contentIds) {
		this.contentIds = contentIds;
	}
	public String getContentCategory() {
		return contentCategory;
	}
	public void setContentCategory(String contentCategory) {
		this.contentCategory = contentCategory;
	}
	public String getContentType() {
		return contentType;
	}
	public void setContentType(String contentType) {
		this.contentType = contentType;
	}
	public String getContentName() {
		return contentName;
	}
	public void setContentName(String contentName) {
		this.contentName = contentName;
	}
	public String getProductType() {
		return productType;
	}
	public void setProductType(String productType) {
		this.productType = productType;
	}
	public String getBrand() {
		return brand;
	}
	public void setBrand(String brand) {
		this.brand = brand;
	}
	public String getLink() {
		return link;
	}
	public void setLink(String link) {
		this.link = link;
	}
	public String getImageLink() {
		return imageLink;
	}
	public void setImageLink(String imageLink) {
		this.imageLink = imageLink;
	}
	public String getClickLink() {
		return clickLink;
	}
	public void setClickLink(String clickLink) {
		this.clickLink = clickLink;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getAvailability() {
		return availability;
	}
	public void setAvailability(String availability) {
		this.availability = availability;
	}
	public String getCurrency() {
		return currency;
	}
	public void setCurrency(String currency) {
		this.currency = currency;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
}