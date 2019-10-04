package com.dmcmedia.tracking.analytics.core.dataset.entity.pixel;

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
* 2018. 4. 30.   cshwang   Initial Release
* 
* Web Pixel Catalog RAW Schema
*
*
************************************************************************************************/
public class Catalog extends Pixel implements Serializable{

	private static final long serialVersionUID = -5286021926651083783L;
	private String availability;
	private String description;
	private String image_link;
	private String link;
	private String product_type;
	private String content_ids;
	private String content_name;
	private String content_category;
	private String brand;
	private String content_type;
	private String value;
	private String currency;
	
	public String getAvailability() {
		return availability;
	}
	public void setAvailability(String availability) {
		this.availability = availability;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getImage_link() {
		return image_link;
	}
	public void setImage_link(String image_link) {
		this.image_link = image_link;
	}
	public String getLink() {
		return link;
	}
	public void setLink(String link) {
		this.link = link;
	}
	public String getProduct_type() {
		return product_type;
	}
	public void setProduct_type(String product_type) {
		this.product_type = product_type;
	}
	public String getContent_ids() {
		return content_ids;
	}
	public void setContent_ids(String content_ids) {
		this.content_ids = content_ids;
	}
	public String getContent_name() {
		return content_name;
	}
	public void setContent_name(String content_name) {
		this.content_name = content_name;
	}
	public String getContent_category() {
		return content_category;
	}
	public void setContent_category(String content_category) {
		this.content_category = content_category;
	}
	public String getBrand() {
		return brand;
	}
	public void setBrand(String brand) {
		this.brand = brand;
	}
	public String getContent_type() {
		return content_type;
	}
	public void setContent_type(String content_type) {
		this.content_type = content_type;
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
}
