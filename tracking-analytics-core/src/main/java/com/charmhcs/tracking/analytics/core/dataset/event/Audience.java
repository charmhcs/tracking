package com.charmhcs.tracking.analytics.core.dataset.event;

import java.io.Serializable;

import com.charmhcs.tracking.analytics.core.dataset.entity.AbstractProcess;

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
public class Audience  extends AbstractProcess implements Serializable{

	private static final long serialVersionUID = 8757987131598983931L;
	private String contentCategory;
	private String contentIds;
	private String contentName;
	private String price;
	private String viewcontent;
	private String addtocart;
	private String purchased;
	
	public String getContentCategory() {
		return contentCategory;
	}
	public void setContentCategory(String contentCategory) {
		this.contentCategory = contentCategory;
	}
	public String getContentIds() {
		return contentIds;
	}
	public void setContentIds(String contentIds) {
		this.contentIds = contentIds;
	}
	public String getContentName() {
		return contentName;
	}
	public void setContentName(String contentName) {
		this.contentName = contentName;
	}
	public String getPrice() {
		return price;
	}
	public void setPrice(String price) {
		this.price = price;
	}
	public String getViewcontent() {
		return viewcontent;
	}
	public void setViewcontent(String viewcontent) {
		this.viewcontent = viewcontent;
	}
	public String getAddtocart() {
		return addtocart;
	}
	public void setAddtocart(String addtocart) {
		this.addtocart = addtocart;
	}
	public String getPurchased() {
		return purchased;
	}
	public void setPurchased(String purchased) {
		this.purchased = purchased;
	}
}