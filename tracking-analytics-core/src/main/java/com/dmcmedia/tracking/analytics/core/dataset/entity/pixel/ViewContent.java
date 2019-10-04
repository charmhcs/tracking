package com.dmcmedia.tracking.analytics.core.dataset.entity.pixel;
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
* 2017. 7. 5.   cshwang   Initial Release
*
*
************************************************************************************************/
public class ViewContent extends PageView{
	
	/**
	 * 상품 ID값 ,구분 
	 */
	private String content_ids;
	/**
	 * 상품명
	 */
	private String content_name;
	/**
	 * 상품 카테고리명
	 */
	private String content_category;
	
	/**
	 * 컨텐츠 타입
	 */
	private String content_type = "product";
	/**
	 * 상품 가격
	 */
	private String value;
	/**
	 * 화폐단위
	 */
	private String currency = "KRW";
	
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
