package com.charmhcs.tracking.analytics.core.dataset.entity.pixel;

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
* 2018-12-22   cshwang  Initial Release
*
*
************************************************************************************************/
public class Purchased extends ViewContent{
	private String products_price;
	private String value;
	private String currency;
	
	public String getProducts_price() {
		return products_price;
	}
	public void setProducts_price(String products_price) {
		this.products_price = products_price;
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
