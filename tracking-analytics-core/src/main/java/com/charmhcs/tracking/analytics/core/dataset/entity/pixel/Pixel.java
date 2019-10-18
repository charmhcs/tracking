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
* 2017. 7. 5.   cshwang   Initial Release
* F-1 web Pixel
*
************************************************************************************************/
public abstract class Pixel {
	/**
	 * Facebook 이벤트 코드 / Reference 참고
	 * https://developers.facebook.com/docs/facebook-pixel/api-reference
	 */
	private String p_ec;
	/**
	 * pixel client id / 유저식별자
	 */
	private String p_cid;
	/**
	 * Facebook Catalog ID
	 */
	private String catalog_id;
	/**
	 * Facebook Pixel ID
	 */
	private String pixel_id;
	
	public String getP_ec() {
		return p_ec;
	}
	public void setP_ec(String p_ec) {
		this.p_ec = p_ec;
	}
	public String getP_cid() {
		return p_cid;
	}
	public void setP_cid(String p_cid) {
		this.p_cid = p_cid;
	}
	public String getCatalog_id() {
		return catalog_id;
	}
	public void setCatalog_id(String catalog_id) {
		this.catalog_id = catalog_id;
	}
	public String getPixel_id() {
		return pixel_id;
	}
	public void setPixel_id(String pixel_id) {
		this.pixel_id = pixel_id;
	}
}
