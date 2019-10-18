package com.charmhcs.tracking.analytics.core.dataset;

import java.io.Serializable;

import com.dmcmedia.tracking.analytics.common.dataset.AbstractTrackingDataSet;
import com.charmhcs.tracking.analytics.core.dataset.entity.pixel.AddToCart;
import com.charmhcs.tracking.analytics.core.dataset.entity.pixel.Catalog;
import com.charmhcs.tracking.analytics.core.dataset.entity.pixel.PageView;
import com.charmhcs.tracking.analytics.core.dataset.entity.pixel.ViewContent;
import com.charmhcs.tracking.analytics.core.dataset.entity.pixel.Purchased;

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
* 2017. 5. 19.   cshwang   Initial Release
*
*
************************************************************************************************/
public class TrackingPixelDataSet extends AbstractTrackingDataSet implements Serializable{	

	private static final long serialVersionUID = 5597593081031651529L;
	private Catalog catalog;
	private PageView pageView;
	private ViewContent viewContent;
	private AddToCart addToCart;
	private Purchased purchased;

	public Catalog getCatalog() {
		return catalog;
	}
	public void setCatalog(Catalog catalog) {
		this.catalog = catalog;
	}
	public PageView getPageView() {
		return pageView;
	}
	public void setPageView(PageView pageView) {
		this.pageView = pageView;
	}
	public ViewContent getViewContent() {
		return viewContent;
	}
	public void setViewContent(ViewContent viewContent) {
		this.viewContent = viewContent;
	}
	public AddToCart getAddToCart() {
		return addToCart;
	}
	public void setAddToCart(AddToCart addToCart) {
		this.addToCart = addToCart;
	}
	public Purchased getPurchased() {
		return purchased;
	}
	public void setPurchased(Purchased purchased) {
		this.purchased = purchased;
	}
}
