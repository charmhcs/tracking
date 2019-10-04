package com.dmcmedia.tracking.analytics.core.dataset.event;

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
* 2018. 5. 15.   cshwang   Initial Release
*
*
************************************************************************************************/
public class Event extends Audience implements Serializable{

	private static final long serialVersionUID = 2651119355074289060L;
	private String viewcontentRank;
	private String addtocartRank;
	private String purchasedRank;
	
	public String getViewcontentRank() {
		return viewcontentRank;
	}
	public void setViewcontentRank(String viewcontentRank) {
		this.viewcontentRank = viewcontentRank;
	}
	public String getAddtocartRank() {
		return addtocartRank;
	}
	public void setAddtocartRank(String addtocartRank) {
		this.addtocartRank = addtocartRank;
	}
	public String getPurchasedRank() {
		return purchasedRank;
	}
	public void setPurchasedRank(String purchasedRank) {
		this.purchasedRank = purchasedRank;
	}
	
}
