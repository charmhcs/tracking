package com.dmcmedia.tracking.analytics.core.dataset.entity;

import java.sql.Timestamp;

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
* 2018-12-30   cshwang  Initial Release
*
*
************************************************************************************************/
public abstract class AbstractProcess extends MongodbEntity {
	
	private String processDate;
	private String ProcessHour;
	private String processHalfHour;
	private String pixelId;
	private String catalogId;
	private String pCid;
	private Timestamp isoDate;
	private String logDateTime;
	
	public String getProcessDate() {
		return processDate;
	}
	public String getPixelId() {
		return pixelId;
	}
	public void setPixelId(String pixelId) {
		this.pixelId = pixelId;
	}
	public void setProcessDate(String processDate) {
		this.processDate = processDate;
	}	
	public String getCatalogId() {
		return catalogId;
	}
	public void setCatalogId(String catalogId) {
		this.catalogId = catalogId;
	}
	public String getpCid() {
		return pCid;
	}
	public void setpCid(String pCid) {
		this.pCid = pCid;
	}
	public String getLogDateTime() {
		return logDateTime;
	}
	public void setLogDateTime(String logDateTime) {
		this.logDateTime = logDateTime;
	}
	public Timestamp getIsoDate() {
		return isoDate;
	}
	public void setIsoDate(Timestamp isoDate) {
		this.isoDate = isoDate;
	}
	public String getProcessHour() {
		return ProcessHour;
	}
	public void setProcessHour(String processHour) {
		ProcessHour = processHour;
	}
	public String getProcessHalfHour() {
		return processHalfHour;
	}
	public void setProcessHalfHour(String processHalfHour) {
		this.processHalfHour = processHalfHour;
	}
}