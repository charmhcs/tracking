package com.dmcmedia.tracking.analytics.common.dataset;

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
* 2018. 6. 6.   cshwang   Initial Release
*
*
************************************************************************************************/
public abstract class AbstractTrackingDataSet extends MongodbEntity{
	
	private String trackingTypeCode;
	private String trackingEventCode;
	private String logDateTime;
	private Timestamp isoDate;
	
	public String getTrackingTypeCode() {
		return trackingTypeCode;
	}
	public void setTrackingTypeCode(String trackingTypeCode) {
		this.trackingTypeCode = trackingTypeCode;
	}
	public String getTrackingEventCode() {
		return trackingEventCode;
	}
	public void setTrackingEventCode(String trackingEventCode) {
		this.trackingEventCode = trackingEventCode;
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
}