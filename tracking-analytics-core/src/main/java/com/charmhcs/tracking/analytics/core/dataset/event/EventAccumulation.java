package com.charmhcs.tracking.analytics.core.dataset.event;

import java.io.Serializable;
import java.math.BigDecimal;

import com.charmhcs.tracking.analytics.common.dataset.entity.MongodbEntity;

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
* 2019-02-13   cshwang  Initial Release
*
*
************************************************************************************************/
public class EventAccumulation extends MongodbEntity implements Serializable{

	private static final long serialVersionUID = -3737560991501270481L;

	private String px;
	private String ct;
	private String pc;
	private String pv;
	private Long vc;
	private Long ac;
	private Long pr;
	private BigDecimal ps;
	private String pd;
	private String ud;
	
	public String getPx() {
		return px;
	}
	public void setPx(String px) {
		this.px = px;
	}
	public String getCt() {
		return ct;
	}
	public void setCt(String ct) {
		this.ct = ct;
	}
	public String getPc() {
		return pc;
	}
	public void setPc(String pc) {
		this.pc = pc;
	}
	public String getPv() {
		return pv;
	}
	public void setPv(String pv) {
		this.pv = pv;
	}
	public Long getVc() {
		return vc;
	}
	public void setVc(Long vc) {
		this.vc = vc;
	}
	public Long getAc() {
		return ac;
	}
	public void setAc(Long ac) {
		this.ac = ac;
	}
	public Long getPr() {
		return pr;
	}
	public void setPr(Long pr) {
		this.pr = pr;
	}
	public BigDecimal getPs() {
		return ps;
	}
	public void setPs(BigDecimal ps) {
		this.ps = ps;
	}	
	public String getPd() {
		return pd;
	}
	public void setPd(String pd) {
		this.pd = pd;
	}
	public String getUd() {
		return ud;
	}
	public void setUd(String ud) {
		this.ud = ud;
	}	
}
