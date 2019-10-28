package com.charmhcs.tracking.analytics.core.dataset.entity.pixel;
/** 
*
*   날  짜      성 명                     수정  내용
* ----------  ---------  ------------------------------------------------------------------------
* 2017. 7. 5.   cshwang   Initial Release
*
*
************************************************************************************************/
public class PageView extends Pixel{

	/**
	 * 유입 경로
	 */
	private String referrer;

	/**
	 * Device 정보
	 */
	private String user_agent;

	/**
	 * OS정보
	 */
	private String platform;

	/**
	 * 링크
	 */
	private String href;

	public String getReferrer() {
		return referrer;
	}

	public void setReferrer(String referrer) {
		this.referrer = referrer;
	}

	public String getUser_agent() {
		return user_agent;
	}

	public void setUser_agent(String user_agent) {
		this.user_agent = user_agent;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getHref() {
		return href;
	}

	public void setHref(String href) {
		this.href = href;
	}
}
