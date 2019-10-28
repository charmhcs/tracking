package com.charmhcs.tracking.analytics.core.dataset.entity.pixel;

/** 
*
*   날  짜      성 명                     수정  내용
* ----------  ---------  ------------------------------------------------------------------------
* 2017. 7. 5.   cshwang   Initial Release
*
*
************************************************************************************************/
public class PixelError extends PageView{

	/**
	 * 에러 코드
	 */
	private String error_code;

	public String getError_code() {
		return error_code;
	}

	public void setError_code(String error_code) {
		this.error_code = error_code;
	}

}
