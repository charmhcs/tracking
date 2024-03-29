package com.charmhcs.tracking.analytics.common.test;

import java.util.Calendar;
import java.util.concurrent.atomic.AtomicInteger;

/**
*
*   날  짜      성 명                     수정  내용
* ----------  ---------  ------------------------------------------------------------------------
* 2017. 5. 16.   cshwang   Initial Release
*
*
************************************************************************************************/
public class CommonJunitTest {

	public AtomicInteger atomicInteger;

	/**
	 *
	 * @param time
	 */
	public void sleep(int time) {

		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
		}

	}

	public static String getCalDate(int calDay) {
		String calDate = "";
		Calendar cal = null;
		try {
			cal = Calendar.getInstance();

			cal.add(5, calDay);

			int year = cal.get(1);
			int month = cal.get(2) + 1;
			int dayOfMonth = cal.get(5);

			calDate = String.valueOf(year);

			if (month < 10) {
				calDate = calDate + "0" + month;
			} else {
				calDate = calDate + month;
			}
			if (dayOfMonth < 10) {
				calDate = calDate + "0" + dayOfMonth;
			} else {
				calDate = calDate + dayOfMonth;
			}
		} catch (Exception e) {
			calDate = "";
		}
		return calDate;
	}

}
