package com.dmcmedia.tracking.analytics.core.etc;

import org.junit.Test;

import com.dmcmedia.tracking.analytics.common.factory.MongodbFactory;
import com.dmcmedia.tracking.analytics.common.test.CommonJunitTest;

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
* 2018. 10. 18.   cshwang   Initial Release
*
*
************************************************************************************************/
public class MongodbFactoryTest extends CommonJunitTest{
	@Test
	public void createCollectionTest(){
		
		MongodbFactory.dropAndCreateAndShardingCollection("tracking_processed_m09", "test7");
		
//		MongodbFactory.createCollection("tracking_processed_m09", "test3");
//		MongodbFactory.shardCollection("tracking_processed_m09", "test3");
	}
}
