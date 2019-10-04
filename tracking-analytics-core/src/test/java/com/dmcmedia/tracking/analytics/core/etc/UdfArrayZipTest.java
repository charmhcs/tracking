package com.dmcmedia.tracking.analytics.core.etc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.api.java.UDF2;
//import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
//import org.jooq.lambda.Seq;
//import org.jooq.lambda.tuple.Tuple2;
import org.junit.Before;
import org.junit.Test;

import com.dmcmedia.tracking.analytics.common.factory.SparkMongodbConnectionFactory;
import com.dmcmedia.tracking.analytics.common.map.ExceptionMap;
import com.dmcmedia.tracking.analytics.common.map.MongodbMap;
import com.dmcmedia.tracking.analytics.common.test.CommonJunitTest;
import com.dmcmedia.tracking.analytics.common.util.LogStack;
import com.dmcmedia.tracking.analytics.core.dataset.TrackingPixelDataSet;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

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
* 2019-02-07   cshwang  Initial Release
*
*
************************************************************************************************/
public class UdfArrayZipTest extends CommonJunitTest{
	
	@Before
	public void setUp() {
		super.atomicInteger = new AtomicInteger(0);
	}
//	/**
//	 * spark sql add_hours
//	 */
//	public static final UDF2<Seq<String>, Seq<String>,  List<Tuple2<String, String>>> arraysZip = new UDF2<Seq<String>, Seq<String>, List<Tuple2<String, String>>>() {
//		private static final long serialVersionUID = -1L;
//		public  List<Tuple2<String, String>> call(final Seq<String> xs, final Seq<String> xy){
//			try {
//				return xs.zip(xy).toList();
//			}catch(Exception e) {
//				LogStack.report.error(e);
//				return null;
//			}
//		}
//	};
	
	
	@Test
	public void testUdfArrayZip() throws Exception {
		
		SparkSession sparkSession = SparkMongodbConnectionFactory.createSparkSession();	
		
		
		
		List<StructField> fields = new ArrayList<>();
		fields.add(DataTypes.createStructField("test", DataTypes.StringType, false));
		fields.add(DataTypes.createStructField("testt", DataTypes.StringType, false));
//		DataType schema = DataTypes.createStructType(fields);

//		sparkSession.udf().register("arrays_zip", arraysZip, DataTypes.createArrayType(schema));
		
		Dataset<Row> resultDs 	= null;
		MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("database", "tracking_raw_m08")
				.withOption("collection", "2017-08-01" + MongodbMap.COLLECTION_PIXEL_PURCHASED), TrackingPixelDataSet.class)
				.createOrReplaceTempView("T_PIXEL_PURCHASED_RAW");
		
		
		try{
			resultDs = sparkSession.sql("SELECT  A.purchased.pixel_id"
									+ "         ,A.purchased.p_cid"
									+ "         ,C.products.test"
									+ " FROM T_PIXEL_PURCHASED_RAW AS A"
									+ " LATERAL VIEW EXPLODE(arrays_zip(split(purchased.content_ids, ','), split(purchased.products_price, ','))) C AS products"
									+ " WHERE A.purchased.p_cid = 'cb8f7296-cb81-4050-9ace-3b22ec80d794'");
			resultDs.schema();
			resultDs.show();
		}catch(Exception e){
			LogStack.report.error(ExceptionMap.REPORT_EXCEPTION + " "+ this.getClass().getSimpleName());
			LogStack.report.error(e);
		}finally {

		}
	}
}
