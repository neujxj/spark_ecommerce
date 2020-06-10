package com.mylp.sparkproject.util;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.alibaba.fastjson.JSONObject;
import com.mylp.sparkproject.conf.ConfigurationManager;
import com.mylp.sparkproject.constant.Constants;
import com.mylp.sparkproject.spark.mock.MockData;

public class SparkUtil {
	
	/**
	 * 设置master
	 * @param sparkConf
	 */
	public static void setMaster(SparkConf sparkConf) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if (local) {
			sparkConf.setMaster("local");
		}
	}
	
	/**
	 * 生成模拟数据(只有本地模式，才会生成模拟数据)
	 * @param sc
	 * @param sparkSession
	 */
	public static void mockData(JavaSparkContext sc, SparkSession sparkSession) {
		
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if (local) {
			MockData.mock(sc, sparkSession);
		}
		
	}
	
	/**
	 * 获取SQLContext
	 * 如果是在本地测试环境的话，那么就生成SQLContext对象
	 * 如果是在生产环境运行的话，那么就生成HiveContext对象
	 * @param sparkConf
	 * @return
	 */
	public static SparkSession getSparkSession(SparkContext sparkContext) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

		SparkSession sparkSession = null;
		
		if (local) {
			sparkSession = SparkSession.builder()
				.sparkContext(sparkContext)
				.getOrCreate();
		} else {
			sparkSession = SparkSession.builder()
					.sparkContext(sparkContext)
					.enableHiveSupport()
					.getOrCreate();

		}
		return sparkSession;
	}
	
	public static JavaRDD<Row> getActionRDDByDateRange(
			SparkSession sparkSession, JSONObject taskParam) {
		
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		String sql = "select * from user_visit_action where update_date>='"
				+ startDate + "' "
				+ "and update_date<='" + endDate + "'";
		Dataset<Row> df = sparkSession.sql(sql);
		
		/**
		 * 这里很可能发生上面说过的问题
		 * 比如说，spark sql默认就给第一个stage设置了20个task，但是根据你的数据量以及算法的复杂度
		 * 实际上，你需要1000个task去并行执行
		 * 
		 * 所以说，在这里，就可以对spark sql刚刚查询出来的RDD执行repartition重分区操作
		 * 
		 */
//		return df.javaRDD().repartition(1000);  // 这里是本地模式
		
		return df.javaRDD();
	}
}
