package com.mylp.sparkproject.spark.product;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.alibaba.fastjson.JSONObject;
import com.mylp.sparkproject.conf.ConfigurationManager;
import com.mylp.sparkproject.constant.Constants;
import com.mylp.sparkproject.dao.ITaskDAO;
import com.mylp.sparkproject.dao.factory.DAOFactory;
import com.mylp.sparkproject.domain.AreaTop3Product;
import com.mylp.sparkproject.domain.Task;
import com.mylp.sparkproject.util.ParamUtils;
import com.mylp.sparkproject.util.SparkUtil;

import scala.Tuple2;

public class AreaTop3ProductSpark {
	
	public static void main(String[] args) {
		
		// 初始化spark
		SparkConf sparkConf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PRODUCT);
		SparkUtil.setMaster(sparkConf);
		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);		
		SparkSession sparkSession = SparkUtil.getSparkSession(sc.sc());
		
		// 注册自定义UDF
		sparkSession.udf().register("concat_long_string_udf", new ConcatLongStringUDF(), DataTypes.StringType);
		sparkSession.udf().register("get_json_object_udf", new GetJsonObjectUDF(), DataTypes.StringType);
		sparkSession.udf().register("group_concat_distinct_udaf", new GroupConcatDistinctUDAF());
		
		// 准备模拟数据
		SparkUtil.mockData(sc, sparkSession);
		
		// 获取命令行传入的taskid，查询对应的任务参数
		long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PRODUCT);
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(taskId);
		
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		
		// 查询用户指定日期范围内的点击行为数据
		JavaPairRDD<Long, Row> clickActionRDD = getCityId2ClickActionRDDByDate(sparkSession, startDate, endDate);
		
		// 获取城市信息
		JavaPairRDD<Long, Row> cityId2CityInfoRDD = getCityId2CityInfoRDD(sparkSession);
		
		// 生成点击产品的基本信息临时表
		generateTempClickProductBasicTable(sparkSession, clickActionRDD, cityId2CityInfoRDD);
		
		// 生成各区域商品点击次数临时表
		generateTempAreaProductClickCountTable(sparkSession);
		
		// 生成包含完整商品信息的各区域商品点击次数临时表
		generateTempAreaFullProductClickCountTable(sparkSession);
		
		// 获得各区域top3商品点击次数RDD
		JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(sparkSession);
		
		// 持久化各区域top3商品点击信息到mysql数据库中
		List<Row> areaTop3Products = areaTop3ProductRDD.collect();
		persistAreaTop3Product(taskId, areaTop3Products);
	}

	private static JavaPairRDD<Long, Row> getCityId2CityInfoRDD(SparkSession sparkSession) {
		String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
		String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
		String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
		Map<String, String> options = new HashMap<String, String>();
		options.put("url", url);
		options.put("dbtable", "city_info");
		options.put("user", user);
		options.put("password", password);
		Dataset<Row> dataset = sparkSession.read().format("jdbc").options(options).load();
		JavaRDD<Row> cityInfoRDD = dataset.javaRDD();
		JavaPairRDD<Long, Row> cityId2CityInfoRDD = cityInfoRDD.mapToPair(
				new PairFunction<Row, Long, Row>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Row> call(Row row) throws Exception {
						return new Tuple2<Long, Row>((long)row.getInt(0), row);
					}
			});
		return cityId2CityInfoRDD;
	}

	/**
	 * 查询用户指定日期范围内的点击行为数据
	 * @param sparkSession
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	private static JavaPairRDD<Long, Row> getCityId2ClickActionRDDByDate(SparkSession sparkSession, String startDate, String endDate) {
		String sql = "select city_id, click_product_id product_id from user_visit_action "
				+ "where click_product_id is not NULL "
//				+ "and click_product_id != 'NULL' "
//				+ "and click_product_id != 'null' "
				+ "and action_time >= '" + startDate + "' "
				+ "and action_time <= '" + endDate + "'";
		
		Dataset<Row> dataset = sparkSession.sql(sql);
		JavaRDD<Row> clickActionRDD = dataset.javaRDD();
		return clickActionRDD.mapToPair(
				new PairFunction<Row, Long, Row>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Row> call(Row row) throws Exception {
						return new Tuple2<Long, Row>(row.getLong(0), row);
					}
				});
	}
	
	private static void generateTempClickProductBasicTable(SparkSession sparkSession,
			JavaPairRDD<Long, Row> cityId2ClickProductRDD,
			JavaPairRDD<Long, Row> cityId2CityInfoRDD) {
		JavaPairRDD<Long, Tuple2<Row, Row>> joinRDD = cityId2ClickProductRDD.join(cityId2CityInfoRDD);
		JavaRDD<Row> mappedRDD = joinRDD.map(
				new Function<Tuple2<Long,Tuple2<Row,Row>>, Row>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Row call(Tuple2<Long, Tuple2<Row, Row>> tuple) throws Exception {
						long cityId = tuple._1;
						Row clickProduct = tuple._2._1;
						Row cityInfo = tuple._2._2;
						return RowFactory.create(cityId, cityInfo.getString(1), cityInfo.getString(2), clickProduct.getLong(1));
					}
				});
		
		StructType schema = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("city_id", DataTypes.LongType, true),
				DataTypes.createStructField("city_name", DataTypes.StringType, true),
				DataTypes.createStructField("area", DataTypes.StringType, true),
				DataTypes.createStructField("product_id", DataTypes.LongType, true)));
		Dataset<Row> dataset = sparkSession.createDataFrame(mappedRDD, schema);
		dataset.createOrReplaceTempView("tmp_click_product_basic");
	}

	/**
	 * 生成各区域商品点击次数临时表
	 * @param sparkSession
	 */
	private static void generateTempAreaProductClickCountTable(SparkSession sparkSession) {
		String sql = "select area, product_id, count(*) click_count, "
				+ "group_concat_distinct_udaf(concat_long_string_udf(city_id,city_name,':')) city_infos "
				+ "from tmp_click_product_basic "
				+ "group by area, product_id";
		
		Dataset<Row> dataset = sparkSession.sql(sql);
		dataset.createOrReplaceTempView("tmp_area_product_click_count");
	}

	/**
	 * 生成包含完整商品信息的各区域商品点击次数临时表
	 * @param sparkSession
	 */
	private static void generateTempAreaFullProductClickCountTable(SparkSession sparkSession) {
		String sql = "select tapcc.area, tapcc.product_id, tapcc.click_count, tapcc.city_infos, "
				+ "pi.product_name, "
				+ "if(get_json_object_udf(pi.extend_info, 'product_status')=0, '自营商品' , '第三方商品') product_status "
				+ "from tmp_area_product_click_count tapcc "
				+ "join product_info pi on tapcc.product_id = pi.product_id";
		Dataset<Row> dataset = sparkSession.sql(sql);
		dataset.createOrReplaceTempView("tmp_area_fullprod_click_count");
	}

	/**
	 * 获取各区域top3商品点击次数
	 * @param sparkSession
	 * @return
	 */
	private static JavaRDD<Row> getAreaTop3ProductRDD(SparkSession sparkSession) {
		String sql = "select area, "
				+ "CASE "
					+ "WHEN area='华北' OR area='华东' THEN 'A级' "
					+ "WHEN area='华南' OR area='华中' THEN 'B级' "
					+ "WHEN area='西北' OR area='西南' THEN 'C级' "
					+ "ELSE 'D级' "
				+ "END area_level, "
				+ "product_id, click_count, city_infos, product_name, product_status "
				+ "from (select area, product_id, click_count, city_infos, product_name, product_status, "
						+ "ROW_NUMBER() OVER (PARTITION BY area ORDER BY click_count DESC) rank "
						+ "from tmp_area_fullprod_click_count "
						+ ") t "
				+ "where rank<=3";
		Dataset<Row> dataset = sparkSession.sql(sql);
		return dataset.javaRDD();
	}
	
	private static void persistAreaTop3Product(final long taskId, List<Row> rows) {
		System.out.println("rows count " + rows.size());
		
		List<AreaTop3Product> areaTop3Products = new ArrayList<AreaTop3Product>();
		for (Row row : rows) {
			AreaTop3Product areaTop3Product = new AreaTop3Product();
			areaTop3Product.setTaskId(taskId);
			areaTop3Product.setArea(row.getString(0));
			areaTop3Product.setAreaLevel(row.getString(1));
			areaTop3Product.setProductId(row.getLong(2));
			areaTop3Product.setCityNames(row.getString(4));
			areaTop3Product.setClickCount(row.getLong(3));
			areaTop3Product.setProductName(row.getString(5));
			areaTop3Product.setProductStatus(row.getString(6));
			
			areaTop3Products.add(areaTop3Product);
		}
		
		DAOFactory.getaAreaTop3ProductDAO().insertBatch(areaTop3Products);
	}
}
