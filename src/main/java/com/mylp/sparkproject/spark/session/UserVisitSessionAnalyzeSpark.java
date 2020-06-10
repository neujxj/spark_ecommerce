package com.mylp.sparkproject.spark.session;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.AccumulatorV2;

import com.alibaba.fastjson.JSONObject;
import com.mylp.sparkproject.constant.Constants;
import com.mylp.sparkproject.dao.ISessionRandomExtractDAO;
import com.mylp.sparkproject.dao.ITaskDAO;
import com.mylp.sparkproject.dao.ITop10CategoryDAO;
import com.mylp.sparkproject.dao.ITop10CategorySessionDAO;
import com.mylp.sparkproject.dao.factory.DAOFactory;
import com.mylp.sparkproject.domain.SessionAggrStat;
import com.mylp.sparkproject.domain.SessionDetail;
import com.mylp.sparkproject.domain.SessionRandomExtract;
import com.mylp.sparkproject.domain.Task;
import com.mylp.sparkproject.domain.Top10Category;
import com.mylp.sparkproject.domain.Top10CategorySession;
import com.mylp.sparkproject.util.DateUtils;
import com.mylp.sparkproject.util.NumberUtils;
import com.mylp.sparkproject.util.ParamUtils;
import com.mylp.sparkproject.util.SparkUtil;
import com.mylp.sparkproject.util.StringUtils;
import com.mylp.sparkproject.util.ValidUtils;

import scala.Tuple2;

/**
 * 用户访问session分析spark作业
 * 
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * 1、时间范围
 * 2、性别
 * 3、年龄
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过指定的品类，那么session就符合条件
 * 
 * 我们的spark作业如何接收用户创建的任务呢？
 * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入mysql的task表中，任务参数以JSON格式封装在task_param
 * 字段中
 * 
 * 接着J2EE平台会执行我们的spark-submit shell脚本，并将taskID作为参数给spark-submit shell脚本
 * spark-submit shell脚本在执行时，是可以接收参数的，并且会将接收的参数，传递给spark作业的main函数，
 * 参数就封装在main函数的args数组中
 * 
 * 这是spark本身提供的特性
 * 
 * @author Administrator
 *
 */
public class UserVisitSessionAnalyzeSpark {
	
	public static void main(String[] args) {
		
		// 构建spark上下文
		SparkConf sparkConf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_SESSION)
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // 设置Kryo序列化
				.registerKryoClasses(new Class[] {CategorySortKey.class})
				.set("spark.locality.wait", "10")	// 设置本地化数据等待时长
				.set("spark.storage.memoryFraction", "0.5") // 设置spark的cache和persist操作的内存占比，默认是0.6
				.set("spark.shuffle.consolidateFiles", "true") // 设置shuffle文件合并
				.set("spark.shuffle.file.buffer", "64")  // 设置shuffle map端文件缓存大小，默认32kb
				.set("spark.shuffle.memoryFraction", "0.3") // 设置shuffle reduce端聚合内存占比，默认0.2
				.set("spark.reducer.maxSizeInFlight", "24"); // 设置reduce端缓冲内存大小
		SparkUtil.setMaster(sparkConf);
		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
//		sc.checkpointFile("hdfs://");
		
		SparkSession sparkSession = SparkUtil.getSparkSession(sc.sc());
		
		// 创建模拟数据
		SparkUtil.mockData(sc, sparkSession);
		
		// 创建需要的DAO组件
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		
		// 那么就首先的查询出来指定的任务
		long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);
		Task task = taskDAO.findById(taskId);
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		
		// 如果要进行session粒度的数据聚合
		// 首先要从user_visit_action表中，查询出指定日期范围里的行为数据
		// 如果要根据用户在创建任务时指定的参数，来进行数据过滤和筛选
		JavaRDD<Row> actionRDD = SparkUtil.getActionRDDByDateRange(sparkSession, taskParam).cache();
		
		JavaPairRDD<String, Row> sessionid2ActionRDD = getSessionid2ActionRDD(actionRDD);
		sessionid2ActionRDD = sessionid2ActionRDD.persist(StorageLevel.MEMORY_ONLY());
		
		// 首先，可以将行为数据，按照session_id进行groupByKey分组
		// 此时的数据的粒度就是session粒度了，然后呢，可以将session粒度的数据
		// 与用户信息数据进行join
		// 然后就可以获取到session粒度的数据，同时呢，数据里面还包含了session对象的user信息
		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = 
				aggregateBySession(sparkSession, sessionid2ActionRDD);
		
		// 接着，就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
		// 相当于我们自己编写的算子，是要访问外面的任务参数对象的
		// 所以，大家记得我们之前说的，匿名内部类访问外部对象，是要给外部对象使用final修饰的
		
		// 同时进行过滤和统计
		AccumulatorV2<String, String> sessionAggrStatAccumulator = new SessionAggrStatAccumulator();
		sc.sc().register(sessionAggrStatAccumulator, "sessionCount");
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = 
				filterSessionAndAggrStat(
						sessionid2FullAggrInfoRDD, 
						taskParam,
						sessionAggrStatAccumulator).cache();		
		
		JavaPairRDD<String, Row> sessionid2DetailRDD = 
				getSessionid2DetailRDD(filteredSessionid2AggrInfoRDD, sessionid2ActionRDD);
		sessionid2DetailRDD = sessionid2DetailRDD.persist(StorageLevel.MEMORY_ONLY());
		
		/**
		 * 特别说明
		 */
		randomExtractSession(sc, taskId, filteredSessionid2AggrInfoRDD, sessionid2DetailRDD);

		// 计算出各个范围的session占比，并写入MySql
		calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskId());
		
		
		/**
		 * session聚合统计（统计出访问时长和访问步长，各个区间的session数据占总session数量的比例）
		 * 如果不进行重构，直接来实现，思路：
		 * 1、actionRDD，映射成<sessionid, Row>格式
		 * 2、按sessionid聚合，计算出每个session的访问时长和访问步长，生成一个新的RDD
		 * 3、遍历新生成的RDD，将每个session的访问时长和访问步长，去更新自定义Accumulator中对应的值
		 * 4、使用自定义Accumulator中的统计值，去计算各个区间的比例
		 * 5、将最后计算出来的结果，写入Mysql对应的表中。
		 * 
		 * 普通实现思路的问题
		 * 1、为什么还要用actionRDD，去映射？其实我们之前在session聚合的时候，映射已经做过了，多此一举
		 * 2、是不是一定要为了session的聚合这个功能，单独去遍历一遍session？其实没有必要，已经有session数据
		 *    之前过滤session的时候，其实相当于是在遍历session，那么这里就没必要再过滤一遍了
		 * 
		 * 重构实现思路
		 * 1、不要生成任何新的RDD
		 * 2、不要单独去遍历一遍session的数据（处理上千万的数据）
		 * 3、可以在进行session聚合的时候就直接计算出来每个sesion的访问时长和访问步长
		 * 4、在进行过滤的时候，本来就要遍历所有的聚合session信息，此时，就可以在某个session通过筛选条件后
		 *    将其访问时长和访问步长，累加到自定义的Accumulator上面去
		 * 5、就是两种截然不同的思考方式和实现方式，在面对上千亿的数据的时候，甚至可以节省数个小时
		 * 
		 * 开发Spark大型复杂项目的一些经验准则
		 * 1、尽量少生成RDD
		 * 2、尽量少对RDD进行算子操作，如果有可能，尽量在一个算子里面，实现多个需要做的功能
		 * 3、尽量少对RDD进行shuffle算子操作，比如groupByKey、reduceBykey、sortByKey
		 *    shuffle操作会导致大量的磁盘读写，严重降低性能
		 *    有shuffle的算子和没有shuffle的算子，甚至性能会达到数个小时的差别
		 *    有shuffle的算子，很容易导致数据倾斜，一旦数据倾斜，简直就是性能杀手（完整的解决方案）
		 * 4、无论做什么功能，性能第一
		 *    在传统的J2EE、.net、php，软件/系统/网站开发中，我认为是架构和可维护性、可扩展性的重要
		 *    程度远远高于了性能，大量分布式的架构、设计模式、代码的划分、类的划分（高并发网站除外）
		 *    
		 *    在大数据项目中，比如MapReduce、Hive、Spark、Storm，我认为性能的重要成都远远大于一些
		 *    代码的规范，和设计模式、代码划分、类的划分；大数据最重要的就是性能；
		 *    主要就是因为大数据以及大数据项目的特点，决定了大数据的程序和项目的速度，都比较慢，如果不考虑
		 *    性能的话，会导致一个大数据处理程序运行时间长达数十个小时，此时对于用户体验简直是场灾难
		 *    
		 *    所以，推荐大数据项目中，优先考虑性能，其次考虑功能代码的划分、解耦和
		 *    
		 *    我们如果采用第一种实现方案，那么就是采用代码划分优先、设计优先
		 *    如果采用第二种方案，就是采用性能优先
		 *    
		 */
		
		// 获取top10热门品类
		List<Tuple2<CategorySortKey, String>> top10CategoryList =
				getTop10Category(task.getTaskId(), sessionid2DetailRDD);
		
		// 获取top10活跃session
		getTop10Session(sc, task.getTaskId(), top10CategoryList, sessionid2DetailRDD);
		
		// 关闭spark上下文
		sparkSession.close();
	}
	
	public static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
//		return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<String, Row> call(Row row) throws Exception {
//				String sessionid = row.getString(2);
//				return new Tuple2<String, Row>(sessionid, row);
//			}
//		});
		
		return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {
				List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String,Row>>();
				
				while (iterator.hasNext()) {
					Row row = iterator.next();
					String sessionid = row.getString(2);
					list.add(new Tuple2<String, Row>(sessionid, row));
				}
				return list.iterator();
			}
		});
	}

	/**
	 * 对行为数据按session粒度进行聚合
	 * @param actionRDD
	 * @return
	 */
	public static JavaPairRDD<String, String> aggregateBySession(
			SparkSession sparkSession, JavaPairRDD<String, Row> sessionid2ActionRDD) {
		
		// 对行为数据按session粒度进行分组
		JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD =
				sessionid2ActionRDD.groupByKey();
		
		// 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
		JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionId2ActionsRDD.mapToPair(
				new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
						String sessionId = tuple._1;
						Iterator<Row> iterator = tuple._2.iterator();
						
						StringBuffer searchKeyWordsBuffer = new StringBuffer("");
						StringBuffer clickCategoryIdsBuffer = new StringBuffer("");
						
						Long userId = null;
						
						Date startTime = null;
						Date endTime = null;
						int stepLength = 0;
						
						// 遍历session所有的访问行为
						while (iterator.hasNext()) {
							// 提取每个访问行为的搜索词字段和点击品类字段
							Row row = iterator.next();
							
							if (userId == null) {
								userId = row.getLong(1);
							}
							
							String searchKeyWord = row.isNullAt(5) ? null : row.getString(5);
							Long clickCategoryId = row.isNullAt(6) ? null : row.getLong(6);
							
							/**
							 * 实际上这里要说明一下
							 * 并不是每一行访问行为都有searchKeyWord和clickCategoryId两个字段的
							 * 其实，只有搜索行为是由searchKeyWord字段的
							 * 只有点击品类行为，是由clickCategoryId字段的
							 * 所以，任何一行的行为数据，都不可能两个字段都有，所以数据是可能出现null值的
							 * 我们决定是否将搜索词或点击品类id拼接到字符串中去
							 * 首先要满足：不能是null值，
							 * 其次，之前的字符串中还没有搜索词或点击品类id
							 */
							if (StringUtils.isNotEmpty(searchKeyWord)) {
								if (!searchKeyWordsBuffer.toString().contains(searchKeyWord)) {
									searchKeyWordsBuffer.append(searchKeyWord + ",");
								}
							}
							
							if (clickCategoryId != null) {
								if (!clickCategoryIdsBuffer.toString().contains(
										String.valueOf(clickCategoryId))) {
									clickCategoryIdsBuffer.append(clickCategoryId + ",");
								}
							}
							
							// 计算session开始和结束时间
							Date actionTime = DateUtils.parseTime(
									row.isNullAt(4) ? "" : row.getString(4));
							if (actionTime != null) {
								if (startTime == null) {
									startTime = actionTime;
								}
								if (endTime == null) {
									endTime = actionTime;
								}
								if (actionTime.before(startTime)) {
									startTime = actionTime;
								}
								if (actionTime.after(endTime)) {
									endTime = actionTime;
								}
							}
							
							// 计算访问步长
							stepLength++;
						}
						
						String searchKeywords = StringUtils.trimComma(searchKeyWordsBuffer.toString());
						String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
						
						// 计算session访问时长，要的是s
						long visitLength = 0;
						if (startTime != null) {
							visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
						}
						
						// 大家思考一下
						// 我们返回的数据格式，即是<sessionid, partAggrInfo>
						// 但是，这一步聚合完了以后，其实，我们是还需要将每一行的数据，跟对应的用户信息进行聚合
						// 问题就来了，如果是跟用户信息进行聚合的话，那么key就不应该是sessionid
						// 那就应该是userid，才能够跟<userid, Row>格式的用户信息进行聚合
						// 如果我们这里直接返回<sessionid, partAggrInfo>，还得再做一次mapToPair的算子
						// 将RDD映射成<userid, partAggrInfo>的格式，那么就多此一举
						
						// 所以，我们这里其实可以直接返回的数据格式就是<userid, partAggrInfo>
						// 然后跟用户信息join的时候，将partAggrInfo关联上userInfo，
						// 然后在直接将返回的Tuple的key设置成sessionid
						// 最后的数据格式，还是<sessionid, fullAggrInfo>
						
						String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
								+ (StringUtils.isNotEmpty(searchKeywords) ? Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" : "")
								+ (StringUtils.isNotEmpty(clickCategoryIds) ? Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" : "")
								+ (Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" )
								+ (Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|")
								+ Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);
						
						return new Tuple2<Long, String>(userId, partAggrInfo);
					}
				});
		
		// 查询所有的用户数据
		String sql = "select * from user_info";
		JavaRDD<Row> userInfoRdd = sparkSession.sql(sql).javaRDD();
		
		JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRdd.mapToPair(
				new PairFunction<Row, Long, Row>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Row> call(Row row) throws Exception {
						return new Tuple2<Long, Row>(row.getLong(0), row);
					}
				});
		
		// 将session粒度聚合数据，与用户信息进行join
		JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = 
				userid2PartAggrInfoRDD.join(userid2InfoRDD);
		
		// 对join起来的数据进行拼接，并且返回<sessionid, fullAggrInfo>格式的数据
		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(
				new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
						String partAggrInfo = tuple._2._1;
						Row userInfoRow = tuple._2._2;
						
						String sessionid = StringUtils.getFieldFromConcatString(
								partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
						
						int age = userInfoRow.getInt(3);
						String professional = userInfoRow.getString(4);
						String city = userInfoRow.getString(5);
						String sex = userInfoRow.getString(6);
						
						String fullAggrInfo = partAggrInfo + "|"
								+ Constants.FIELD_AGE + "=" + age + "|"
								+ Constants.FIELD_PROFESSIONNAL + "=" + professional + "|"
								+ Constants.FIELD_CITY + "=" + city + "|"
								+ Constants.FIELD_SEX + "=" + sex;
						
						if (fullAggrInfo.endsWith("\\|")) {
							fullAggrInfo = fullAggrInfo.substring(0, fullAggrInfo.length()-1);
						}
						
						return new Tuple2<String, String>(sessionid, fullAggrInfo);
					}
				});
		
		return sessionid2FullAggrInfoRDD;
	}
	
	public static JavaPairRDD<String, String> filterSessionAndAggrStat(
			JavaPairRDD<String, String> sessionid2AggrInfoRDD, 
			final JSONObject taskParam,
			final AccumulatorV2<String, String> sessionAggrStatAccumulator) {
		
		String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
		String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
		String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
		String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
		String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
		String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
		String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);
		
		String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
				+ (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
				+ (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
				+ (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
				+ (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
				+ (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
				+ (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");
		
		if (_parameter.endsWith("\\|")) {
			_parameter = _parameter.substring(0, _parameter.length()-1);
		}
		
		final String parameter = _parameter;
		
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(new Function<Tuple2<String,String>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> tuple) throws Exception {
				// 首先，从tuple中获取聚合数据
				String aggrInfo = tuple._2;
				
				// 接着，依次按照筛选条件进行过滤
				// 按照年龄范围进行过滤				
				if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, 
						parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
					return false;
				}
				
				// 按照职业范围进行过滤
				if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONNAL,
						parameter, Constants.PARAM_PROFESSIONALS)) {
					return false;
				}
				
				// 按照城市范围进行过滤
				if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
						parameter, Constants.PARAM_CITIES)) {
					return false;
				}
				
				// 按照性别进行过滤
				if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
						parameter, Constants.PARAM_SEX)) {
					return false;
				}
				
				// 按照搜索词进行过滤
				if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
						parameter, Constants.PARAM_KEYWORDS) ) {
					return false;
				}
				
				// 按照点击品类ID进行过滤
				if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
						parameter, Constants.PARAM_CATEGORY_IDS)) {
					return false;
				}
				
				// 如果经过了前面多个过滤条件，那么说明该session就是合法的
				// 那么就要对session的访问时长和访问步长进行统计，根据session对应的范围进行累加计数
				sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
				Long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
						aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
				Long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
						aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
				calculateVisitLength(visitLength);
				calculateStepLength(stepLength);
				
				return true;
			}
			
			/**
			 * 计算访问时长范围
			 * @param visitLength
			 */
			private void calculateVisitLength(long visitLength) {
				if(visitLength >=1 && visitLength <= 3) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);  
				} else if(visitLength >=4 && visitLength <= 6) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);  
				} else if(visitLength >=7 && visitLength <= 9) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);  
				} else if(visitLength >=10 && visitLength <= 30) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);  
				} else if(visitLength > 30 && visitLength <= 60) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);  
				} else if(visitLength > 60 && visitLength <= 180) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);  
				} else if(visitLength > 180 && visitLength <= 600) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);  
				} else if(visitLength > 600 && visitLength <= 1800) {  
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);  
				} else if(visitLength > 1800) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);  
				} 
			}
			
			/**
			 * 计算访问步长范围
			 * @param stepLength
			 */
			private void calculateStepLength(long stepLength) {
				if(stepLength >= 1 && stepLength <= 3) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);  
				} else if(stepLength >= 4 && stepLength <= 6) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);  
				} else if(stepLength >= 7 && stepLength <= 9) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);  
				} else if(stepLength >= 10 && stepLength <= 30) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);  
				} else if(stepLength > 30 && stepLength <= 60) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);  
				} else if(stepLength > 60) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);    
				}
			}
		});
		
		return filteredSessionid2AggrInfoRDD;
	}
		
	private static JavaPairRDD<String, Row> getSessionid2DetailRDD(
			JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2ActionRDD) {
		JavaPairRDD<String, Row> sessionid2detailRDD =
				filteredSessionid2AggrInfoRDD.join(sessionid2ActionRDD).mapToPair(
						new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, Row>() {

							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
								return new Tuple2<String, Row>(tuple._1, tuple._2._2);
							}
				});
		return sessionid2detailRDD;
	}
	
	/**
	 * 随机抽取session
	 * @param session2AggrInfoRDD
	 */
	private static void randomExtractSession(
			JavaSparkContext sc,
			final long taskid,
			JavaPairRDD<String, String> session2AggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2DetailRDD) {
		// 第一步，计算出每天的session数量，
		
		// 获取<yyyy-MM-dd_HH,sessionid>格式的RDD
		JavaPairRDD<String, String> time2AggrInfoRDD = session2AggrInfoRDD.mapToPair(
				new PairFunction<Tuple2<String,String>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
						String aggrInfo = tuple._2;
						
						String startTime = StringUtils.getFieldFromConcatString(
								aggrInfo, "\\|", Constants.FIELD_START_TIME);
						String dateHour = DateUtils.getDateHour(startTime);					
						
						return new Tuple2<String, String>(dateHour, aggrInfo);
					}
		});
		
		/**
		 * 思考一下，
		 * 
		 * 
		 */
		Map<String, Long> countMap = time2AggrInfoRDD.countByKey();
		
		// 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取的session索引
		Map<String, Map<String, Long>> dayHourCountMap = 
				new HashMap<String, Map<String, Long>>();
		for(Map.Entry<String, Long> countEntry : countMap.entrySet()) {
			String dateHour = countEntry.getKey();
			String date = dateHour.split("_")[0];
			String hour = dateHour.split("_")[1];
			Long count = countEntry.getValue();
			
			Map<String, Long> hourCountMap = dayHourCountMap.get(date);
			if (hourCountMap == null) {
				hourCountMap = new HashMap<String, Long>();
				dayHourCountMap.put(date, hourCountMap);
			}
			
			hourCountMap.put(hour, count);
		}
		
		// 开始实现我们的按时间比例随机抽取算法
		// 总共要抽取100个session，先按照天数，进行平分
		Random random = new Random();
		long extractNumberPerDay = 100 / dayHourCountMap.size();		
		Map<String, Map<String, List<Integer>>> dateHourExtractMap = 
				new HashMap<String, Map<String, List<Integer>>>();
		
		for (Map.Entry<String, Map<String, Long>> dayHourCountEntry : dayHourCountMap.entrySet()) {
			String date = dayHourCountEntry.getKey();
			Map<String, Long> hourCountMap = dayHourCountEntry.getValue();
			
			// 计算出这一天session总数
			long sessionCount = 0L;
			for (long hourCount : hourCountMap.values()) {
				sessionCount += hourCount;
			}
			
			Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
			if (hourExtractMap == null) {
				hourExtractMap = new HashMap<String, List<Integer>>();
				dateHourExtractMap.put(date, hourExtractMap);
			}
			for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
				String hour = hourCountEntry.getKey();
				long count = hourCountEntry.getValue();
				
				long hourExtractNumber = (long) (((double)count / (double)sessionCount)
						* extractNumberPerDay);
				if (hourExtractNumber > count) {
					hourExtractNumber = count;
				}
				
				// 先获取当前小时的存放随机数的list
				List<Integer> extractIndexList = hourExtractMap.get(hour);
				if (extractIndexList == null) {
					extractIndexList = new ArrayList<Integer>();
					hourExtractMap.put(hour, extractIndexList);
				}
				
				// 生成上面计算出来的数量的随机数
				for (int i = 0; i < hourExtractNumber; i++) {
					int extractIndex = random.nextInt((int)count);
					
					while (extractIndexList.contains(extractIndex)) {
						extractIndex = random.nextInt((int)count);
					}
					
					extractIndexList.add(extractIndex);
				}
			}
		}
		
		// 第三步 遍历每天每小时的session，然后根据随机索引抽取session
		
		// 对于大变量，使用broadcast可以减少网络传输和内存消耗
		final Broadcast<Map<String, Map<String, List<Integer>>>> dateHourExtractMapBroadcast =
				sc.broadcast(dateHourExtractMap);

		// 对之前groupByKey算子，得到<dateHour, <session aggrInfo>>
		JavaPairRDD<String, Iterable<String>> time2SessionsRDD = time2AggrInfoRDD.groupByKey();
		JavaPairRDD<String, String> extractSessionidsRDD = time2SessionsRDD
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple)
							throws Exception {
						List<Tuple2<String, String>> extractSessionids = new ArrayList<Tuple2<String, String>>();

						String dateHour = tuple._1;
						String date = dateHour.split("_")[0];
						String hour = dateHour.split("_")[1];

						Iterator<String> iterator = tuple._2.iterator();

						try {
							Map<String, Map<String, List<Integer>>> dateHourExtractMap = 
									dateHourExtractMapBroadcast.getValue();
							
							List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);
	
							ISessionRandomExtractDAO sessionRandomExtractDAO = 
									DAOFactory.getSessionRandomExtractDAO();
							int index = 0;
							while (iterator.hasNext()) {
								String sessionAggrInfo = iterator.next();
								String sessionId = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|",
										Constants.FIELD_SESSION_ID);
	
								if (extractIndexList.contains(index)) {
									SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
									sessionRandomExtract.setTaskid(taskid);
									sessionRandomExtract.setSessionid(sessionId);
									sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessionAggrInfo,
											"\\|", Constants.FIELD_START_TIME));
									sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
											sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
									sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
											sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
									sessionRandomExtractDAO.insert(sessionRandomExtract);
	
									extractSessionids.add(new Tuple2<String, String>(sessionId, sessionId));
								}
	
								index++;
							}
						} catch (Exception e) {
							System.out.println("Date " + date + ", Hour " + hour);
						}

						return extractSessionids.iterator();
					}
				});
		
		// 第四步 获取抽取出来的session的明细数据
		JavaPairRDD<String, Tuple2<String, Row>> extractSessionid2DetailRDD =
				extractSessionidsRDD.join(sessionid2DetailRDD);
		extractSessionid2DetailRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Tuple2<String,Row>>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Iterator<Tuple2<String, Tuple2<String, Row>>> iterator) throws Exception {
				List<SessionDetail> sessionDetails = new ArrayList<SessionDetail>();
				
				while(iterator.hasNext()) {
					Tuple2<String, Tuple2<String, Row>> tuple = iterator.next();
					Row row = tuple._2._2;
				
					SessionDetail sessionDetail = new SessionDetail();
					sessionDetail.setTaskid(taskid);
					sessionDetail.setUserid(row.getLong(1));
					sessionDetail.setSessionid(row.getString(2));
					sessionDetail.setPageid(row.getLong(3));
					sessionDetail.setActionTime(row.getString(4));
					sessionDetail.setSearchKeyword(row.getString(5));
					sessionDetail.setClickCategoryId(row.isNullAt(6)?0L:row.getLong(6));
					sessionDetail.setClickProductId(row.isNullAt(7)?0L:row.getLong(7));
					sessionDetail.setOrderCategoryIds(row.getString(8));
					sessionDetail.setOrderProductIds(row.getString(9));
					sessionDetail.setPayCategoryIds(row.getString(10));
					sessionDetail.setPayProductIds(row.getString(11));
					
					sessionDetails.add(sessionDetail);
				}
								
				DAOFactory.getSessionDetailDAO().insertBatch(sessionDetails);
			}
		});
	}
	
	/**
	 * 计算各session范围占比，并写入MySql
	 * @param value
	 */
	public static void calculateAndPersistAggrStat(String value, long taskId) {
		System.out.println(value);
		
		long session_count = Long.valueOf(StringUtils
				.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));
		
		long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_1s_3s));  
		long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_4s_6s));
		long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_7s_9s));
		long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_10s_30s));
		long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_30s_60s));
		long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_1m_3m));
		long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_3m_10m));
		long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_10m_30m));
		long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_30m));
		
		long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_1_3));
		long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_4_6));
		long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_7_9));
		long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_10_30));
		long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_30_60));
		long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_60));
		
		//System.out.println("session_count " + session_count);
		
		// 计算各个访问时长和访问步长的范围
		double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
				(double)visit_length_1s_3s / (double)session_count, 2);  
		double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
				(double)visit_length_4s_6s / (double)session_count, 2);  
		double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
				(double)visit_length_7s_9s / (double)session_count, 2);  
		double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
				(double)visit_length_10s_30s / (double)session_count, 2);  
		double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
				(double)visit_length_30s_60s / (double)session_count, 2);  
		double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
				(double)visit_length_1m_3m / (double)session_count, 2);
		double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
				(double)visit_length_3m_10m / (double)session_count, 2);  
		double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
				(double)visit_length_10m_30m / (double)session_count, 2);
		double visit_length_30m_ratio = NumberUtils.formatDouble(
				(double)visit_length_30m / (double)session_count, 2);  
		
		double step_length_1_3_ratio = NumberUtils.formatDouble(
				(double)step_length_1_3 / (double)session_count, 2);  
		double step_length_4_6_ratio = NumberUtils.formatDouble(
				(double)step_length_4_6 / (double)session_count, 2);  
		double step_length_7_9_ratio = NumberUtils.formatDouble(
				(double)step_length_7_9 / (double)session_count, 2);  
		double step_length_10_30_ratio = NumberUtils.formatDouble(
				(double)step_length_10_30 / (double)session_count, 2);  
		double step_length_30_60_ratio = NumberUtils.formatDouble(
				(double)step_length_30_60 / (double)session_count, 2);  
		double step_length_60_ratio = NumberUtils.formatDouble(
				(double)step_length_60 / (double)session_count, 2);  
		
		SessionAggrStat sessionAggrStat = new SessionAggrStat();
		sessionAggrStat.setTaskid(taskId);
		sessionAggrStat.setSession_count(session_count);
		sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);  
		sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);  
		sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);  
		sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);  
		sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);  
		sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio); 
		sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);  
		sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio); 
		sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);  
		sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);  
		sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);  
		sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);  
		sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);  
		sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);  
		sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio); 
		
		DAOFactory.getSessionAggrStatDAO().insert(sessionAggrStat);
	}


	/**
	 * 获取top10品类，并写入mysql数据表中
	 * @param taskId
	 * @param sessionid2DetailRDD
	 */
	private static List<Tuple2<CategorySortKey, String>> getTop10Category(
			long taskId, JavaPairRDD<String, Row> sessionid2DetailRDD) {
		
		// 第一步，获取符合条件的session访问过的所有品类
		JavaPairRDD<Long, Long> categoryidRDD = sessionid2DetailRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> t) throws Exception {
						Row row = t._2;
						
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long,Long>>();
						if (!row.isNullAt(6)) {
							list.add(new Tuple2<Long, Long>(row.getLong(6), row.getLong(6)));
						}
						
						if (!row.isNullAt(8)) {
							String[] orderCategoryids = row.getString(8).split(",");
							for (String orderCategoryid : orderCategoryids) {
								list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryid),
										Long.valueOf(orderCategoryid)));
							}
						}
						
						if (!row.isNullAt(9)) {
							String[] payCategoryids = row.getString(9).split(",");
							for (String payCategoryid : payCategoryids) {
								list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryid),
										Long.valueOf(payCategoryid)));
							}
						}
						
						return list.iterator();
					}
				});
		
		// categoryid去重
		categoryidRDD = categoryidRDD.distinct();
		
		// 第二步，计算各品类的点击、下单、支付次数
		JavaPairRDD<Long, Long> clickCategoryid2CountRDD =
				getClickCategoryid2CountRDD(sessionid2DetailRDD);
		JavaPairRDD<Long, Long> orderCategoryid2CountRDD =
				getOrderCategoryid2CountRDD(sessionid2DetailRDD);
		JavaPairRDD<Long, Long> payCategoryid2CountRDD =
				getPayCategoryid2CountRDD(sessionid2DetailRDD);
		
		// 第三步，join各个品类和它的点击、下单、支付次数
		JavaPairRDD<Long, String> categoryid2CountRDD = joinCategoryAndData(
				categoryidRDD, clickCategoryid2CountRDD, 
				orderCategoryid2CountRDD, payCategoryid2CountRDD);
		
		// 第四步，自定义二次排序key
		
		// 第五步，将数据映射成<CategorySortKey, String>格式的RDD，然后进行二次排序
		JavaPairRDD<CategorySortKey, String> sortCategory2CountRDD = categoryid2CountRDD.mapToPair(
				new PairFunction<Tuple2<Long,String>, CategorySortKey, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple) throws Exception {
						String value = tuple._2;
						long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
								value, "\\|", Constants.FIELD_CLICK_COUNT));
						long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
								value, "\\|", Constants.FIELD_ORDER_COUNT));
						long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
								value, "\\|", Constants.FIELD_PAY_COUNT));
						
						CategorySortKey categorySortKey = new CategorySortKey(
								clickCount, orderCount, payCount);
						
						return new Tuple2<CategorySortKey, String>(categorySortKey, value);
					}
				});
		
		JavaPairRDD<CategorySortKey, String> sortedCategoryid2CountRDD =
				sortCategory2CountRDD.sortByKey(false);
		
		// 第六步，用take(10)取出top10热门品类，并写入mysql
		ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
		List<Tuple2<CategorySortKey, String>> top10Category2CountList = 
				sortedCategoryid2CountRDD.take(10);
		for (Tuple2<CategorySortKey, String> category : top10Category2CountList) {
			Top10Category top10Category = new Top10Category();
			top10Category.setTaskId(taskId);
			top10Category.setCategoryId(Long.valueOf(StringUtils.getFieldFromConcatString(
					category._2, "\\|", Constants.FIELD_CATEGORY_ID)));
			top10Category.setClickCount(Long.valueOf(StringUtils.getFieldFromConcatString(
					category._2, "\\|", Constants.FIELD_CLICK_COUNT)));
			top10Category.setOrderCount(Long.valueOf(StringUtils.getFieldFromConcatString(
					category._2, "\\|", Constants.FIELD_ORDER_COUNT)));
			top10Category.setPayCount(Long.valueOf(StringUtils.getFieldFromConcatString(
					category._2, "\\|", Constants.FIELD_PAY_COUNT)));
			top10CategoryDAO.insert(top10Category);
		}
		
		return top10Category2CountList;
	}
	
	private static JavaPairRDD<Long, Long> getClickCategoryid2CountRDD(
			JavaPairRDD<String, Row> sessionid2DetailRDD) {
		
		/**
		 * 点击行为的数据只占总数据的一小部分，所以在过滤后，很容易产生一些partition的数据很少，导致数据倾斜
		 * 所以在过滤后，要做coalesce操作，对partition进行压缩，减少partition的数量，使得每个partition数据均衡
		 */
		
		// 先过滤出点击的category
		JavaPairRDD<String, Row> clickActionRDD = sessionid2DetailRDD.filter(new Function<Tuple2<String,Row>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Row> tuple) throws Exception {
				return tuple._2.isNullAt(6) ? false : true;
			}
		});
		//.coalesce(100);
		
		// map
		JavaPairRDD<Long, Long> clickCategoryidRDD = clickActionRDD.mapToPair(
				new PairFunction<Tuple2<String,Row>, Long, Long>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Long> call(Tuple2<String, Row> t) throws Exception {
						Row row = t._2;
						return new Tuple2<Long, Long>(row.getLong(6), 1L);
					}
				});
		
		// reduce
		JavaPairRDD<Long, Long> clickCategoryid2CountRDD = clickCategoryidRDD.reduceByKey(
				new Function2<Long, Long, Long>() {
					
					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				});
		
		return clickCategoryid2CountRDD;
	}

	private static JavaPairRDD<Long, Long> getOrderCategoryid2CountRDD(
			JavaPairRDD<String, Row> sessionid2DetailRDD) {
		
		// 先过滤出下单的category
		JavaPairRDD<String, Row> orderActionRDD = sessionid2DetailRDD
				.filter(new Function<Tuple2<String, Row>, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						return tuple._2.isNullAt(8) ? false : true;
					}
				});

		// map
		JavaPairRDD<Long, Long> orderCategoryidRDD = orderActionRDD
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						String[] orderCategoryids = row.getString(8).split(",");
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long,Long>>();
						for (String orderCategoryid : orderCategoryids) {
							list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryid), 1L));
						}
						return list.iterator();
					}
				});

		// reduce
		JavaPairRDD<Long, Long> orderCategoryid2CountRDD = orderCategoryidRDD
				.reduceByKey(new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				});

		return orderCategoryid2CountRDD;
	}

	private static JavaPairRDD<Long, Long> getPayCategoryid2CountRDD(
			JavaPairRDD<String, Row> sessionid2DetailRDD) {
		// 先过滤出下单的category
		JavaPairRDD<String, Row> payActionRDD = sessionid2DetailRDD
				.filter(new Function<Tuple2<String, Row>, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						return tuple._2.isNullAt(9) ? false : true;
					}
				});

		// map
		JavaPairRDD<Long, Long> payCategoryidRDD = payActionRDD
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						String[] payCategoryids = row.getString(9).split(",");
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
						for (String payCategoryid : payCategoryids) {
							list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryid), 1L));
						}
						return list.iterator();
					}
				});

		// reduce
		JavaPairRDD<Long, Long> payCategoryid2CountRDD = payCategoryidRDD
				.reduceByKey(new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				});

		return payCategoryid2CountRDD;
	}

	private static JavaPairRDD<Long, String> joinCategoryAndData(JavaPairRDD<Long, Long> categoryidRDD,
			JavaPairRDD<Long, Long> clickCategoryid2CountRDD, JavaPairRDD<Long, Long> orderCategoryid2CountRDD,
			JavaPairRDD<Long, Long> payCategoryid2CountRDD) {
		
		JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD = 
				categoryidRDD.leftOuterJoin(clickCategoryid2CountRDD);
		
		JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD.mapToPair(
				new PairFunction<Tuple2<Long,Tuple2<Long,Optional<Long>>>, Long, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple) throws Exception {
						long categoryid = tuple._1;
						Optional<Long> optional = tuple._2._2;
						long clickCount = 0L;
						
						if (optional.isPresent()) {
							clickCount = optional.get();
						}
						
						String value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|"
								+ Constants.FIELD_CLICK_COUNT + "=" + clickCount;
						
						return new Tuple2<Long, String>(categoryid, value);
					}
				});
		
		tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryid2CountRDD).mapToPair(
				new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
						long categoryid = tuple._1;
						Optional<Long> optional = tuple._2._2;
						String value = tuple._2._1;
						long orderCount = 0L;
						
						if (optional.isPresent()) {
							orderCount = optional.get();
						}
						
						value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;
						
						return new Tuple2<Long, String>(categoryid, value);
					}
				});
		
		tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryid2CountRDD).mapToPair(
				new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
						long categoryid = tuple._1;
						Optional<Long> optional = tuple._2._2;
						String value = tuple._2._1;
						long payCount = 0L;
						
						if (optional.isPresent()) {
							payCount = optional.get();
						}
						
						value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;
						
						return new Tuple2<Long, String>(categoryid, value);
					}
				});
		
		return tmpMapRDD;
	}


	private static void getTop10Session(JavaSparkContext sc, final long taskId, 
			List<Tuple2<CategorySortKey, String>> top10CategoryList,
			JavaPairRDD<String, Row> sessionid2DetailRDD) {
		// 第一步，将top10热门品类的id，生成一份RDD
		List<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<Tuple2<Long,Long>>();
		for (Tuple2<CategorySortKey, String> top10Category : top10CategoryList) {
			long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(
					top10Category._2, "\\|", Constants.FIELD_CATEGORY_ID));
			top10CategoryIdList.add(new Tuple2<Long, Long>(categoryId, categoryId));
		}
		
		JavaPairRDD<Long, Long> top10CategoryIdRDD = sc.parallelizePairs(top10CategoryIdList);
		
		// 第二步，计算top10品类被各session点击的次数
		
		// 按session分组
		JavaPairRDD<String, Iterable<Row>> sessionid2DetailGroupRDD =
				sessionid2DetailRDD.groupByKey();
		JavaPairRDD<Long, String> categoryid2SessionCountRDD = sessionid2DetailGroupRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
						Iterator<Row> iterator = tuple._2.iterator();
						String sessionid = tuple._1;
						
						Map<Long, Long> categoryCountMap = new HashMap<Long, Long>();
						while (iterator.hasNext()) {
							Row row = iterator.next();
							
							if (!row.isNullAt(6)) {
								long categoryid = row.getLong(6);
								Long count = categoryCountMap.get(categoryid);
								if (count == null) {
									count = 0L;
								}
								count++;
								categoryCountMap.put(categoryid, count);
							}							
						}
						
						List<Tuple2<Long, String>> categoryCountList = new ArrayList<Tuple2<Long,String>>();
						for(Map.Entry<Long, Long> entry : categoryCountMap.entrySet()) {
							long categoryid = entry.getKey();
							long count = entry.getValue();
							
							String value = sessionid + "," + count;
							categoryCountList.add(new Tuple2<Long, String>(categoryid, value));
						}
						
						return categoryCountList.iterator();
					}
					
				});
		
		// 获取top10热门品类被各个session点击的次数
		JavaPairRDD<Long, String> top10CategorySessionCountRDD =
				top10CategoryIdRDD.join(categoryid2SessionCountRDD).mapToPair(
						new PairFunction<Tuple2<Long,Tuple2<Long,String>>, Long, String>() {

							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> tuple) throws Exception {
								
								return new Tuple2<Long, String>(tuple._1, tuple._2._2);
							}
						});
		
		// 第三步，分组获取Top10算法，获取每个品类的top10活跃用户
		JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD =
				top10CategorySessionCountRDD.groupByKey();
		JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<Long,Iterable<String>>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> tuple) throws Exception {
						long categoryid = tuple._1;
						Iterator<String> iterator = tuple._2.iterator();
						
						String[] top10Sessions = new String[10];
						while (iterator.hasNext()) {
							String value = iterator.next();
							long count = Long.valueOf(value.split(",")[1]);
							
							for (int i = 0; i < top10Sessions.length; i++) {
								if (top10Sessions[i] == null) {
									top10Sessions[i] = value;
									break;
								} else {
									long tmpCount = Long.valueOf(top10Sessions[i].split(",")[1]);
									if (count > tmpCount) {
										for(int j = 9; j > i; j--) {
											top10Sessions[j] = top10Sessions[j-1];
										}
										top10Sessions[i] = value;
										break;
									}
								}
							}
						}
						
						ITop10CategorySessionDAO top10CategorySessionDAO = DAOFactory.getTop10CategorySessionDAO();
						List<Tuple2<String, String>> list = new ArrayList<Tuple2<String,String>>();
						for (String session : top10Sessions) {
							if (session != null) {
								String sessionid = session.split(",")[0];
								long count = Long.valueOf(session.split(",")[1]);
								
								Top10CategorySession top10CategorySession = new Top10CategorySession();
								top10CategorySession.setTaskId(taskId);
								top10CategorySession.setCategoryId(categoryid);
								top10CategorySession.setSessionId(sessionid);
								top10CategorySession.setClickCount(count);
								top10CategorySessionDAO.insert(top10CategorySession);
								
								list.add(new Tuple2<String, String>(sessionid, sessionid));
							}
						}
						
						return list.iterator();
					}
				});
		
		JavaPairRDD<String, Tuple2<String, Row>> top10SessionDetailRDD = 
				top10SessionRDD.join(sessionid2DetailRDD);
		
		top10SessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
				Row row = tuple._2._2;
			
				SessionDetail sessionDetail = new SessionDetail();
				sessionDetail.setTaskid(taskId);
				sessionDetail.setUserid(row.getLong(1));
				sessionDetail.setSessionid(row.getString(2));
				sessionDetail.setPageid(row.getLong(3));
				sessionDetail.setActionTime(row.getString(4));
				sessionDetail.setSearchKeyword(row.getString(5));
				sessionDetail.setClickCategoryId(row.isNullAt(6)?0L:row.getLong(6));
				sessionDetail.setClickProductId(row.isNullAt(7)?0L:row.getLong(7));
				sessionDetail.setOrderCategoryIds(row.getString(8));
				sessionDetail.setOrderProductIds(row.getString(9));
				sessionDetail.setPayCategoryIds(row.getString(10));
				sessionDetail.setPayProductIds(row.getString(11));
				DAOFactory.getSessionDetailDAO().insert(sessionDetail);
			}
		});
	}

}
