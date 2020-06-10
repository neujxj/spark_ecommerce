package com.mylp.sparkproject.spark.page;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.alibaba.fastjson.JSONObject;
import com.mylp.sparkproject.constant.Constants;
import com.mylp.sparkproject.dao.IPageSplitConvertRateDAO;
import com.mylp.sparkproject.dao.ITaskDAO;
import com.mylp.sparkproject.dao.factory.DAOFactory;
import com.mylp.sparkproject.domain.PageSplitConvertRate;
import com.mylp.sparkproject.domain.Task;
import com.mylp.sparkproject.util.DateUtils;
import com.mylp.sparkproject.util.NumberUtils;
import com.mylp.sparkproject.util.ParamUtils;
import com.mylp.sparkproject.util.SparkUtil;
import com.mylp.sparkproject.util.StringUtils;

import scala.Tuple2;

public class PageOneStepConvertRateSpark {
	
	public static void main(String[] args) {
		// 1、构建spark上下文
		SparkConf sparkConf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PAGE);
		SparkUtil.setMaster(sparkConf);
		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		SparkSession sparkSession = SparkUtil.getSparkSession(sc.sc());
		
		// 2、生成模拟数据
		SparkUtil.mockData(sc, sparkSession);
		
		// 3、查询任务，获取任务参数
		long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE);
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(taskId);
		if (task == null) {
			System.out.println(new Date() + ": not find task id [" + taskId + "].");
			return;
		}
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		
		// 4、查询指定日期范围内的用户访问行为数据
		JavaRDD<Row> actionRDD = SparkUtil.getActionRDDByDateRange(sparkSession, taskParam);
		
		// 获取session到actionRDD
		JavaPairRDD<String, Row> sessionid2ActionRDD = getSessionid2ActionRDD(actionRDD);
		JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2ActionRDD.groupByKey().cache();
		
		// 最核心的一步，每个session的单跳页面切片生成，以及页面流的匹配算法
		JavaPairRDD<String, Integer> pageSplitRDD = generateAndMatchPageSplit(sc, sessionid2ActionsRDD, taskParam);
		Map<String, Long> pageSplitPvMap = pageSplitRDD.countByKey();
		
		// 计算起始页面的PV
		long startPagePv = getStartPagePv(taskParam, sessionid2ActionsRDD);
		
		// 计算页面流转化率
		Map<String, Double> pageConvertRateMap = calculatePageConvertRate(taskParam, startPagePv, pageSplitPvMap);
		
		// 持久化页面流pv到mysql数据库
		persistPageConvertRate(taskId, pageConvertRateMap);
	}

	private static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
		return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				String sessionid = row.getString(2);
				return new Tuple2<String, Row>(sessionid, row);
			}
		});
	}

	/**
	 * 获取页面切片的PV
	 * @param sc
	 * @param sessionid2ActionsRDD
	 * @param taskParam
	 * @return
	 */
	private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(JavaSparkContext sc,
			JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD, JSONObject taskParam) {
		
		String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
		final Broadcast<String> targetPageFlowBroadcast = sc.broadcast(targetPageFlow);	
					
		return sessionid2ActionsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
				
				List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String,Integer>>();

				Iterator<Row> iterator = tuple._2.iterator();
				
				String[] targetPages = targetPageFlowBroadcast.getValue().split(",");
				
				// 把所有的页面点击按照时间先后进行排序
				List<Row> rows = new ArrayList<Row>();
				while (iterator.hasNext()) {
					rows.add(iterator.next());
				}
				
				Collections.sort(rows, new Comparator<Row>() {

					@Override
					public int compare(Row o1, Row o2) {
						String actionTime1 = o1.getString(4);
						String actionTime2 = o2.getString(4);
						
						Date date1 = DateUtils.parseTime(actionTime1);
						Date date2 = DateUtils.parseTime(actionTime2);
						
						return (int)(date1.getTime() - date2.getTime());
					}
				});
				
				// 页面切片的生成，以及页面流的匹配
				Long lastPageId = null;
				for (Row row : rows) {
					long pageid = row.getLong(3);
					
					if (lastPageId == null) {
						lastPageId = pageid;
						continue;
					}
					
					String pageSplit = lastPageId + "_" + pageid;
					
					// 判断这个页面切片是否在用户指定的页面流中
					for (int i = 1; i < targetPages.length; i++) {
						String targetPageSplit = targetPages[i-1] + "_" + targetPages[i];
						if (pageSplit.equals(targetPageSplit)) {
							list.add(new Tuple2<String, Integer>(pageSplit, 1));
						}
					}
					
					lastPageId = pageid;
				}
				
				
				return list.iterator();
			}
		});
	}
	
	/**
	 * 获取起始页面的点击量
	 * @param taskParam
	 * @param sessionid2ActionsRDD
	 * @return
	 */
	private static long getStartPagePv(JSONObject taskParam,
			JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD) {
		String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
		final long startPageId = Long.valueOf(targetPageFlow.split(",")[0]);
		
		JavaRDD<Long> startPageRDD = sessionid2ActionsRDD.flatMap(new FlatMapFunction<Tuple2<String,Iterable<Row>>, Long>() {
				private static final long serialVersionUID = 1L;
	
				@Override
				public Iterator<Long> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
					Iterator<Row> iterator = tuple._2.iterator();
					
					List<Long> list = new ArrayList<Long>();
					while (iterator.hasNext()) {
						long pageId = iterator.next().getLong(3);
						if (pageId == startPageId) {
							list.add(pageId);
						}
					}
					
					return list.iterator();
				}
			});
		
		return startPageRDD.count();
	}

	/**
	 * 计算页面流点击转化率
	 * @param taskParam
	 * @param startPagePv
	 * @param pageSplitPvMap
	 * @return
	 */
	private static Map<String, Double> calculatePageConvertRate(JSONObject taskParam, long startPagePv,
			Map<String, Long> pageSplitPvMap) {
		String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
		String[] pages = targetPageFlow.split(",");
		
		Map<String, Double> pageSplitConvertRateMap = new HashMap<String, Double>();
		
		long lastPageSplitPv = 0;
		double convertRate = 0.0;
		for (int i = 1; i < pages.length; i++) {
			String targetPagePv = pages[i-1] + "_" + pages[i];
			long pageSplitPv = pageSplitPvMap.get(targetPagePv);
			if (i == 1) {
				convertRate = NumberUtils.formatDouble((double) pageSplitPv / (double)startPagePv, 2);
			} else {
				convertRate = NumberUtils.formatDouble((double) pageSplitPv / (double)lastPageSplitPv, 2);
			}

			pageSplitConvertRateMap.put(targetPagePv, convertRate);
			lastPageSplitPv = pageSplitPv;
		}
		
		return pageSplitConvertRateMap;
	}
	
	/**
	 * 持久化页面流点击转化率
	 * @param taskId
	 * @param taskParam
	 * @param pageConvertRateMap
	 */
	private static void persistPageConvertRate(long taskId, 
			Map<String, Double> pageConvertRateMap) {

		StringBuffer buffer = new StringBuffer("");
		
		for (Map.Entry<String, Double> entry : pageConvertRateMap.entrySet()) {
			buffer.append(entry.getKey() + "=" + entry.getValue() + "|");
		}
		
		String convertRate = buffer.toString();
		if (!StringUtils.isEmpty(convertRate)) {
			convertRate = convertRate.substring(0, convertRate.length()-1);
		}
		
		PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
		pageSplitConvertRate.setTaskid(taskId);
		pageSplitConvertRate.setConvertRate(convertRate);
		
		IPageSplitConvertRateDAO pageSplitConvertRateDAO = DAOFactory.getPageSplitConvertRateDAO();
		pageSplitConvertRateDAO.insert(pageSplitConvertRate);
	}
}
