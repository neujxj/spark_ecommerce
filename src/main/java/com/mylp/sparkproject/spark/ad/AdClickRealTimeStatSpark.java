package com.mylp.sparkproject.spark.ad;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.mylp.sparkproject.conf.ConfigurationManager;
import com.mylp.sparkproject.constant.Constants;
import com.mylp.sparkproject.dao.factory.DAOFactory;
import com.mylp.sparkproject.domain.AdUserClickCount;
import com.mylp.sparkproject.util.DateUtils;

import scala.Tuple2;

public class AdClickRealTimeStatSpark {

	public static void main(String[] args) throws InterruptedException {
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("AdClickRealTimeStatSpark");

		// spark streaming的上下文是构建JavaStreamingContext对象
		// 而不是像之前的JavaSparkContext、SQLContext/HiveContext
		// 传入的第一个参数，和之前的spark上下文一样，也是SparkConf对象；第二个参数则不太一样

		// 第二个参数是spark streaming类型作业比较有特色的一个参数
		// 实时处理batch的interval
		// spark streaming，每隔一小段时间，会去收集一次数据源（kafka）中的数据，做成一个batch
		// 每次都是处理一个batch中的数据

		// 通常来说，batch interval，就是指每隔多少时间收集一次数据源中的数据，然后进行处理
		// 一遍spark streaming的应用，都是设置数秒到数十秒（很少会超过1分钟）

		// 咱们这里项目中，就设置5秒钟的batch interval
		// 每隔5秒钟，咱们的spark streaming作业就会收集最近5秒内的数据源接收过来的数据
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

		// 正式开始进行代码的编写
		// 实现咱们需要的实时计算的业务逻辑和功能

		// 创建针对Kafka数据来源的输入DStream（离线流，代表了一个源源不断的数据来源，抽象）
		// 选用kafka direct api（很多好处，包括自己内部自适应调整每次接收数据量的特性，等等）

		// 构建kafka参数map
		// 主要要放置的就是，你要连接的kafka集群的地址（broker集群的地址列表）
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put(Constants.KAFKA_METADATA_BROKER_LIST,
				ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		
		// 构建topic set
		String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
		String[] kafkaTopicsSplited = kafkaTopics.split(",");
		
		Set<String> topics = new HashSet<String>();
		for (String kafkaTopic : kafkaTopicsSplited) {
			topics.add(kafkaTopic);
		}
		
		// 基于kafka direct api模式，构建出了针对kafka集群中指定topic的输入DStream
		// 两个值，val1，val2；val1没有什么特殊的意义；val2中包含了kafka topic中的一条一条的实时日志数据
		JavaInputDStream<ConsumerRecord<String, String>> adRealTimeLogDStream = KafkaUtils.createDirectStream(
				jssc, LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		
		// 一条一条的实时日志
		// timestamp province city userid adid
		// 某个时间点 某个省份 某个城市 某个用户 某个广告

		// 计算出每5个秒内的数据中，每天每个用户每个广告的点击量

		// 通过对原始实时日志的处理
		// 将日志的格式处理成<yyyyMMdd_userid_adid, 1L>格式
		JavaPairDStream<String, Long> dailyUserAdClickDStream = adRealTimeLogDStream.mapToPair(new PairFunction<ConsumerRecord<String,String>, String, Long>() {
				private static final long serialVersionUID = 1L;
	
				@Override
				public Tuple2<String, Long> call(ConsumerRecord<String, String> consumer) throws Exception {
					// 从tuple中获取到每一条原始的实时日志
					String log = consumer.value();
					String[] logSplited = log.split(" "); 
					
					// 提取出日期（yyyyMMdd）、userid、adid
					String timestamp = logSplited[0];
					Date date = new Date(Long.valueOf(timestamp));
					String datekey = DateUtils.formatDateKey(date);
					
					long userid = Long.valueOf(logSplited[3]);
					long adid = Long.valueOf(logSplited[4]); 
					
					// 拼接key
					String key = datekey + "_" + userid + "_" + adid;
					
					return new Tuple2<String, Long>(key, 1L);  
				}
			});
		
			// 针对处理后的日志格式，执行reduceByKey算子即可
			// （每个batch中）每天每个用户对每个广告的点击量
			JavaPairDStream<String, Long> dailyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey(

					new Function2<Long, Long, Long>() {

						private static final long serialVersionUID = 1L;

						@Override
						public Long call(Long v1, Long v2) throws Exception {
							return v1 + v2;
						}

					});
			
			// 到这里为止，获取到了什么数据呢？
			// dailyUserAdClickCountDStream DStream
			// 源源不断的，每个5s的batch中，当天每个用户对每支广告的点击次数
			// <yyyyMMdd_userid_adid, clickCount>
			
			// 写入mysql数据库
			dailyUserAdClickCountDStream.foreachRDD(new VoidFunction<JavaPairRDD<String,Long>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public void call(JavaPairRDD<String, Long> rdd) throws Exception {
					rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {
						private static final long serialVersionUID = 1L;

						@Override
						public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
							List<AdUserClickCount> adUserClickCounts = new ArrayList<AdUserClickCount>();
							
							while (iterator.hasNext()) {
								Tuple2<String, Long> tuple = iterator.next();
								String[] keySplited = tuple._1.split("_");
								String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
								long userId = Long.valueOf(keySplited[1]);
								long adId = Long.valueOf(keySplited[2]);
								long clickCount = tuple._2;
								
								AdUserClickCount adUserClickCount = new AdUserClickCount();
								adUserClickCount.setDate(date);
								adUserClickCount.setUserId(userId);
								adUserClickCount.setAdId(adId);
								adUserClickCount.setClickCount(clickCount);
								adUserClickCounts.add(adUserClickCount);
							}
							
							DAOFactory.getAdUserClickCountDAO().updateBatch(adUserClickCounts);
						}
					});
				}
			});
			
			// 构建完spark streaming上下文之后，记得要进行上下文的启动、等待执行结束、关闭
			jssc.start();
			jssc.awaitTermination();
			jssc.close();
		}

}
