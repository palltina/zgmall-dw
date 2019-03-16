package com.atguigu.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.common.constant.ZmallConstant
import com.atguigu.realtime.bean.StartUpLog
import com.atguigu.realtime.util.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object StartupApp {
	def main(args: Array[String]): Unit = {
		val sparkConf: SparkConf = new SparkConf().setAppName("zmall-realtime-startup").setMaster("local[*]")
		val sc = new SparkContext(sparkConf)
		val ssc = new StreamingContext(sc,Seconds(5))
		val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(ZmallConstant.KAFKA_TOPIC_STARTUP,ssc)

		val startupDstream: DStream[StartUpLog] = recordDstream.map { record =>
			val string: String = record.value()
			val startupLog = JSON.parseObject(string, classOf[StartUpLog])
			val formatDateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm")
			val logDateTime: String = formatDateTime.format(new Date(startupLog.ts))
			startupLog.logDate = logDateTime.split(" ")(0)
			startupLog.logHour = logDateTime.split(" ")(1).split(":")(0)
			startupLog.logHourMinute = logDateTime.split(" ")(1)
			startupLog
		}
		val filteredStartupLogDstream: DStream[StartUpLog] = startupDstream.transform { rdd =>
			val jedis: Jedis = RedisUtil.getJedisClient
			val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
			val today: String = dateFormat.format(new Date())
			val dauSet: util.Set[String] = jedis.smembers("dau" + today)
			val dauBC: Broadcast[util.Set[String]] = sc.broadcast(dauSet)
			val filtedRDD: RDD[StartUpLog] = rdd.filter { startupLog =>
				var isExist = false
				if (dauBC.value != null) {
					isExist = dauBC.value.contains(startupLog.mid)
				}
				!isExist
			}
			filtedRDD
		}
		filteredStartupLogDstream.foreachRDD{rdd =>
			rdd.foreachPartition{ startupItr =>
				val jedis = RedisUtil.getJedisClient
				val list = new ListBuffer[Any]()
				for (startuplog <- startupItr) {
					val dauKey: String = "dau" + startuplog.logDate
					jedis.sadd(dauKey,startuplog.mid)
					list+=startuplog
				}
				jedis.close()
				MyEsUtil.insertBulk(ZmallConstant.ES_INDEX_DAU,list.toList)

			}

		}
		ssc.start()
		ssc.awaitTermination()
	}
}
