package com.atguigu.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.common.constant.ZmallConstant
import com.atguigu.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object NewOrderApp {
	def main(args: Array[String]): Unit = {
		val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("realtime-newOrder")
		val sc = new SparkContext(sparkConf)
		val ssc = new StreamingContext(sc,Seconds(5))
		val startupStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(ZmallConstant.KAFKA_TOPIC_STARTUP,ssc)
		val newOrderDstream: DStream[JSONObject] = startupStream.map { record =>
			val jsonString: String = record.value()
			val orderInfo: JSONObject = JSON.parseObject(jsonString)
			orderInfo
		}
		newOrderDstream

		ssc.start()
		ssc.awaitTermination()
	}
}
