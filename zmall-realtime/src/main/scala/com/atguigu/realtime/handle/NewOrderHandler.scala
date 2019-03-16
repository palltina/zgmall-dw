package com.atguigu.realtime.handle

import com.atguigu.realtime.bean.StartUpLog
import org.apache.spark.streaming.dstream.DStream

object NewOrderHandler {
	def handleOrder(newOrderDstream: DStream[StartUpLog])={

	}
}
