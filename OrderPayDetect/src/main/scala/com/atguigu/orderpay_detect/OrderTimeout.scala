package com.atguigu.orderpay_detect


import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

case class OrderEvent(orderId: Long, eventType: String, transactionId: String, timestamp: Long)

case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val resource = getClass.getResource("/OrderLog.csv")

    // 转换成样例类，并提取时间戳和watermark
    val orderEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val arr = data.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.orderId)

    // 1. 定义pattern

    val orderPayPattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 2. Pattern应用到流上
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    // 3. 定义侧输出流标签用于处理超时事件
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

    // 4. 调用select 提取并处理匹配成功支付事件和超时事件

    val resultStream = patternStream.select(orderTimeoutOutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect())

    resultStream.print() // 成功支付的订单

    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute()
  }
}

class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeoutOrderId = map.get("create").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout:" + l)
  }
}

class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = map.get("pay").iterator().next().orderId
    OrderResult(payedOrderId, "payed")
  }
}
