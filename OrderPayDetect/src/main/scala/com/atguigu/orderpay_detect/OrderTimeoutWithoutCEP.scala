package com.atguigu.orderpay_detect

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object OrderTimeoutWithoutCEP {
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

    // 自定义ProcessFunction进行复杂事件检测
    val orderResultStream = orderEventStream
      .keyBy(_.orderId)
      .process(new OrderPayMatchResult())

    orderResultStream.print("paid")

    orderResultStream.getSideOutput(new OutputTag[OrderResult]("timeout")).print()

    env.execute()
  }
}

class OrderPayMatchResult() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

  // 定义状态 两个标志位 create pay是否来过 定时器时间戳
  lazy val isCreatedState = getRuntimeContext.getState[Boolean](new ValueStateDescriptor[Boolean]("is-created", classOf[Boolean]))
  lazy val isPaidState = getRuntimeContext.getState[Boolean](new ValueStateDescriptor[Boolean]("is-paid", classOf[Boolean]))
  lazy val timerTsState = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  // 侧输出流标签
  val orderTimeoutOutputTag = new OutputTag[OrderResult]("timeout")

  override def processElement(value: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, collector: Collector[OrderResult]): Unit = {
    // 先拿到当前状态
    val isPaid = isPaidState.value()
    val isCreated = isCreatedState.value()
    val timerTs = timerTsState.value()

    // 1. 来了create 判断是否pay过
    if (value.eventType == "create") {
      // 1.1 已经支付过 匹配成功 正常支付 输出匹配成功的结果
      if (isPaid) {
        collector.collect(OrderResult(value.orderId, "paid successfully"))
        // 处理完毕 清空状态和定时器
        isCreatedState.clear()
        isPaidState.clear()
        timerTsState.clear()
        context.timerService().deleteEventTimeTimer(timerTs)
      } else {
        // 1.2 没有pay 注册定时器等待15分钟
        val ts = value.timestamp * 1000L + 900 * 1000L
        context.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
        isCreatedState.update(true)
      }
    }
    // 当前来的是pay
    else if (value.eventType == "pay") {
      // 2.1 如果create过 匹配成功 还要判断是否超时
      if (isCreated) {
        if (value.timestamp * 1000L < timerTs) {
          // 2.1.1 没有超时
          collector.collect(OrderResult(value.orderId, "paid successfully"))
        } else {
          context.output(orderTimeoutOutputTag, OrderResult(value.orderId, "paid but timeout"))
        }

        // 处理结束清空状态定时器
        isCreatedState.clear()
        isPaidState.clear()
        timerTsState.clear()
        context.timerService().deleteEventTimeTimer(timerTs)
      } else {
        // 2.2 如果create没来，注册定时器，等到pay的时间就可以
        context.timerService().registerEventTimeTimer(value.timestamp * 1000L)
        // 更新状态
        timerTsState.update(value.timestamp * 1000L)
        isPaidState.update(true)
      }
    }
  }

  override def onTimer(timestamp: Long, context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {

    // 定时器触发
    // 1. pay来了，没等到create
    if (isPaidState.value()) {
      context.output(orderTimeoutOutputTag, OrderResult(context.getCurrentKey, "payed but not found create log"))
    } else {
      // 2. create来了，没有pay
      context.output(orderTimeoutOutputTag, OrderResult(context.getCurrentKey, "order timeout"))
    }
    // 清空状态
    isCreatedState.clear()
    isPaidState.clear()
    timerTsState.clear()
  }
}
