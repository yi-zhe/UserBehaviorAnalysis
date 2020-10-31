package com.atguigu.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)

object TransactionMatch {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val resource1 = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(resource1.getPath)
      .map(data => {
        val arr = data.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.eventType == "pay")
      .keyBy(_.transactionId)

    val resource2 = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream = env.readTextFile(resource2.getPath)
      .map(data => {
        val arr = data.split(",")
        ReceiptEvent(arr(0), arr(1), arr(2).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.txId)

    // 合并两条流
    val resultStream = orderEventStream.connect(receiptEventStream)
      .process(new TxPayMatchResult())

    resultStream.print("matched")
    resultStream.getSideOutput(new OutputTag[OrderEvent]("unmatched-pay")).print("unmatched-1")
    resultStream.getSideOutput(new OutputTag[ReceiptEvent]("unmatched-receipt")).print("unmatched-2")

    env.execute()
  }
}

class TxPayMatchResult() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

  lazy val payEventState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay", classOf[OrderEvent]))
  lazy val receiptEventState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]))

  val unmatchedPayEventOutputTag = new OutputTag[OrderEvent]("unmatched-pay")
  val unmatchedReceiptEventOutputTag = new OutputTag[ReceiptEvent]("unmatched-receipt")

  override def processElement1(pay: OrderEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 订单支付事件来了 判断之前是否有到账事件
    val receipt = receiptEventState.value()
    if (receipt != null) {
      collector.collect((pay, receipt))
      receiptEventState.clear()
      payEventState.clear()
    } else {
      // 还没来 注册定时器
      context.timerService().registerEventTimeTimer(pay.timestamp * 1000 + 5000)
      payEventState.update(pay)
    }
  }

  override def processElement2(receipt: ReceiptEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 订单支付事件来了 判断之前是否有到账事件
    val pay = payEventState.value()
    if (pay != null) {
      collector.collect((pay, receipt))
      receiptEventState.clear()
      payEventState.clear()
    } else {
      // 还没来 注册定时器
      context.timerService().registerEventTimeTimer(receipt.timestamp * 1000 + 3000)
      receiptEventState.update(receipt)
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 定时器触发 判断状态中哪个还存在 就代表另一个没来
    if (payEventState.value() != null) {
      ctx.output(unmatchedPayEventOutputTag, payEventState.value())
    }
    if (receiptEventState.value() != null) {
      ctx.output(unmatchedReceiptEventOutputTag, receiptEventState.value())
    }

    payEventState.clear()
    receiptEventState.clear()
  }
}
