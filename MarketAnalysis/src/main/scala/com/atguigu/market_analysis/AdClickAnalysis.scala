package com.atguigu.market_analysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class AdClickCountByProvince(windowEnd: String, province: String, count: Long)

case class BlackListUserWarning(userId: Long, adId: Long, msg: String)

object AdClickAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/AdClickLog.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

    // 转换成样例类，并提取时间戳和watermark
    val adLogStream = inputStream
      .map( data => {
        val arr = data.split(",")
        AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 插入一步过滤操作，并将有刷单行为的用户输出到侧输出流（黑名单报警）
    val filterBlackListUserStream: DataStream[AdClickLog] = adLogStream
      .keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUserResult(100))

    val adCountResultStream = filterBlackListUserStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountWindowResult())
    adCountResultStream.print("count result")
    filterBlackListUserStream
      .getSideOutput(new OutputTag[BlackListUserWarning]("warning"))
      .print("warning")
    env.execute()
  }
}

class FilterBlackListUserResult(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {
  // 广告的点击量 每天0点定时清空状态的时间戳 标记当前用户是否已经进入黑名单
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
  lazy val resetTimerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-ts", classOf[Long]))
  lazy val isBlackState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-black", classOf[Boolean]))

  override def processElement(i: AdClickLog, context: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, collector: Collector[AdClickLog]): Unit = {
    val currentCount = countState.value()

    // 判断只要是第一个数据
    if (currentCount == 0) {
      val millisOfDay = 1000 * 60 * 60 * 24
      val millisOfHour = 1000 * 60 * 60
      val ts = (context.timerService().currentProcessingTime() / millisOfDay + 1) * millisOfDay - 8 * millisOfHour
      resetTimerTsState.update(ts)
      context.timerService().registerProcessingTimeTimer(ts)
    }

    // 判断count是否达到阈值 如果达到则输出到黑名单
    if (currentCount >= maxCount) {
      // 判断是否已经在黑名单中 没有的才输出到侧输出流
      if (!isBlackState.value()) {
        isBlackState.update(true)
        context.output(new OutputTag[BlackListUserWarning]("warning"), BlackListUserWarning(i.userId, i.adId, "Click ad over " + maxCount + " times today"))
      }
      return
    }
    // 正常情况 count 加1 数据原样输出
    countState.update(currentCount + 1)
    collector.collect(i)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    if (timestamp == resetTimerTsState.value()) {
      isBlackState.clear()
      countState.clear()
    }
  }
}

class AdCountAgg() extends AggregateFunction[AdClickLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickLog, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class AdCountWindowResult() extends WindowFunction[Long, AdClickCountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdClickCountByProvince]): Unit = {
    val end = new Timestamp(window.getEnd).toString
    out.collect(AdClickCountByProvince(end, key, input.head))
  }
}
