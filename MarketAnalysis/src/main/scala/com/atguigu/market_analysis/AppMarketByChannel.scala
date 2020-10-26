package com.atguigu.market_analysis

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

// 输入数据样例类
case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

case class MarketViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

// 自定义测试数据源
class SimulatedSource() extends RichSourceFunction[MarketUserBehavior] {

  var running = true

  // 定义用户行为和渠道的集合

  val behaviorSet: Seq[String] = Seq("view", "download", "install", "uninstall")
  val channelSet: Seq[String] = Seq("appstore", "weibo", "wechat", "tieba")
  val rand: Random = Random

  override def run(sourceContext: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
    // 定义一个生成数据最大的数量
    val maxCounts = Long.MaxValue
    var count = 0L

    while (count < maxCounts && running) {
      val id = UUID.randomUUID().toString
      val behavior = behaviorSet(rand.nextInt(behaviorSet.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()

      sourceContext.collect(MarketUserBehavior(id, behavior, channel, ts))

      count += 1
      Thread.sleep(50)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}

object AppMarketByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource(new SimulatedSource())
      .assignAscendingTimestamps(_.timestamp)

    // 开窗统计输出
    val resultStream = dataStream
      .filter(_.behavior != "uninstall")
      .keyBy(data => (data.channel, data.behavior)) // 按渠道和行为分别分组
      .timeWindow(Time.hours(1), Time.seconds(5))
      .process(new MarketCountByChannel())
      .print()


    env.execute()
  }
}

class MarketCountByChannel() extends ProcessWindowFunction[MarketUserBehavior, MarketViewCount, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketViewCount]): Unit = {
    val start = new Timestamp(context.window.getStart).toString
    val end = new Timestamp(context.window.getEnd).toString
    val channel = key._1
    val behavior = key._2
    val count = elements.size
    out.collect(MarketViewCount(start, end, channel, behavior, count))
  }
}
