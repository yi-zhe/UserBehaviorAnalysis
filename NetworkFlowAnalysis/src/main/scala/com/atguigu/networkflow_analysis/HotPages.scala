package com.atguigu.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

// 定义输入数据样例类
case class ApacheLogEvent(ip: String, userId: String, timestamp: Long, method: String, url: String)

// 定义窗口聚合结果样例类
case class PageViewCount(url: String, windowEnd: Long, count: Long)

object HotPages {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据转换成样例并提取时间戳和watermark
    val inputPath = getClass.getResource("/apache.log").getPath
    //    val inputStream = env.readTextFile(inputPath)
    val inputStream = env.socketTextStream("node01", 7777)
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(" ")
        // 对事件时间转换得到时间戳
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val ts = simpleDateFormat.parse(arr(3)).getTime
        ApacheLogEvent(arr(0), arr(1), ts, arr(5), arr(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = t.timestamp
      })

    // 开窗聚合，以及排序输出
    val aggStream = dataStream
      .filter(_.method == "GET")
      .filter(data => {
        val pattern = "^((?!\\.(css|js)$).)*$".r
        (pattern findFirstIn data.url).nonEmpty
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      // 允许延迟1min
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
      .aggregate(new PageCountAgg(), new PageViewCountWindowResult())

    val resultStream = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotPages(3))


    dataStream.print("data")
    aggStream.print("agg")
    // 读侧输出流
    aggStream.getSideOutput(new OutputTag[ApacheLogEvent]("late")).print("late")

    resultStream.print()

    env.execute()
  }
}


// 自定义预聚合函数
class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0

  // 每来一条数据调用一次add，count加1
  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数

class PageViewCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNHotPages(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {

  //  lazy val pageViewCountListState: ListState[PageViewCount] =
  //    getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pageViewCount-list", classOf[PageViewCount]))

  lazy val pageViewCountMapState: MapState[String, Long] =
    getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageViewCount-map", classOf[String], classOf[Long]))

  override def processElement(i: PageViewCount, context: KeyedProcessFunction[Long, PageViewCount, String]#Context, collector: Collector[String]): Unit = {
    //    pageViewCountListState.add(i)
    pageViewCountMapState.put(i.url, i.count)
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
    // 注册一个定时器 1分钟后触发 这时候窗口已经彻底关闭 可以清空状态
    context.timerService().registerEventTimeTimer(i.windowEnd + 60 * 1000L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //    val allPageViewCounts: ListBuffer[PageViewCount] = ListBuffer()
    /*val iter = pageViewCountListState.get.iterator()
    while (iter.hasNext) {
      allPageViewCounts += iter.next()
    }
    pageViewCountListState.clear()*/

    // 判断定时器触发时间，如果已经是窗口结束时间1分钟后，那么直接清空状态
    if (timestamp == ctx.getCurrentKey + 60 * 1000L) {
      pageViewCountMapState.clear()
      return
    }

    val allPageViewCounts: ListBuffer[(String, Long)] = ListBuffer()
    val iter = pageViewCountMapState.entries().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      allPageViewCounts += ((entry.getKey, entry.getValue))
    }

    // 按照访问量排序
    val sortedPageViewCounts = allPageViewCounts.sortWith(_._2 > _._2).take(n)

    // 将排名信息格式化成String
    val result: StringBuilder = new StringBuilder()
    result.append("窗口结束时间: ").append(new Timestamp(timestamp - 1)).append("\n")
    // 遍历结果列表中的每个ItemViewCount，输出到一行
    for (i <- sortedPageViewCounts.indices) {
      val currentItemViewCount = sortedPageViewCounts(i)
      result.append("NO").append(i + 1).append(": ")
        .append(" 页面URL=").append(currentItemViewCount._1)
        .append(" 热门度=").append(currentItemViewCount._2).append("\n")
    }
    result.append("==================================\n")
    Thread.sleep(200)
    out.collect(result.toString())
  }
}

/**
 *83.149.9.216 - - 17/05/2015:10:25:49 +0000 GET /presentations/
 *83.149.9.216 - - 17/05/2015:10:25:50 +0000 GET /presentations/
 *83.149.9.216 - - 17/05/2015:10:25:51 +0000 GET /presentations/
 *83.149.9.216 - - 17/05/2015:10:25:52 +0000 GET /presentations/
 *83.149.9.216 - - 17/05/2015:10:25:46 +0000 GET /presentations/
 *83.149.9.216 - - 17/05/2015:10:25:31 +0000 GET /presentations/
 *83.149.9.216 - - 17/05/2015:10:25:53 +0000 GET /presentations/
 *83.149.9.216 - - 17/05/2015:10:25:23 +0000 GET /presentations/
 *83.149.9.216 - - 17/05/2015:10:25:54 +0000 GET /presentations/
 */
