package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
// 定义输出UV统计样例类

case class UvCount(windowEnd: Long, count: Long)

object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val uvStream = dataStream
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1)) // 不分组 基于DataStream开窗
      .apply(new UvCountResult())

    uvStream.print()
    env.execute()
  }
}

class UvCountResult() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    // 定义一个Set 保存所有的userId
    var userIdSet = Set[Long]()
    // 遍历窗口中的所有数据，把userId添加到set中，自动去重
    for (userBehavior <- input) {
      userIdSet += userBehavior.userId
    }

    // 将set的size做为去重后的uv值输出
    out.collect(UvCount(window.getEnd, userIdSet.size))
  }
}
