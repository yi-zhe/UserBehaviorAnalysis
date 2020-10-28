package com.atguigu.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object LoginFailAdvance {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val resource = getClass.getResource("/LoginLog.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

    // 转换成样例类，并提取时间戳和watermark
    val loginEventStream = inputStream
      .map(data => {
        val arr = data.split(",")
        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(t: LoginEvent): Long = t.timestamp * 1000L
      })

    // 如果2秒内连续登陆失败 输出报警信息

    val loginWarningStream = loginEventStream
      .keyBy(_.userId)
      .process(new LoginFailWarningAdvanceResult())
    loginWarningStream.print()
    env.execute()
  }
}

class LoginFailWarningAdvanceResult() extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {

  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list", classOf[LoginEvent]))

  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, collector: Collector[LoginFailWarning]): Unit = {
    // 判断当前事件类型
    if (i.eventType == "fail") {
      val iter = loginFailListState.get().iterator()
      // 判断之前是否有登录失败事件
      if (iter.hasNext) {
        // 1.1 如果有 进一步判断两次失败的时间差
        val firstFailEvent = iter.next()
        if (i.timestamp < firstFailEvent.timestamp + 2) {
          // 如果两次失败在两秒内
          collector.collect(LoginFailWarning(i.userId, firstFailEvent.timestamp, i.timestamp, "login fail in 2s "))
        }
        // 不管是否报警 当前都已处理完毕 将状态更新为最近一次登录的事件
        loginFailListState.clear()
        loginFailListState.add(i)
      } else {
        // 1.2 如果没有 把当前事件添加到liststate
        loginFailListState.add(i)
      }
    } else {
      loginFailListState.clear()
    }
  }
}
