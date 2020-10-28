package com.atguigu.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailWithCep {
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

    // 1. 定义一个匹配模式要求是一个登录失败事件后 紧跟另一个登录失败事件

    val loginFailPattern =
      Pattern.begin[LoginEvent]("firstFail")
        .where(_.eventType == "fail")
        .next("secondFail")
        .where(_.eventType == "fail")
        .within(Time.seconds(2))

    // 2. 将模式应用到数据流上 得到一个PatternStream
    val patternStream = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)

    // 3. 捡出符合模式的数据流 需要调用select方法
    val loginFailWarningStream = patternStream.select(new LoginFailEventMatch())

    loginFailWarningStream.print()

    env.execute()
  }
}

class LoginFailEventMatch() extends PatternSelectFunction[LoginEvent, LoginFailWarning] {
  override def select(pattern: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {
    // 当前匹配到的事件序列 就保存在Map里
    val firstFailEvent = pattern.get("firstFail").get(0)
    val secondFailEvent = pattern.get("secondFail").iterator().next()
    LoginFailWarning(firstFailEvent.userId, firstFailEvent.timestamp, secondFailEvent.timestamp, "login fail")
  }
}
