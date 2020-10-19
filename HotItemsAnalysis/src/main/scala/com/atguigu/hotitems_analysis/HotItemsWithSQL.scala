package com.atguigu.hotitems_analysis

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.api.{EnvironmentSettings, Slide}
import org.apache.flink.types.Row

object HotItemsWithSQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从Kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "node01:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")

    val inputStream = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))

    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    val dataTable = tableEnv.fromDataStream(dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)
    // 1. Table api进行开窗聚合统计
    val aggTable = dataTable
      .filter('behavior === "pv")
      .window(Slide over 1.hours every 5.minutes on 'ts as 'sw)
      .groupBy('itemId, 'sw)
      .select('itemId, 'sw.end as 'windowEnd, 'itemId.count as 'cnt)

    // 用SQL去实现TopN的选取
    tableEnv.createTemporaryView("aggTable", aggTable, 'itemId, 'windowEnd, 'cnt)
    val resultTable = tableEnv.sqlQuery(
      """
        |SELECT *
        |FROM (
        |  SELECT *, row_number()
        |    OVER(PARTITION BY windowEnd ORDER BY cnt DESC)
        |    AS row_num
        |  FROM aggTable
        |)
        |WHERE row_num <= 5
        |""".stripMargin
    )

    // 纯SQL实现
    tableEnv.createTemporaryView("dataTable", dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)
    val resultSQLTable = tableEnv.sqlQuery(
      """
        |SELECT *
        |FROM (
        |  SELECT
        |    *,
        |    row_number()
        |      OVER (PARTITION BY windowEnd ORDER BY cnt DESC)
        |      AS row_num
        |  FROM (
        |     SELECT
        |       itemId,
        |       HOP_END(ts, interval '5' MINUTE, interval '1' HOUR) AS windowEnd,
        |       COUNT(itemId) AS cnt
        |     FROM dataTable
        |     WHERE behavior = 'pv'
        |     GROUP BY
        |       itemId,
        |       HOP(ts, interval '5' MINUTE, interval '1' HOUR)
        |  )
        |)
        |WHERE row_num <= 5
        |""".stripMargin)


    resultSQLTable.toRetractStream[Row].print()

    env.execute("HotItemsSQL")
  }
}

