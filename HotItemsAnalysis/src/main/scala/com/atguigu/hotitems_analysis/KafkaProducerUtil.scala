package com.atguigu.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducerUtil {

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "node01:9092")
    properties.setProperty("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](properties)
    // 从文件读取数据，逐行写入Kafka
    val inputPath = getClass.getResource("/UserBehavior.csv").getPath
    val bufferedSource = io.Source.fromFile(inputPath)
    for (line <- bufferedSource.getLines()) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }

    producer.close()
  }

  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }
}