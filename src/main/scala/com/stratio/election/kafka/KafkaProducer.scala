package com.stratio.election.kafka

import java.util.Properties

import com.typesafe.config.Config
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}

object KafkaProducer {

  def getInstance(config: Config): Producer[String, String] = {
    val properties: Properties = new Properties()
    properties.put("metadata.broker.list", config.getString("brokerList"))
    properties.put("serializer.class", "kafka.serializer.StringEncoder")
    properties.put("request.required.acks", "1")

    val producerConfig = new ProducerConfig(properties)
    new Producer[String, String](producerConfig)
  }

  def put(producer: Producer[String,String], topic: String, message: String): Unit = {
    val keyedMessage: KeyedMessage[String, String] = new KeyedMessage[String, String](topic, message)
    producer.send(keyedMessage)
  }
}
