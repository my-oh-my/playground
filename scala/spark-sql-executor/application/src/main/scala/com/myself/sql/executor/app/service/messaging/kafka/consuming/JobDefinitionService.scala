package com.myself.sql.executor.app.service.messaging.kafka.consuming

import java.time.Duration

import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._

object JobDefinitionService {

  def apply(config: Config): JobDefinitionService = {

    val kafkaConsumer: KafkaConsumer[String, String] =
      new KafkaConsumer[String, String](ConsumerProperties[StringDeserializer, StringDeserializer](config))

    // subscribe Consumer to topics
    val topics: java.util.List[String] = config.getStringList("kafka.consumer.topics.job-definition")
    kafkaConsumer.subscribe(topics)

    new JobDefinitionService(kafkaConsumer, topics.asScala.toList)
  }

}

class JobDefinitionService(private val kafkaConsumer: KafkaConsumer[String, String],
                           override val topics: List[String])
  extends AbstractConsumer[String, String](kafkaConsumer, topics) {

  override def poll(): List[String] = {

    val pollTimeout = Duration.ofSeconds(1L)
    val records = kafkaConsumer.poll(pollTimeout).asScala

    records.map(record => record.value()).toList
  }

}