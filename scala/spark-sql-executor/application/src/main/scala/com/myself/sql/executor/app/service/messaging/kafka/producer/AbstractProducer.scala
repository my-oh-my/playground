package com.myself.sql.executor.app.service.messaging.kafka.producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory

abstract class AbstractProducer[K, V](private val kafkaProducer: KafkaProducer[K, V],
                                      val topic: String) {

  val logger = LoggerFactory.getLogger(getClass)

  def getProducer: KafkaProducer[K, V] = kafkaProducer

  def shutdown(): Unit = {

    if (kafkaProducer != null) kafkaProducer.close()
  }

  def send(payload: ProducerRecord[K, V]): Unit

}
