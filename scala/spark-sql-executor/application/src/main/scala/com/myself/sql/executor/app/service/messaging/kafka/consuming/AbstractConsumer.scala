package com.myself.sql.executor.app.service.messaging.kafka.consuming

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory

abstract class AbstractConsumer[K, V](private val kafkaConsumer: KafkaConsumer[K, V],
                                      val topics: List[String]) {

  val logger = LoggerFactory.getLogger(getClass)

  def getConsumer: KafkaConsumer[K, V] = kafkaConsumer

  def shutdown(): Unit = {

    if (kafkaConsumer != null) kafkaConsumer.close()
  }

  /**
   * Poll messages from queue
   *
   * @return
   */
  def poll(): List[V]

}
