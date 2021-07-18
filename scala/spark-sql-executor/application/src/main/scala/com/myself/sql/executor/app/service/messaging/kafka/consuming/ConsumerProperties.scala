package com.myself.sql.executor.app.service.messaging.kafka.consuming

import java.util.Properties

import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.reflect.{ClassTag, classTag}

object ConsumerProperties {

  def apply[K: ClassTag, V: ClassTag](config: Config): Properties = {

    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap-servers"))
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classTag[K].runtimeClass)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classTag[V].runtimeClass)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    // each instance polls only single record
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString("kafka.consumer.group-id"))

    props
  }

}
