package com.myself.sql.executor.app.service.messaging.kafka.producer

import java.util.Properties

import com.typesafe.config.Config
import org.apache.kafka.clients.producer.ProducerConfig

import scala.reflect.{ClassTag, classTag}

object ProducerProperties {

  def apply[K: ClassTag, V: ClassTag](config: Config): Properties = {

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap-servers"))
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classTag[K].runtimeClass)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classTag[V].runtimeClass)

    props
  }

}

