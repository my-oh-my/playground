package com.myself.sql.executor.app.service.messaging.kafka.producer

import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

object HeartbeatService {

  def apply(config: Config): HeartbeatService = {

    val kafkaProducer = new KafkaProducer[String, String](ProducerProperties[StringSerializer, StringSerializer](config))

    // topic to publish messages to
    val topic = config.getString("kafka.producer.topics.heartbeat")

    new HeartbeatService(kafkaProducer, topic)
  }
}

class HeartbeatService(private val kafkaProducer: KafkaProducer[String, String],
                       override val topic: String)
  extends AbstractProducer[String, String] (kafkaProducer, topic){

  override def send(payload: ProducerRecord[String, String]): Unit = {

    kafkaProducer.send(payload, new ProducerCallback)
    kafkaProducer.flush()
  }

  // Callback trait only contains the one abstract method onCompletion
  private class ProducerCallback extends Callback {

    @Override
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {

      if (exception == null) {

        val message = s"[onCompletion] sent message to topic: ${metadata.topic}, " +
          s"partition: ${metadata.partition}, " +
          s"offset: ${metadata.offset}"

        logger.info(message)
      } else {

        exception.printStackTrace()
      }
    }
  }

}