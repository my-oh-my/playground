package com.myself.sql.executor.app.actor

import java.sql.Timestamp
import java.util.Calendar

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorSystem, Behavior}
import com.myself.sql.executor.app.service.messaging.kafka.producer.AbstractProducer
import com.myself.sql.executor.app.util.JsonUtil
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}

object HeartbeatProducerActor {

  sealed trait Command

  final case class Start(heartbeatInterval: FiniteDuration) extends Command

  final case class Heartbeat(serviceAddress: String, timestamp: Timestamp, message: String = "heartbeat") extends Command

  def send(messageProducer: AbstractProducer[String, String], payload: String): Unit = {

    val record = new ProducerRecord[String, String](messageProducer.topic, payload)

    messageProducer.send(record)
  }

  def apply(config: Config, producer: AbstractProducer[String, String]): Behavior[Command] =
    Behaviors.setup(context => new HeartbeatProducerActor(context, config, producer))

}

class HeartbeatProducerActor(context: ActorContext[HeartbeatProducerActor.Command],
                             config: Config,
                             producer: AbstractProducer[String, String])
  extends AbstractBehavior[HeartbeatProducerActor.Command](context) {

  import HeartbeatProducerActor._

  val address = s"${config.getString("sql-executor.host")}:${config.getInt("sql-executor.port")}"

  implicit val system: ActorSystem[_] = context.system
  implicit val ex: ExecutionContextExecutor = system.executionContext

  override def onMessage(message: Command): Behavior[Command] = {

    message match {
      case Start(heartbeatInterval) =>

        system.scheduler.scheduleWithFixedDelay(0 seconds, heartbeatInterval) {

          new Runnable {
            override def run(): Unit = {

              val timestamp = new Timestamp(Calendar.getInstance().getTimeInMillis)

              val payload = JsonUtil.toJson(Heartbeat(address, timestamp))

              send(producer, payload)
            }
          }
        }

        Behaviors.same
    }
  }

}
