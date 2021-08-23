package com.myself.sql.executor.app.actor

import java.util.concurrent.TimeUnit

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{Behavior, PostStop}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import com.myself.sql.executor.app.service.api.RestEndPoints
import com.myself.sql.executor.app.service.messaging.kafka.consuming.JobDefinitionService
import com.myself.sql.executor.app.service.messaging.kafka.producer.HeartbeatService
import com.myself.sql.executor.app.service.spark.SparkSessionWrapper
import com.typesafe.config.Config

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

object ServerActor {

  sealed trait Command

  private final case class StartFailed(cause: Throwable) extends Command
  private final case class Started(serverBinding: ServerBinding) extends Command

  final case object Stop extends Command

  def apply(config: Config): Behavior[Command] = {
    Behaviors.setup { context =>

      implicit val system = context.system
      implicit val ex = system.executionContext

      val routes = RestEndPoints.bindRoutes(config)

      val serverBinding: Future[Http.ServerBinding] =
        Http().newServerAt(config.getString("host"), config.getInt("port")).bind(routes)

      context.pipeToSelf(serverBinding) {
        case Success(binding) =>
          Started(binding)
        case Failure(ex) =>
          StartFailed(ex)
      }

      // start processing
      startingServer(config, wasStopped = false)
    }
  }

  private def startProcessing(context: ActorContext[_], config: Config): Unit = {

    // spawn SqlExecutor actor
    //  initialize SparkSession
    implicit val sparkSession = SparkSessionWrapper()
    val sqlExecutorActor = context.spawn(SqlExecutorActor(config), "sql-executor")

    // spawn JobDefinition actor
    //  JobDefinition Consumer service
    val jobDefinitionService = JobDefinitionService(config)
    val jobDefinitionConsumerActor = context.spawn(JobDefinitionConsumerActor(sqlExecutorActor, jobDefinitionService), "job-definition")

    jobDefinitionConsumerActor ! JobDefinitionConsumerActor.Consume

    // spawn Heartbeat actor
    val heartbeatInterval = config.getInt("sql-executor.heartbeat.interval.ms")
    val heartbeatService = HeartbeatService(config)
    val heartbeatActor = context.spawn(HeartbeatProducerActor(config, heartbeatService), "heartbeat")

    heartbeatActor ! HeartbeatProducerActor.Start(FiniteDuration(heartbeatInterval, TimeUnit.MILLISECONDS))
  }

  private def runningServer(binding: ServerBinding): Behavior[Command] = {

    Behaviors.receivePartial[Command] {

      case (context, Stop) =>
        context.log.info(s"Stopping server at http://${binding.localAddress.getHostString}:${binding.localAddress.getPort}/")

        Behaviors.stopped
    }.receiveSignal {
      case (_, PostStop) =>
        binding.unbind()

        Behaviors.same
    }

  }

  def startingServer(config: Config, wasStopped: Boolean): Behaviors.Receive[Command] =
    Behaviors.receive[Command] {

      case (_, StartFailed(exception)) =>
        throw new RuntimeException("Server failed to start" , exception)

      case (context, Started(binding)) =>
        context.log.info(s"Server runningServer at http://${binding.localAddress.getHostString}:${binding.localAddress.getPort}/")
        if (wasStopped) {
          context.self ! Stop
        } else startProcessing(context, config)

        runningServer(binding)
      case (_, Stop) =>
        startingServer(config, wasStopped = true)
    }

}
