package com.myself.sql.executor.app.actor

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.myself.sql.executor.app.ExecutorApp
import com.myself.sql.executor.app.model.{JobDefinition, JobRequest}
import com.myself.sql.executor.app.util.{JsonUtil, Utilities}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object SqlExecutorActor {

  sealed trait Message

  final case class Execute(responseTo: ActorRef[JobDefinitionConsumerActor.Command],
                           jobDefinition: JobRequest)
    extends Message

  private val log = LoggerFactory.getLogger(this.getClass())

  def execute(jobRequest: JobRequest, config: Config)
             (implicit sparkSession: SparkSession): Option[DataFrame] = {

    // collect JobDefinition
    val jobDefinition = JsonUtil.fromJson[JobDefinition](
      Utilities.decompress(jobRequest.compressionType.get, jobRequest.jobDefinition)
    )
    // recreate SparkSession each time new JobRequest is processed
    //  or reuse existing one
    val sparkConfMap = jobDefinition.sparkConfMap
    implicit val currentSparkSession: SparkSession = if (sparkConfMap.isDefined) {
      // apply new settings
      sparkSession.sparkContext.getConf.setAll(sparkConfMap.get)

      sparkSession
    } // else - return existing one
    else sparkSession

    // call main execution class
    val sqlExecutor = new ExecutorApp(config)(currentSparkSession)

    Try {
      Utilities.measureJob[(SparkSession, JobDefinition, DataFrame)](sqlExecutor.execute(jobDefinition))
    } match {
      case Success(result) => {

        log.info(s"JobDefinition processing took: ${result._2}")

        // return result DataFrame
        Some(result._1._3)
      }
      case Failure(exception) => {

        log.error(s"Exception when processing JobDefinition: $exception")

        None
      }
    }
  }

  def apply(config: Config)
           (implicit sparkSession: SparkSession): Behavior[Message] =
    Behaviors.setup(context => new SqlExecutorActor(context, config))

}

class SqlExecutorActor(context: ActorContext[SqlExecutorActor.Message],
                       config: Config)
                      (implicit sparkSession: SparkSession)
  extends AbstractBehavior[SqlExecutorActor.Message](context) {

  import SqlExecutorActor._

  override def onMessage(message: Message): Behavior[Message] = {

    message match {
      case Execute(from, jobDefinition) =>

        // process JobDefinition execution
        //  execute returns Option - this can further processed
        execute(jobDefinition, config)

        // order next message consumption
        from ! JobDefinitionConsumerActor.Consume

        Behaviors.same
    }
  }

}
