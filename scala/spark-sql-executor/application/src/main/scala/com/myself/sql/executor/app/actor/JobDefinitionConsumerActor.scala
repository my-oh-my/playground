package com.myself.sql.executor.app.actor

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import com.myself.sql.executor.app.model.JobRequest
import com.myself.sql.executor.app.service.messaging.kafka.consuming.AbstractConsumer
import com.myself.sql.executor.app.util.JsonUtil

import scala.concurrent.ExecutionContextExecutor
import scala.language.{implicitConversions, postfixOps}

object JobDefinitionConsumerActor {

  sealed trait Command

  final case object Consume extends Command

  def consumeJobDefinitions(consumer: AbstractConsumer[String, String]): List[JobRequest] = {

    consumer.poll().map(message => JsonUtil.fromJson[JobRequest](message))
  }

  def apply(sendTo: ActorRef[SqlExecutorActor.Message], consumer: AbstractConsumer[String, String]): Behavior[Command] =
    Behaviors.setup(context => new JobDefinitionConsumerActor(context, sendTo, consumer))

}

class JobDefinitionConsumerActor(context: ActorContext[JobDefinitionConsumerActor.Command],
                                 sendTo: ActorRef[SqlExecutorActor.Message],
                                 consumer: AbstractConsumer[String, String])
  extends AbstractBehavior[JobDefinitionConsumerActor.Command](context) {

  import JobDefinitionConsumerActor._

  implicit val system: ActorSystem[_] = context.system
  implicit val ex: ExecutionContextExecutor = system.executionContext

  override def onMessage(message: Command): Behavior[Command] = {

    message match {
      case Consume =>
        // poll JobDefinition messages and execute through SqlExecutorActor
        consumeJobDefinitions(consumer).foreach(jobDefinition =>

          sendTo ! SqlExecutorActor.Execute(context.self, jobDefinition)
        )

        Behaviors.same
    }
  }

}
