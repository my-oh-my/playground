package com.myself.sql.executor.app

import akka.actor.typed.ActorSystem
import com.myself.sql.executor.app.actor.ServerActor
import com.myself.sql.executor.app.model.Arguments
import com.myself.sql.executor.app.util.Utilities
import com.typesafe.config.Config

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object ExecutorAsService {

  def main(args: Array[String]): Unit = {
    // resolver application.conf properties
    val arguments: Arguments = Arguments.parse(args)
    val config: Config = Utilities.resolveConfig(arguments)

    val system = ActorSystem(ServerActor(config), "sql-executor-server")

    val terminatedSystem = system.whenTerminated
    Await.result(terminatedSystem, Duration.Inf)
  }

}
