package com.myself.sql.executor.app.service.api.http.routes

import akka.http.scaladsl.server.Route

final case class HealthStatus(status: String)

object HealthRoute extends SqlExecutorRoute {

  def route: Route =
      path("health") {
        complete(HealthStatus("UP"))
      }

}
