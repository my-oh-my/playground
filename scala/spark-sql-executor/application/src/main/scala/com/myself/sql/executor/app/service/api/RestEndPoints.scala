package com.myself.sql.executor.app.service.api

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import com.myself.sql.executor.app.service.api.http.routes.HealthRoute
import com.typesafe.config.Config

object RestEndPoints {

  def bindRoutes(config: Config)(implicit system: ActorSystem[_]): Route = {

    handleExceptions(exceptionHandler) {

      concat(
        pathPrefix("service"){
          concat(
            pathPrefix("v1") {
              concat(HealthRoute.route)
            }
          )
        }
      )
    }
  }

  private def exceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case ex: Exception =>
        complete {
          HttpResponse(
            StatusCodes.Unauthorized,
            entity = HttpEntity(
              ContentTypes.`application/json`,
              s"""{"error":"invalid_request", "error_description":"$ex"}"""
            )
          )
        }
    }

}
