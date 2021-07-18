package com.myself.sql.executor.app.service.api.http.routes

import akka.http.scaladsl.server.Directives
import com.myself.sql.executor.app.service.api.http.spray.JsonSupport

trait SqlExecutorRoute extends Directives with JsonSupport {

}
