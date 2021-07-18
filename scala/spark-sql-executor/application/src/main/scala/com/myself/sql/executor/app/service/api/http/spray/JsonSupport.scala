package com.myself.sql.executor.app.service.api.http.spray

import java.sql.Timestamp
import java.time.Instant

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.myself.sql.executor.app.service.api.http.routes.HealthStatus
import spray.json.{DefaultJsonProtocol, JsNumber, JsValue, JsonFormat, RootJsonFormat}

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {

  implicit val timestampJsonFormat: JsonFormat[Timestamp] =
    new JsonFormat[Timestamp] {

      override def read(json: JsValue): Timestamp = json match {

        case JsNumber(value) => Timestamp.from(Instant.ofEpochMilli(value.toLong))
        case _ =>
          throw new IllegalArgumentException(s"Cannot parse JSON value [$json] to timestamp object")
      }

      override def write(obj: Timestamp): JsValue = JsNumber(obj.getTime)
    }

  implicit val healthResponse: RootJsonFormat[HealthStatus] = jsonFormat1(HealthStatus)
}
