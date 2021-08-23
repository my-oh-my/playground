package com.myself.sql.executor.app.util

import com.myself.sql.executor.app.model.DatasetSchemaJson
import org.apache.spark.sql.types.{DataType, StructType}

import scala.util.{Failure, Success, Try}

object SchemaConverter {

  def convertFromJson(schemaJson: String): StructType = {

    Try {
      DataType.fromJson(schemaJson).asInstanceOf[StructType]
    } match {
      case Success(schema) => schema
      case Failure(exception) => {

        throw new RuntimeException(s"Error when converting schema from JSON, ${exception.getMessage}")
      }
    }
  }

  def convertSchema(schemaJson: Option[DatasetSchemaJson], schemaPath: Option[String]): Option[StructType] = {

    if (schemaJson.isDefined) {

      Some(
        convertFromJson(
          Utilities.decompress(
            schemaJson.get.text,
            schemaJson.get.compressionType
          )
        )
      )
    }
    else if (schemaPath.isDefined) {

      val (resource, iterator) = Utilities.readFromFile(schemaPath.get)
      val schemaFromFile = iterator.mkString("\n")

      // explicitly close resource
      resource.close()

      Some(convertFromJson(schemaFromFile))
    } else None
  }

}
