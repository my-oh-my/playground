package com.myself.sql.executor.app

import com.myself.sql.executor.app.model.{Arguments, InputDataInfo, JobDefinition}
import com.myself.sql.executor.app.service.spark.reading.{CsvReader, DataReader, ParquetReader}
import com.myself.sql.executor.app.service.spark.writing.DataWriter
import com.myself.sql.executor.app.service.spark.{SparkSessionWrapper, SqlExecutor}
import com.myself.sql.executor.app.util.{JsonUtil, SchemaConverter, Utilities}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object ExecutorApp {

  private val log = LoggerFactory.getLogger(this.getClass())

  def decideOnReader(format: String,
                     formatOptions: Option[Map[String, String]] = None,
                     schema: Option[StructType] = None)
                    (implicit sparkSession: SparkSession): DataReader = {

    format match {
      case "csv" => CsvReader(formatOptions, schema)
      case "parquet" => ParquetReader(formatOptions, schema)
    }
  }

  def processInputData(inputDataInfo: InputDataInfo)(implicit sparkSession: SparkSession): String = {

    // get Spark schema
    val schema: Option[StructType] =
      SchemaConverter.convertSchema(
        inputDataInfo.dataSetSchemaJson,
        inputDataInfo.dataSetSchemaPath
      )

    // input DataFrame
    val dataFrameReader = decideOnReader(
      inputDataInfo.format,
      inputDataInfo.formatOptions,
      schema
    )
    val inputDF = dataFrameReader.readDataFrame(inputDataInfo.path)

    // register temporary view to SparkContext
    val temporaryViewAlias = inputDataInfo.dataAlias

    inputDF.createOrReplaceTempView(temporaryViewAlias)

    temporaryViewAlias
  }

  // start standard mode execution
  def main(args: Array[String]): Unit = {

    // resolve configuration
    val config: Config = ConfigFactory.load()

    // parse arguments
    val arguments = Arguments.parse(args)
    val jobDefinitionStr: String = arguments.getJobDefinition
    require(
      jobDefinitionStr != null,
      "Exception when starting standard execution mode, -jobDefinition was not provided"
    )

    val compressionType: Option[String] = Option(arguments.getCompressionType)

    val jobDefinition: JobDefinition = JsonUtil.fromJson[JobDefinition] {
      Utilities.decompress(jobDefinitionStr, compressionType)
    }

    implicit val sparkSession: SparkSession = SparkSessionWrapper(jobDefinition.sparkConfMap)

    val sqlExecutor = new ExecutorApp(config)

    Try {
      Utilities.measureJob(sqlExecutor.execute(jobDefinition))
    } match {
      case Success(result) => {
        // explicitly close SparkSession
        result._1._1.close()

        log.info(s"Job processing took: ${result._2}")
      }
      case Failure(ex) => {}
    }
  }

}

/**
 * Main processing class
 * logic is wrapped when used with long running service
 *
 * @param config
 * @param sparkSession
 */
class ExecutorApp(config: Config)(implicit sparkSession: SparkSession) {

  import ExecutorApp._

  def execute(jobDefinition: JobDefinition): (SparkSession, JobDefinition, DataFrame) = {

    // iterate over JobDefinition.inputsDataInfo list
    //  register provided data sets to Spark Context as temporary views
    jobDefinition.inputsDataInfo
      .foreach(_ => log.info(s"Registered temporary view: ${processInputData(_)}"))

    // execute provide compressed SQL
    val resultDF = SqlExecutor().execute(
      Utilities.decompress(jobDefinition.sql.text, jobDefinition.sql.compressionType)
    )

    // write result DataFrame
    val outputDataInfo = jobDefinition.outputDataInfo
    if (outputDataInfo.isDefined) {

      DataWriter.write(resultDF, outputDataInfo.get)
    }

    (sparkSession, jobDefinition, resultDF)
  }

}
