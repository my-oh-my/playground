package com.myself.sql.executor.app.model

import com.myself.sql.executor.app.service.spark.SparkSessionWrapper
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * Data set main information
 *
 * @param format        file format extension
 * @param formatOptions file format related information
 * @param path          file path
 */
sealed class DataInfo(format: String, formatOptions: Option[Map[String, String]], path: String)

/**
 * Spark JSON-schema with compression type information
 *
 * @param text            Spark JSON-schema
 * @param compressionType text compression type of 'none', 'lz4'
 */
final case class DatasetSchemaJson(text: String, compressionType: Option[String] = Some("none"))

/**
 * Input data set information
 *
 * @param format            file format extension
 * @param formatOptions     file format related information
 * @param path              file path
 * @param dataAlias         used for SQL processing/Spark temporary view registration
 * @param dataSetSchemaJson Spark JSON-schema for data set
 * @param dataSetSchemaPath path to Spark JSON-schema for data set
 */
final case class InputDataInfo(format: String,
                               formatOptions: Option[Map[String, String]],
                               path: String,
                               dataAlias: String,
                               dataSetSchemaJson: Option[DatasetSchemaJson],
                               dataSetSchemaPath: Option[String])
  extends DataInfo(format, formatOptions, path)

/**
 * Output data set information
 *
 * @param format            file format extension
 * @param formatOptions     file format related information for DataFrameReader https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html
 * @param path              file path
 * @param saveMode          file save mode accordign to {@link org.apache.spark.sql.SaveMode}
 */
final case class OutputDataInfo(format: String,
                                formatOptions: Option[Map[String, String]],
                                path: String,
                                saveMode: Option[String] = None)
  extends DataInfo(format, formatOptions, path)

/**
 * Spark JSON-schema with compression type information
 *
 * @param text            SQL
 * @param compressionType text compression type of 'none', 'lz4'
 */
final case class SQL(text: String, compressionType: Option[String] = Some("none"))

/**
 * Job processing information
 *
 * @param inputsDataInfo  input data sets information
 * @param outputDataInfo  output data set information
 * @param sql             SQL with compression type information
 * @param sparkConfMap    map with SparkConf properties
 * @param configuration   process configuration
 * @param jobName         descriptive job name
 */
final case class JobDefinition(inputsDataInfo: List[InputDataInfo],
                               outputDataInfo: OutputDataInfo,
                               sql: SQL,
                               sparkConfMap: Option[Map[String, String]],
                               configuration: Option[Map[String, Any]],
                               jobName: String)

/**
 * JobDefinition wrapper with compression type information
 *
 * @param jobDefinition   Job processing information
 * @param compressionType text compression of 'none', 'lz4'
 */
final case class JobRequest(jobDefinition: String,
                            compressionType: Option[String] = Some("none"))

object SaveToParquet extends App {

  implicit val sparkSession = SparkSessionWrapper()

  val schema = StructType(
    Seq(StructField("col_a", StringType),
    StructField("col_b", IntegerType))
  )

  val data = Seq(
    Row("first", 1000),
    Row("second", 2000)
  )

  val dataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(data), schema)

  dataFrame.write.mode(SaveMode.Overwrite).parquet("/Users/me/Desktop/Data/in/")
}