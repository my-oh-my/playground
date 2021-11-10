package com.myself.sql.executor.app.service.spark.reading

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

sealed abstract class DataReader(schema: Option[StructType] = None)
                                (implicit sparkSession: SparkSession) {

  /**
   * Input data set file format extension
   */
  val format: String

  /**
   * Input data set file format DataFrameReader options
   */
  val formatOptions: Option[Map[String, String]]

  protected def applyFormatOptions(reader: DataFrameReader, formatOptions: Option[Map[String, String]]): DataFrameReader = {

    if (formatOptions.isDefined) reader.options(prepareFormatOptions(formatOptions.get))
    else reader
  }

  protected def applySchema(reader: DataFrameReader, schema: Option[StructType]): DataFrameReader = {

    schema match {
      case Some(definedSchema) => reader.schema(definedSchema)
      case None => reader
    }
  }

  protected def getDataFrameReader(format: String): DataFrameReader = {

    sparkSession.read.format(format)
  }

  /**
   * Execute load method on DataFrameReader with format options and schema
   *
   * @param path  input data set path
   * @return
   */
  def readDataFrame(path: Option[String] = None): DataFrame = {

    val dataFrameReader = getDataFrameReader(format)

    // apply format options
    val dataFrameReaderWithOptions = applyFormatOptions(dataFrameReader, formatOptions)

    // apply schema
    val dataFrameReaderWithSchema = applySchema(dataFrameReaderWithOptions, schema)

    // for JDBC connections path is not provided
    if (path.isDefined) {
      dataFrameReaderWithSchema.load(path.get)
    } else dataFrameReaderWithSchema.load()
  }

  /**
   * Abstract method, transform input data set format options to be applied to DataFrameReader
   *  if no operation needed on provided options input becomes output
   *
   * @param map input data set format DataFrameReader options
   * @return
   */
  def prepareFormatOptions(map: Map[String, String]): Map[String, String]

}

object CsvReader {

  def apply(formatOptions: Option[Map[String, String]], schema: Option[StructType] = None)
           (implicit sparkSession: SparkSession): CsvReader = {

    new CsvReader(formatOptions, schema)
  }

}

final class CsvReader(providedFormatOptions: Option[Map[String, String]], schema: Option[StructType] = None)
                     (implicit sparkSession: SparkSession) extends DataReader(schema)(sparkSession) {
  /**
   * Input data set file format extension
   */
  override val format: String = "csv"
  /**
   * Input data set file format DataFrameReader options
   */
  override val formatOptions: Option[Map[String, String]] = providedFormatOptions

  /**
   * Transforms input data set format options to be applied to DataFrameReader
   *
   * @param map input data set format options
   * @return
   */
  override def prepareFormatOptions(map: Map[String, String]): Map[String, String] = {

    map
  }

}

object ParquetReader {

  def apply(formatOptions: Option[Map[String, String]], schema: Option[StructType] = None)
           (implicit sparkSession: SparkSession): ParquetReader = {

    new ParquetReader(formatOptions, schema)
  }

}

final class ParquetReader(providedFormatOptions: Option[Map[String, String]] = None, schema: Option[StructType] = None)
                         (implicit sparkSession: SparkSession) extends DataReader(schema)(sparkSession) {
  /**
   * Input data set file format extension
   */
  override val format: String = "parquet"
  /**
   * Input data set file format DataFrameReader options
   */
  override val formatOptions: Option[Map[String, String]] = providedFormatOptions

  /**
   * Transforms input data set format options to be applied to DataFrameReader
   *
   * @param map input data set format options
   * @return
   */
  override def prepareFormatOptions(map: Map[String, String]): Map[String, String] = {

    // if needed please refer to: https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html#parquet-scala.collection.Seq-
    Map.empty
  }

}