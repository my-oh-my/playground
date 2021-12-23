package com.myself.xml.reader.app.processing.spark.input

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

sealed abstract class DataReader(schema: Option[StructType] = None)
                                (implicit sparkSession: SparkSession) {

  /**
   * Input data set file format extension
   */
  val format: String

  /**
   * Input data set file format DataFrameReader/databrics spark-xml options
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
  def readDataFrame(path: String): DataFrame = {

    val dataFrameReader = getDataFrameReader(format)

    // apply format options
    val dataFrameReaderWithOptions = applyFormatOptions(dataFrameReader, formatOptions)

    // apply schema
    val dataFrameReaderWithSchema = applySchema(dataFrameReaderWithOptions, schema)

    dataFrameReaderWithSchema.load(path)
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

object XmlReader {

  def apply(formatOptions: Option[Map[String, String]], schema: Option[StructType] = None)
           (implicit sparkSession: SparkSession): XmlReader = {

    new XmlReader(formatOptions, schema)
  }

}

final class XmlReader(providedFormatOptions: Option[Map[String, String]], schema: Option[StructType] = None)
                     (implicit sparkSession: SparkSession) extends DataReader(schema)(sparkSession) {
  /**
   * Input data set file format extension
   */
  override val format: String = "com.databricks.spark.xml"
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