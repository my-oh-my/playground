package com.myself.sql.executor.app.service.spark.writing

import com.myself.sql.executor.app.model.OutputDataInfo
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}

import scala.util.{Failure, Success, Try}

object DataWriter {

  def getDataFrameWriter(resultDF: DataFrame, format: String): DataFrameWriter[Row] = {

    resultDF.write.format(format)
  }

  def applyOptions(dataFrameWriter: DataFrameWriter[Row], options: Option[Map[String, String]]): DataFrameWriter[Row] = {

    options match {
      case Some(definedOptions) =>dataFrameWriter.options(definedOptions)
      case None => dataFrameWriter
    }
  }

  def getSaveMode(saveModeStr: Option[String]): Option[SaveMode] = {

    if (saveModeStr.isDefined) {
      Try {
        SaveMode.valueOf(saveModeStr.get)
      } match {
        case Success(definedEnum) => Some(definedEnum)
        case Failure(exception) => {

          val listOfSaveModes: String = s"${SaveMode.Append.toString}, ${SaveMode.ErrorIfExists.toString}, ${SaveMode.Ignore.toString}, ${SaveMode.Overwrite.toString}"
          throw new IllegalArgumentException(s"Value not match any of: $listOfSaveModes, ${exception.getMessage}")
        }
      }
    } else None
  }

  def applyWriterSaveMode(dataFrameWriter: DataFrameWriter[Row], mode: Option[SaveMode] = None): DataFrameWriter[Row] = {

    mode match {
      case Some(definedMode) => dataFrameWriter.mode(definedMode)
      // default is SaveMode.ErrorIfExists
      case None => dataFrameWriter
    }
  }

  def write(resultDF: DataFrame, outputDataInfo: OutputDataInfo): String = {

    val dataFrameWriter = getDataFrameWriter(resultDF, outputDataInfo.format)

    // apply writer.options
    val dataFrameWriterWithOptions = applyOptions(dataFrameWriter, outputDataInfo.formatOptions)

    // apply writer.mode
    val dataFrameWriterWithSaveMode = applyWriterSaveMode(dataFrameWriterWithOptions, outputDataInfo.saveMode)

    // save result DataFrame
    dataFrameWriterWithSaveMode.save(outputDataInfo.path)

    outputDataInfo.path
  }

}