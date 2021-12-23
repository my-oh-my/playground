package com.myself.xml.reader.app.processing.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkSessionWrapper {

  private val log = LoggerFactory.getLogger(this.getClass)
  Logger.getLogger("org").setLevel(Level.OFF)

  def apply(sparkConfMap: Option[Map[String, String]]): SparkSession = {

    val sparkConf = sparkConfMap match {

      case Some(definedSparkConf) => setAllSparkConf(definedSparkConf)
      case None => new SparkConf()
    }

    new SparkSessionWrapper(sparkConf).getInstance
  }

  def apply(): SparkSession = {

    apply(None)
  }

  def setAllSparkConf(sparkConfMap: Map[String, String]): SparkConf = {

    for ((key, value) <- sparkConfMap) {
      log.info(s"SparkConf property: $key=$value")
    }

    new SparkConf().setAll(sparkConfMap)
  }

}

/**
 * Provides SparkSession
 *
 * @param sparkConf
 */
class SparkSessionWrapper(sparkConf: SparkConf) {

  private val sparkSession: SparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  def getInstance: SparkSession = sparkSession

}