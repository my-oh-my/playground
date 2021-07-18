package com.myself.sql.executor.app.service.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

object SqlExecutor {

  def apply()(implicit sparkSession: SparkSession): SqlExecutor =
    new SqlExecutor()

}

/**
 * Spark SQL API service class
 *
 * @param sparkSession
 */
class SqlExecutor()(implicit sparkSession: SparkSession) {

  def execute(sql: String): DataFrame = {

    sparkSession.sql(sql)
  }

}
