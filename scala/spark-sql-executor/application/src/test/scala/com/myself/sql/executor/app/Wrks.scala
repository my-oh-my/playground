package com.myself.sql.executor.app

import com.myself.sql.executor.app.service.spark.SparkSessionWrapper
import com.myself.sql.executor.app.util.compression.Lz4

object Wrks extends App {

  val json = "{\n  \"outputDataInfo\": {\n    \"format\": \"parquet\",\n    \"path\": \"/Users/me/Desktop/Data/out\",\n    \"saveMode\": \"Overwrite\"\n  },\n  \"sql\": {\n    \"text\": \"SELECT 1\"\n  },\n  \"jobName\": \"playground\"\n}"

  println(Lz4.compress(json))
}

object LastCheck extends App {

  val sparkSession = SparkSessionWrapper()

  val dF = sparkSession.read.parquet("/Users/me/Desktop/Data/out")
  dF.show()
}