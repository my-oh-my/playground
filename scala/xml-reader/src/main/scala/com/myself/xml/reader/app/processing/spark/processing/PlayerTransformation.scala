package com.myself.xml.reader.app.processing.spark.processing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, explode, rank}
import org.apache.spark.sql.types.IntegerType


object PlayerTransformation {

  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org").setLevel(Level.OFF)

  val initialDataTransformation: DataFrame => DataFrame = inputDf => {

    val initialSelectDf = inputDf.withColumn("players", explode(col("PlayerLineUp.MatchPlayer")))
    val explodedStatDf = initialSelectDf
      .select(col("players"))
      .withColumn("player_id", col("players._PlayerRef"))
      .withColumn("stats", explode(col("players.Stat")))
      .drop(col("players"))

    val beforeAggregationDf = explodedStatDf
      .select(
        col("player_id"),
        col("stats._Type").as("statisticsType"),
        col("stats._VALUE").cast(IntegerType).as("statisticsValue")
      )

    beforeAggregationDf
  }

  val topNPlayersStatistics: (DataFrame, String, Integer) => DataFrame = (inputDf, statisticsType, limit) => {

    val windowSpecification  = Window.orderBy(col("statisticsValue").desc)

    val filteredDf = inputDf
      .where(col("statisticsType") === statisticsType)
      .orderBy(col("statisticsValue").desc)
      .limit(limit)

    filteredDf
      .withColumn("rank", rank().over(windowSpecification))
      .select(col("rank"), col("player_id"), col("statisticsType"), col("statisticsValue"))
  }

}
