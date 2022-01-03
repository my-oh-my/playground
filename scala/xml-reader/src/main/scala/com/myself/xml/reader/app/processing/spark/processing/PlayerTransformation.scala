package com.myself.xml.reader.app.processing.spark.processing

import com.myself.xml.reader.app.processing.spark.processing.matchresults.MatchResultsTransformations
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, explode, rank}
import org.apache.spark.sql.types.IntegerType

object PlayerTransformation extends MatchResultsTransformations {

  val initialDataTransformation: DataFrame => DataFrame = inputDf => {

    SelectPlayers(inputDf)
      .select(col("players"))
      .withColumn("player_id", col("players._PlayerRef"))
      .withColumn("stats", explode(col("players.Stat")))
      .select(
        col("player_id"),
        col("stats._Type").as("statisticsType"),
        col("stats._VALUE").cast(IntegerType).as("statisticsValue")
      )
  }

  val topNPlayersStatistics: (DataFrame, String, Integer) => DataFrame = (inputDf, statisticsType, limit) => {

    val windowSpecification  = Window.orderBy(col("statisticsValue").desc)

    val filteredDf = inputDf
      .where(col("statisticsType") === statisticsType)
      .orderBy(col("statisticsValue").desc)
      .limit(limit)

    filteredDf
      .withColumn("rank", rank().over(windowSpecification))
      .select(
        col("rank").as("POSITION_IN_RANKING"),
        col("player_id").as("PLAYER_ID"),
        col("statisticsValue").as("STATISTICS_VALUE")
      )
  }

}
