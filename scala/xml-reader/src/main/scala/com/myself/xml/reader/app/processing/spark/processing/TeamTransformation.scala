package com.myself.xml.reader.app.processing.spark.processing

import com.myself.xml.reader.app.processing.spark.processing.matchresults.MatchResultsTransformations
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode, sum}
import org.apache.spark.sql.types.IntegerType

object TeamTransformation extends MatchResultsTransformations {

  val initialDataTransformation: DataFrame => DataFrame = inputDf => {

    inputDf
      .withColumn("TEAM_SIDE", col("_Side"))
      .withColumn("TEAM_NAME", col("_TeamRef"))
      .transform(SelectPlayers)
      .withColumn("stats", explode(col("players.Stat")))
      .select(
        col("TEAM_SIDE"),
        col("TEAM_NAME"),
        col("stats._Type").as("statisticsType"),
        col("stats._VALUE").cast(IntegerType).as("statisticsValue")
      )
  }

  val statisticsByTeamAndSide: (DataFrame, String) => DataFrame = (inputDf, statisticsType) => {

    inputDf
      .where(col("statisticsType") === statisticsType)
      .groupBy(col("TEAM_SIDE"), col("TEAM_NAME"))
      .agg(
        sum(col("statisticsValue")).as("SUM_OF_STATISTICS_VALUES")
      )
  }

}
