package com.myself.xml.reader.app

import com.myself.xml.reader.app.domain.Arguments
import com.myself.xml.reader.app.processing.spark.SparkSessionWrapper
import com.myself.xml.reader.app.processing.spark.input.XmlReader
import com.myself.xml.reader.app.processing.spark.processing.{PlayerTransformation, TeamTransformation}
import org.apache.spark.sql.SparkSession

object XmlProcessorApp {

  def main(args: Array[String]): Unit = {

    // parse arguments
    val arguments = Arguments.parse(args)
    val statisticsType = arguments.getStatisticsType
    val statisticsPath = arguments.getStatisticsPath

    // non-production code with spark-master set to local
    implicit val sparkSession: SparkSession = SparkSessionWrapper(Some(Map("spark.master" -> "local[2]")))

    // read to DataFrame
    val xmlReaderFormatOptions = Map("rootTag" -> "MatchData", "rowTag" -> "TeamData")
    val inputDF = XmlReader(Some(xmlReaderFormatOptions)).readDataFrame(statisticsPath).cache()

    // 1. run statistics on Players
    val top5PlayersStatisticsDf = inputDF
      .transform(PlayerTransformation.initialDataTransformation)
      .transform(PlayerTransformation.topNPlayersStatistics(_, statisticsType, 5))
    // outputs table-like result to stdout
    top5PlayersStatisticsDf.show(false)

    // 2. run statistics on Teams
    val teamStatisticsDf = inputDF
      .transform(TeamTransformation.initialDataTransformation)
      .transform(TeamTransformation.statisticsByTeamAndSide(_, statisticsType))
    // outputs table-like result to stdout
    teamStatisticsDf.show(false)
  }

}
