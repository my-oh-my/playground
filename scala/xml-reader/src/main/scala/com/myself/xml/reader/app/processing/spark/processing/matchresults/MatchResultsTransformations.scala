package com.myself.xml.reader.app.processing.spark.processing.matchresults

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

trait MatchResultsTransformations {

  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org").setLevel(Level.OFF)

  val SelectPlayers: DataFrame => DataFrame =
    _.withColumn("players", explode(col("PlayerLineUp.MatchPlayer")))

}
