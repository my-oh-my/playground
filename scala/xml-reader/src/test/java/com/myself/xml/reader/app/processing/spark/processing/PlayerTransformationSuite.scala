package com.myself.xml.reader.app.processing.spark.processing

import com.myself.xml.reader.app.processing.spark.SparkSessionWrapper
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class PlayerTransformationSuite extends AnyFunSuite {

  import org.apache.log4j.{Level, Logger}

  Logger.getLogger("org").setLevel(Level.OFF)

  implicit val sparkSession: SparkSession = SparkSessionWrapper(Some(Map("spark.master" -> "local[2]")))

  test("should return top player for specific statistics type") {

    // given
    val statisticsTypeCondition = "stat_A"

    val expectedSchema = StructType(
      Seq(
        StructField("rank", IntegerType),
        StructField("player_id", StringType),
        StructField("statisticsType", StringType),
        StructField("statisticsValue", IntegerType)
      )
    )

    val expectedData = Seq(Row(1, "p01", statisticsTypeCondition, 100))
    val expectedDf = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(expectedData), expectedSchema)

    val inputSchema = StructType(
      Seq(
        StructField("player_id", StringType),
        StructField("statisticsType", StringType),
        StructField("statisticsValue", IntegerType)
      )
    )

    val playersData = Seq(
      Row("p01", statisticsTypeCondition, 100),
      Row("p01", "stat_B", 200),
      Row("p01", statisticsTypeCondition, 1)
    )

    val playersDf = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(playersData), inputSchema)
    // when
    val actualDf = playersDf.transform(PlayerTransformation.topNPlayersStatistics(_, statisticsTypeCondition, 1))

    // then
    // silly check based on set operators
    assert(actualDf.except(expectedDf).count() == expectedDf.except(actualDf).count())
  }

}
