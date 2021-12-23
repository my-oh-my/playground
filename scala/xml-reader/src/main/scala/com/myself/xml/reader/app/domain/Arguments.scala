package com.myself.xml.reader.app.domain

import com.beust.jcommander.{JCommander, Parameter}

object Arguments {

  def parse(args: Array[String]): Arguments = {
    val arguments = new Arguments()

    JCommander
      .newBuilder()
      .addObject(arguments)
      .build()
      .parse(args.mkString(" ").split(" "): _*)

    arguments
  }

}

class Arguments {

  @Parameter(names = Array("-statisticsType"), description = "Type of statistics")
  private var statisticsType: String = _

  @Parameter(names = Array("-statisticsPath"), description = "Path to statistics file")
  private var statisticsPath: String = _

  def getStatisticsType: String = statisticsType

  def getStatisticsPath: String = statisticsPath

  override def toString: String =
    s"Arguments: statisticsType=$statisticsType, statisticsPath=$statisticsPath"

}
