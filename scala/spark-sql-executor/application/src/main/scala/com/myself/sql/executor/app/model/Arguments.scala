package com.myself.sql.executor.app.model

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

  @Parameter(names = Array("-jobDefinition"), description = "Input parameters")
  private var jobDefinition: String = _

  @Parameter(names = Array("-compressionType"), description = "JobDefinition compressionType")
  private var compressionType: String = _

  @Parameter(names = Array("-config"), description = "Application properties")
  private var config: String = _

  def getJobDefinition: String = jobDefinition

  def getCompressionType: String = compressionType

  def getConfig: String = config

  override def toString: String =
      s"Arguments: jobDefinition=$jobDefinition, compressionType=$compressionType, config=$config"

}