package com.myself.sql.executor.app.util

import java.io.ByteArrayInputStream
import java.util.Properties

import com.myself.sql.executor.app.model.Arguments
import com.myself.sql.executor.app.util.compression.Lz4
import com.typesafe.config.{Config, ConfigFactory}

import scala.io.Source

object Utilities {

  def resolveConfig(arguments: Arguments): Config = {

    val separator = ";"

    val properties = new Properties()
    val parsedConfig = arguments.getConfig.replace(separator, "\n")
    properties.load(new ByteArrayInputStream(parsedConfig.getBytes()))

    val config =  ConfigFactory.parseProperties(properties).resolve()

    // resolve application.conf properties
    ConfigFactory.parseResources("application.conf").withFallback(config).resolve()
  }

  def readFromFile(path: String): Iterator[String] = {

    val source = Source.fromFile(path)
    try {
      source.getLines()
    } finally {
      source.close()
    }
  }

  def measureJob[T](process: => T): (T, Long) = {

    val processStart: Long = System.currentTimeMillis()
    val assignedProcess = process
    val processStop: Long = System.currentTimeMillis()

    (assignedProcess, processStop - processStart)
  }

  def decompress(compressionType: String, text: String): String = {

    compressionType.toLowerCase match {

      case "lz4" => Lz4.decompress(text)
      case "none" => text
      case _ => throw new RuntimeException(s"Provided compression $compressionType is not supported")
    }
  }

}
