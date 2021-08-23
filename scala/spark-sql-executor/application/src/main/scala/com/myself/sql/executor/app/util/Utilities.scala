package com.myself.sql.executor.app.util

import java.io.ByteArrayInputStream
import java.util.Properties
import com.myself.sql.executor.app.model.Arguments
import com.myself.sql.executor.app.util.compression.Lz4
import com.typesafe.config.{Config, ConfigFactory}

import scala.io.{BufferedSource, Source}

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

  def readFromFile(path: String): (BufferedSource, Iterator[String]) = {

    val resource = Source.fromFile(path)
    val lines = try {
      resource.getLines()
    } catch {
      case exception: Throwable =>
        resource.close()

        throw new RuntimeException(s"Exception when reading from file: $exception")
    }

    (resource, lines)
  }

  def measureJob[T](process: => T): (T, Long) = {

    val processStart: Long = System.currentTimeMillis()
    val assignedProcess = process
    val processStop: Long = System.currentTimeMillis()

    (assignedProcess, processStop - processStart)
  }

  def decompress(text: String, compressionType: Option[String] = None): String = {

    compressionType match {
      case Some(definedCompressionType) => definedCompressionType match {
        case "lz4" => Lz4.decompress(text)
        case "none" => text
        case _ => throw new RuntimeException(s"Provided compression $compressionType is not supported")
      }
      case None => text
    }
  }

}
