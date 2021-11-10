package com.myself.sql.executor.app.util

import com.myself.sql.executor.app.model.Arguments
import com.myself.sql.executor.app.util.compression.Lz4
import com.typesafe.config.Config
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UtilitiesSuite extends AnyFunSuite {

  val hostValue = "localhost"
  val host = s"host=$hostValue"

  val portValue = 8070
  val port = s"port=$portValue"

  val bootstrapServersValue = "localhost:2821"
  val bootstrapServer = s"bootstrap-servers=$bootstrapServersValue"

  val configValueString = Array(host, port, bootstrapServer).mkString(";")
  val configString = s"-config $configValueString"

  test("resolveConfig should return proper values") {

    val arguments = Arguments.parse(Array(configString))

    val actual = Utilities.resolveConfig(arguments)
    //
    assert(actual.isInstanceOf[Config])
    assertResult(hostValue)(actual.getString("host"))
    assertResult(portValue)(actual.getInt("port"))
    assertResult(bootstrapServersValue)(actual.getString("bootstrap-servers"))
  }

  test("measureJob returns tuple of the process and time in long") {

    val process: Int = {

      1
    }

    val (actualProcess, time) = Utilities.measureJob(process)
    //
    assert(actualProcess.isInstanceOf[Int])
    assertResult(process)(actualProcess)
    assert(time.isInstanceOf[Long])
  }

  test("decompress returns decompressed text - from lz4") {

    val rawData = "Uncompressed data"

    val lz4Compressed = Lz4.compress(rawData)

    val actual = Utilities.decompress(lz4Compressed, Some("lz4"))
    //
    assertResult(rawData)(actual)
  }

}
