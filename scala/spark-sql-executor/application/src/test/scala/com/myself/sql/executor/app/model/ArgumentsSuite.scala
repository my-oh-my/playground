package com.myself.sql.executor.app.model

import com.beust.jcommander.ParameterException
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ArgumentsSuite extends AnyFunSuite{

  val jobDefinitionValueString = "fromJobDefinition"
  val jobDefinitionString = s"-jobDefinition $jobDefinitionValueString"

  val compressionTypeValueString = "fromCompressionType"
  val compressionTypeString = s"-compressionType $compressionTypeValueString"

  val configValueString = "fromConfig"
  val configString = s"-config $configValueString"

  test("Instantiate Argument object with parse method - all fields") {

    val argumentsArray = Array(jobDefinitionString, compressionTypeString, configString)

    val actual = Arguments.parse(argumentsArray)

    //
    assert(actual.isInstanceOf[Arguments])
    assertResult(jobDefinitionValueString)(actual.getJobDefinition)
    assertResult(compressionTypeValueString)(actual.getCompressionType)
    assertResult(configValueString)(actual.getConfig)
  }

  test("Instantiate Argument object with parse method - with only -jobDefinition") {

    val argumentsArray = Array(jobDefinitionString)

    val actual = Arguments.parse(argumentsArray)

    //
    assert(actual.isInstanceOf[Arguments])
    assertResult(jobDefinitionValueString)(actual.getJobDefinition)
    assert(actual.getCompressionType == null)
    assert(actual.getConfig == null)
  }

  test("Instantiate Argument object with parse method - with only -compressionType") {

    val argumentsArray = Array(compressionTypeString)

    val actual = Arguments.parse(argumentsArray)

    //
    assert(actual.isInstanceOf[Arguments])
    assert(actual.getJobDefinition == null)
    assertResult(compressionTypeValueString)(actual.getCompressionType)
    assert(actual.getConfig == null)
  }

  test("Instantiate Argument object with parse method - with only -config") {

    val argumentsArray = Array(configString)

    val actual = Arguments.parse(argumentsArray)

    //
    assert(actual.isInstanceOf[Arguments])
    assert(actual.getJobDefinition == null)
    assert(actual.getCompressionType == null)
    assertResult(configValueString)(actual.getConfig)
  }

  test("Throws exception when invalid argument name") {

    val argumentsArray = Array("-invalid fromInvalid")

    assertThrows[ParameterException](Arguments.parse(argumentsArray))
  }
}
