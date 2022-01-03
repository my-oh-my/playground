package com.myself.xml.reader.app.domain

import com.beust.jcommander.ParameterException
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ArgumentsSuite extends AnyFunSuite {

  test("should instantiate Argument object with parse method") {

    // given
    val statisticsTypeParameterValue = "type"
    val statisticsTypeParameter = s"-statisticsType $statisticsTypeParameterValue"

    val statisticsPathParameterValue = "path"
    val statisticsPathParameter = s"-statisticsPath $statisticsPathParameterValue"

    // when
    val actual = Arguments.parse(Array(statisticsTypeParameter, statisticsPathParameter))

    // then
    assert(actual.isInstanceOf[Arguments])
    assertResult(statisticsTypeParameterValue)(actual.getStatisticsType)
    assertResult(statisticsPathParameterValue)(actual.getStatisticsPath)
  }

  test("Throws exception when invalid argument name") {

    // given
    val argumentsArray = Array("-invalid fromInvalid")

    // then
    assertThrows[ParameterException](Arguments.parse(argumentsArray))
  }

}
