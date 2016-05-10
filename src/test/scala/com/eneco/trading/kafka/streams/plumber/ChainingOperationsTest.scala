package com.eneco.energy.kafka.streams.plumber

import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.util.Utf8
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{FunSuite, Matchers}

class ChainingOperationsTest extends FunSuite with Matchers with MockFactory with Utl {
  test("x") {
    val inSchema = rec("t", Map(
      "i" -> int
    ))
    val outSchema = rec("t", Map(
      "j" -> int
    ))
    val lua =
      """
        |return pb
        | .mapValues(function(v) return {zxcv = v.i * 2} end)
        | .filter(function(k,v) return v.zxcv >= 16 end)
        | .mapValues(function(v) return {j = v.zxcv} end)
        | .filter(function(k,v) return v.j <= 32 end)
        |
      """.stripMargin

    val r = new Record(inSchema)

    r.put("i", 8)
    val ro = new StreamingOperations(lua,outSchema).transformGenericRecord((null,r)).get._2
    ro.get("j") shouldEqual 16

    r.put("i", 7)
    new StreamingOperations(lua,outSchema).transformGenericRecord((null,r)).isDefined shouldBe false

    r.put("i", 17)
    new StreamingOperations(lua,outSchema).transformGenericRecord((null,r)).isDefined shouldBe false

  }
}