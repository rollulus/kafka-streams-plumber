package com.eneco.energy.kafka.streams.plumber

import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
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
    val ro = process[String, GenericRecord](lua, (null, r), KeyValueType(StringType, AvroType(Some(outSchema)))).get._2

    ro.get("j") shouldEqual 16

    r.put("i", 7)
    process[String, GenericRecord](lua, (null, r), KeyValueType(StringType, AvroType(Some(outSchema)))).isDefined shouldBe false

    r.put("i", 17)
    process[String, GenericRecord](lua, (null, r), KeyValueType(StringType, AvroType(Some(outSchema)))).isDefined shouldBe false

  }
}