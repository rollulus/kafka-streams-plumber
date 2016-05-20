package com.eneco.energy.kafka.streams.plumber

import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{FunSuite, Matchers}

class ChainingOperationsTest extends FunSuite with Matchers with MockFactory with Utl {
  test("Functional-style operators are correctly chained") {
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
        | .filter(function(k,v) return k and #k==5 or not k end)
        | .map(function(k,v) return k:upper(),v end)
        | .filter(function(k,v) return k:byte(-1) == 79 end)
        |
      """.stripMargin

    val r = new Record(inSchema)

    r.put("i", 8)

    process[String, GenericRecord](lua, ("hello", r), KeyValueType(StringType, AvroType(Some(outSchema)))).foreach { case (k, v) => {
      k.toString shouldEqual "HELLO"
      v.get("j") shouldEqual 16
    }}

    //k and #k==5 or not k fails
    process[String, GenericRecord](lua, ("hellooo", r), KeyValueType(StringType, AvroType(Some(outSchema)))).isDefined shouldBe false

    //return k:byte(-1) == 79 fails
    process[String, GenericRecord](lua, ("hellp", r), KeyValueType(StringType, AvroType(Some(outSchema)))).isDefined shouldBe false

    r.put("i", 7)
    process[String, GenericRecord](lua, ("hello", r), KeyValueType(StringType, AvroType(Some(outSchema)))).isDefined shouldBe false

    r.put("i", 17)
    process[String, GenericRecord](lua, ("hello", r), KeyValueType(StringType, AvroType(Some(outSchema)))).isDefined shouldBe false
  }
}