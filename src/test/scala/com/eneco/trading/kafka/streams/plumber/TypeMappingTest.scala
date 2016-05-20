package com.eneco.energy.kafka.streams.plumber

import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConverters._

class TypeMappingTest extends FunSuite with Matchers with MockFactory with Utl {
  test("Avro-Avro key-value in/out") {
    val L =
      """return pb.map(function(k,v)
        | assert(k.a=="x")
        | assert(v.b==42)
        | return {c=.5},{d=19}
        |end)
      """.stripMargin
    val kIn = new Record(rec("t", Map("a" -> string)))
    val vIn = new Record(rec("t", Map("b" -> int)))
    val kOutSchema = rec("t", Map("c" -> float))
    val vOutSchema = rec("t", Map("d" -> long))
    kIn.put("a", "x")
    vIn.put("b", 42)
    val ro2 = process[GenericRecord, GenericRecord](L, (kIn, vIn), KeyValueType(AvroType(Some(kOutSchema)), AvroType(Some(vOutSchema)))).foreach {
      case (k, v) => {
        k.get("c") shouldBe .5
        v.get("d") shouldBe 19
      }
    }
  }
  test("String-String key-value in/out") {
    val L =
      """return pb.map(function(k,v)
        | assert(k=="k")
        | assert(v=="v")
        | return "ok", "ov"
        |end)
      """.stripMargin
    val ro2 = process[String, String](L, ("k", "v"), KeyValueType(StringType, StringType)).foreach {
      case (k, v) => {
        k.toString shouldBe "ok"
        v.toString shouldBe "ov"
      }
    }
  }
  test("Long-Long key-value in/out") {
    val L =
      """return pb.map(function(k,v)
        | assert(k==111)
        | assert(v==222)
        | return 333,444
        |end)
      """.stripMargin
    val ro2 = process[java.lang.Long, java.lang.Long](L, (java.lang.Long.valueOf(111), java.lang.Long.valueOf(222)), KeyValueType(LongType, LongType)).foreach {
      case (k, v) => {
        k shouldBe 333
        v shouldBe 444
      }
    }
  }
  test("Avro value in, Long value out") {
    val L =
      """return pb.map(function(k,v)
        | assert(k==nil)
        | assert(v.l==12345)
        | return nil, 0x1337c0de
        |end)
      """.stripMargin
    val vIn = new Record(rec("t", Map("l" -> long)))
    vIn.put("l", 12345L)
    val ro2 = process[String, java.lang.Long](L, (null, vIn), KeyValueType(VoidType, LongType)).foreach {
      case (k, v) => {
        k shouldBe null
        v shouldBe 0x1337c0de
      }
    }
  }
}