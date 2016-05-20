package com.eneco.energy.kafka.streams.plumber

import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConverters._

class AllFieldTypesTest extends FunSuite with Matchers with MockFactory with Utl {
  test("Enums") {
    val msgQueueEnum = enum("MessageQueue",Seq("Kafka","ZeroMQ","NATS","Other"))
    val inSchema = rec("t", Map(
      "queue" -> msgQueueEnum
    ))
    val L =
      """function process(t)
        |assert(t.queue == "ZeroMQ")
        |return {queue="Kafka"}
        |end
        |return pb.mapValues(process)
      """.stripMargin
    val r = new Record(inSchema)
    r.put("queue", new GenericData.EnumSymbol(msgQueueEnum,"ZeroMQ"))
    val ro2 = process[String, GenericRecord](L, (null, r), KeyValueType(StringType, AvroType(Some(inSchema)))).get._2
    ro2.get("queue").asInstanceOf[GenericData.EnumSymbol].toString shouldEqual "Kafka"
  }

  test("Union of nullable strings") {
    val lua =
      """function process(t)
        |  assert(t.optstring0 == nil)
        |  assert(t.optstring1 == "o")
        |  assert(t.mandstring == "m")
        |  return {mandstring="mm", optstring0="o0"} -- this makes optstring0 nil
        |end
        |return pb.mapValues(process)
      """.stripMargin

    val inSchema = rec("t", Map(
      "optstring0" -> union(nul, string),
      "optstring1" -> union(nul, string),
      "mandstring" -> string
    ))
    val r = new Record(inSchema)
    r.put("optstring0", null)
    r.put("optstring1", "o")
    r.put("mandstring", "m")

    val ro2 = process[String, GenericRecord](lua, (null, r), KeyValueType(StringType, AvroType(Some(inSchema)))).get._2

    ro2.get("optstring0").toString shouldBe "o0"
    ro2.get("optstring1") shouldBe null
    ro2.get("mandstring").toString shouldBe "mm"
  }

  test("A generic record should properly appear in Lua world, and vice versa") {
  def bananaSchema() = rec("banana", Map("color"->string, "weight"->float))
  def inSchema() =
    rec("t", Map(
      "boolean" -> boolean,
      "int" -> int,
      "long" -> long,
      "float" -> float,
      "double" -> double,
      "string" -> string,
      "strings" -> array(string),
      "bananas" -> array(bananaSchema)
    ))


  val myLua =
    """function process(t)
      |    assert(t.boolean==true)
      |    assert(t.int==1)
      |    assert(t.long==2)
      |    assert(t.float==3)
      |    assert(t.double==4)
      |    assert(t.string=="s")
      |    assert(t.strings[1]..t.strings[2]..t.strings[3] == "aapnootmies")
      |
      | return {
      |  boolean=false,
      |  int=7,
      |  long=6,
      |  float=5,
      |  double=4,
      |  string="q",
      |  strings={"it","works"},
      |  bananas={ {color="brown", weight="3.2"} },
      |	}
      |end
      |return pb.mapValues(process)
    """.stripMargin

    val b0 = new Record(bananaSchema)
    b0.put("color","yellow")
    b0.put("weight",7.4f)
    val r = new Record(inSchema)
    r.put("boolean",true)
    r.put("int",1)
    r.put("long",2L)
    r.put("float",3f)
    r.put("double",4.0)
    r.put("string","s")
    r.put("strings",Seq("aap","noot","mies").asJavaCollection)
    r.put("bananas",Seq(b0).asJavaCollection)

    val ro2 = process[String, GenericRecord](myLua, (null, r), KeyValueType(StringType, AvroType(Some(inSchema)))).get._2

    ro2.get("boolean") shouldBe false
    ro2.get("int") shouldBe 7
    ro2.get("long") shouldBe 6
    ro2.get("float") shouldBe 5
    ro2.get("double") shouldBe 4
    ro2.get("string").toString shouldBe "q"
    ro2.get("strings").asInstanceOf[java.util.List[Utf8]].asScala.toSeq shouldEqual Seq(new Utf8("it"), new Utf8("works"))
    val bo0 = ro2.get("bananas").asInstanceOf[java.util.List[GenericRecord]].get(0)
    bo0.get("color").toString shouldBe "brown"
    bo0.get("weight") shouldBe 3.2f
 }
}