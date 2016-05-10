package com.eneco.energy.kafka.streams.plumber

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.{GenericRecord, GenericData}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.util.Utf8
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConverters._

//Avro API && me = :(
trait Utl {
  def rec(n: String, fs: Map[String, Schema] = null): Schema = {
    val r = Schema.createRecord(n, null, null, false)
    if (fs != null) {
      r.setFields(fs.map { case (n, s) => field(n, s) }.toList.asJava)
    }
    r
  }

  def field(n: String, s: Schema) = new Schema.Field(n, s, null, null)
  def boolean() = Schema.create(Type.BOOLEAN)
  def int() = Schema.create(Type.INT)
  def long() = Schema.create(Type.LONG)
  def float() = Schema.create(Type.FLOAT)
  def double() = Schema.create(Type.DOUBLE)
  def string() = Schema.create(Type.STRING)
  def nul() = Schema.create(Type.NULL)
  def array(elms:Schema) = Schema.createArray(elms)
  def union(ss:Schema*) = Schema.createUnion(ss.asJava)
  def enum(n:String, ss:Seq[String]) = Schema.createEnum(n,null,null,ss.asJava)
}

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
    val r2 = TestUtils.reserialize(r)
    val ro = new StreamingOperations(L,inSchema).transformGenericRecord((null,r2)).get
    val ro2 = TestUtils.reserialize(ro._2)
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

    val r2 = TestUtils.reserialize(r)
    val ro = new StreamingOperations(lua,inSchema).transformGenericRecord((null,r2)).get
    val ro2 = TestUtils.reserialize(ro._2)

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

    val r2 = TestUtils.reserialize(r)
    val ro = new StreamingOperations(myLua,inSchema).transformGenericRecord((null,r2)).get
    val ro2 = TestUtils.reserialize(ro._2)

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