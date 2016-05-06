package com.eneco.energy.kafka.streams.plumber

import java.io.ByteArrayOutputStream
import java.util

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericRecord, GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{DecoderFactory, EncoderFactory, BinaryEncoder}
import org.apache.avro.util.Utf8
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{FunSuite, Matchers}
import scala.collection.JavaConverters._

class ColumnNameDrivenMapperSmokeTest extends FunSuite with Matchers with MockFactory {
  val inSchema = new Parser().parse(
    """{
      |"type":"record",
      |"name":"UndesiredStructure",
      |"fields":[
      |{"name":"redundantField", "type": "long"},
      |{"name":"notValid", "type": "boolean"},
      |{"name":"fingers_lh", "type": "long"},
      |{"name":"fingers_rh", "type": "long"},
      |{"name":"dogs","type": {"type":"array", "items": "string"}},
      |{"name":"person", "type": {
      |	"type": "record",
      |	"name":"UndesiredSubStructure",
      |	"fields":[
      |        {"name": "name","type":"string"},
      |        {"name": "species","type":"string"}
      |        ]
      |    }}]
      |}
    """.stripMargin)

  val outSchema = new Parser().parse(
    """{
      |"type":"record",
      |"name":"DesiredStructure",
      |"fields":[
      |{"name":"valid", "type": "boolean"},
      |{"name": "name","type":"string"},
      |{"name":"fingers", "type": "long"}
      |]
      |}
    """.stripMargin)

  val myLua =
    """function process(undesired)
      | return {
      |		valid = not undesired.notValid,
      |		name = undesired.person.name:lower(),
      |		fingers = undesired.fingers_lh + undesired.fingers_rh
      |	}
      |end
    """.stripMargin

  def reserializeJ(rIn:GenericRecord): GenericRecord = {
    val schema = rIn.getSchema
    val bos = new ByteArrayOutputStream()
    val jenc = EncoderFactory.get().jsonEncoder(schema,bos,true)
    new GenericDatumWriter(schema).write(rIn, jenc)
    jenc.flush()
    val jstring = bos.toString()
    val reader = new GenericDatumReader[GenericRecord](schema)
    val dec = DecoderFactory.get().jsonDecoder(schema, jstring)
    reader.read(null, dec)
  }

  def reserialize(rIn:GenericRecord): GenericRecord = {
    val schema = rIn.getSchema
    val bos = new ByteArrayOutputStream()
    val benc = EncoderFactory.get().directBinaryEncoder(bos, null)
    new GenericDatumWriter(schema).write(rIn, benc)
    benc.flush()
    val reader = new GenericDatumReader[GenericRecord](schema)
    val dec = DecoderFactory.get().binaryDecoder(bos.toByteArray,0,bos.size(),null)
    reader.read(null, dec)
  }

  test("README example") {
    val rIn = new Record(inSchema)
    val rInPerson = new Record(inSchema.getField("person").schema)
    rInPerson.put("name",new Utf8("ROEL"))
    rInPerson.put("species",new Utf8("Rollulus rouloul"))
    rIn.put("redundantField",7L)
    rIn.put("notValid",false)
    rIn.put("fingers_lh",5L)
    rIn.put("fingers_rh",5L)
    rIn.put("person",rInPerson)

    val rInV = reserialize(rIn)
    val rOut = new StreamingOperations(myLua,outSchema).transformGenericRecord(rInV)
    val rOutV = reserialize(rOut)

    rOutV.get("valid") shouldBe true
    rOutV.get("fingers") shouldBe 10L
    rOutV.get("name").toString shouldBe "roel"
  }
}