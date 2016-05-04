package com.eneco.energy.kafka.streams.plumber

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.util.Utf8
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{FunSuite, Matchers}

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
      |	return {
      |		valid = not undesired.notValid,
      |		name = undesired.person.name:lower(),
      |		fingers = undesired.fingers_lh + undesired.fingers_rh
      |	}
      |end
    """.stripMargin

  test("no smoke") {
    val rIn = new Record(inSchema)
    val rInPerson = new Record(inSchema.getField("person").schema)
    rInPerson.put("name",new Utf8("ROEL"))
    rInPerson.put("species",new Utf8("Rollulus rouloul"))
    rIn.put("redundantField",7L)
    rIn.put("notValid",false)
    rIn.put("fingers_lh",5L)
    rIn.put("fingers_rh",5L)
    rIn.put("person",rInPerson)

    val rOut = new StreamingOperations(myLua,outSchema).transformGenericRecord(rIn)

    rOut.get("valid") shouldBe true
    rOut.get("fingers") shouldBe 10L
    rOut.get("name") shouldBe "roel"
  }
}