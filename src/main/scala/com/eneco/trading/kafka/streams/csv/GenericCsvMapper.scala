package com.eneco.energy.kafka.streams.plumber

import java.io.File
import java.util.Properties
import org.apache.avro.Schema
import org.apache.avro.Schema.{Type, Parser}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}
import org.apache.kafka.streams._
import Properties._
import io.confluent.kafka.serializers.{KafkaAvroSerializer}
import org.apache.kafka.common.Configurable
import org.apache.kafka.common.serialization.{Serializer}
import java.util
import org.luaj.vm2.{LuaValue, LuaTable}
import org.luaj.vm2.lib.jse.{CoerceLuaToJava, CoerceJavaToLua, JsePlatform}
import collection.JavaConverters._

import scala.util.matching.Regex

object StreamProcessor {
  lazy val SOURCE_TOPIC_CONFIG = "source.topic"
  lazy val SINK_TOPIC_CONFIG = "sink.topic"
  lazy val SCHEMA_FILE_CONFIG = "schema.file"

  def recordToLua(r:GenericRecord): LuaTable ={
    sel = sel + 1
    val t = new LuaTable()
    r.getSchema.getFields.asScala.foreach(f=>{
    val jv = r.get(f.name)
      val lv = f.schema().getType match {
      case Type.DOUBLE => LuaValue.valueOf(jv.asInstanceOf[Double])
      case Type.FLOAT => LuaValue.valueOf(jv.asInstanceOf[Float])
      case Type.STRING => LuaValue.valueOf(jv.asInstanceOf[org.apache.avro.util.Utf8].toString)
      case Type.INT => LuaValue.valueOf(jv.asInstanceOf[Int])
      case Type.LONG => LuaValue.valueOf(jv.asInstanceOf[Long])
      case Type.BOOLEAN => LuaValue.valueOf(jv.asInstanceOf[Boolean])
      case Type.RECORD => recordToLua(jv.asInstanceOf[GenericRecord])
      case _ => println(f.schema().getType.getName); throw new NotImplementedError()
    }
    t.set(f.name, lv)
    })
    t
  }

  var sel=0

  def luaToRecord(l:LuaTable, r:Record): Record ={
    sel = sel + 1
    r.getSchema.getFields.asScala.foreach(f=>{
      val lv = l.get(f.name)
       val jv = f.schema().getType match {
        case Type.DOUBLE => lv.todouble()
        case Type.FLOAT => lv.tofloat()
        case Type.STRING => lv.tojstring()
        case Type.INT => lv.toint()
        case Type.LONG => lv.tolong()
        case Type.BOOLEAN => lv.toboolean()
        case _ => println(f.schema().getType.getName); throw new NotImplementedError()
      }
      r.put(f.name, jv)
    })
    r
  }

  def main(args: Array[String]): Unit = {
    // configure
    require(args.length > 0, "at least one .properties file should be given as program argument")
    val builder = new KStreamBuilder
    val cfg = propertiesFromFiles(args) | fixedProperties
    val sourceTopic = cfg.getProperty(SOURCE_TOPIC_CONFIG)
    val sinkTopic = cfg.getProperty(SINK_TOPIC_CONFIG)
    val sche = new Parser().parse(new File(cfg.getProperty(SCHEMA_FILE_CONFIG)))
    //sche.getFields



      val myCode =
        """function process(undesired)
          |    return {
          |      valid = not undesired.notValid,
          |      name = undesired.person.name:lower(),
          |      fingers = undesired.fingers_lh + undesired.fingers_rh
          |    }
          |end
        """.stripMargin

        val globals = JsePlatform.standardGlobals()
          val chunk = globals.load(myCode).call()
        val f = globals.get("process")


    // source
    val in = builder.stream[String, GenericRecord](sourceTopic)

    // transformations
    val out = in.mapValues(xxx=>{
      var rrrr:Record = null
      var i = 0
      val t0 = System.nanoTime()
      for(i <- 1 to 1000000)
      {
        val lv = recordToLua(xxx)
        val v = f.call(lv)
        val t = v.checktable()
        val r = new Record(sche)
        rrrr = luaToRecord(t, r)
      }
      val t1 = System.nanoTime()
      println("Elapsed time: " + (t1 - t0) + "ns")
      rrrr
    })

    // sinks
    out.to(sinkTopic)

    // run
    new KafkaStreams(builder, cfg).start()
  }

  def propertiesFromFiles(files: Array[String]) = files.map(Properties.fromFile).foldLeft(new java.util.Properties)(_ | _)

  def fixedProperties() = Properties.create(Map(
    StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[GenericAvroSerializer[GenericRecord]],
    StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[GenericAvroDeserializer[GenericRecord]]
  ))

}

