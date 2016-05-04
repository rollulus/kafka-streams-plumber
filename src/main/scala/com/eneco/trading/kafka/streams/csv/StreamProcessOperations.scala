package com.eneco.energy.kafka.streams.plumber

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.kstream.KStream
import org.luaj.vm2.lib.jse.JsePlatform
import org.luaj.vm2.{LuaTable, LuaValue}

import scala.collection.JavaConverters._

class StreamingOperations(luaFileName:String, outputSchema:Schema) extends Logging {
  def recordToLua(r:GenericRecord): LuaTable ={
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

  def luaOntoRecord(l:LuaTable, r:Record): GenericRecord ={
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

  def luaToRecord(l:LuaTable, s:Schema): GenericRecord ={
    val r = new Record(outputSchema)
    luaOntoRecord(l, r)
  }

  def getLuaFunction(luaFileName:String): LuaValue = {
    val globals = JsePlatform.standardGlobals()
    val chunk = globals.loadfile(luaFileName).call()
    globals.get("process")
  }

  val luaFunction = getLuaFunction(luaFileName)

  def transform(in: KStream[String, GenericRecord]): KStream[String, GenericRecord] = {
    in.mapValues(inRawRecord=>{
      val inLuaRecord = recordToLua(inRawRecord)
      val outLuaRecord = luaFunction.call(inLuaRecord).checktable
      luaToRecord(outLuaRecord, outputSchema)
    })
  }
}
