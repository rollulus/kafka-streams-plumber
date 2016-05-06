package com.eneco.energy.kafka.streams.plumber

import java.util

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.kstream.KStream
import org.luaj.vm2.lib.jse.JsePlatform
import org.luaj.vm2.{LuaTable, LuaValue}

import scala.collection.JavaConverters._

class StreamingOperations(luaCode:String, outputSchema:Schema) extends Logging {
  // (scala/java object, schema) -> lua
  def objectToLuaValue(jv:Object, s:Schema): LuaValue = s.getType match {
    case Type.DOUBLE => LuaValue.valueOf(jv.asInstanceOf[Double])
    case Type.FLOAT => LuaValue.valueOf(jv.asInstanceOf[Float])
    case Type.STRING => LuaValue.valueOf(jv.asInstanceOf[org.apache.avro.util.Utf8].toString)
    case Type.INT => LuaValue.valueOf(jv.asInstanceOf[Int])
    case Type.LONG => LuaValue.valueOf(jv.asInstanceOf[Long])
    case Type.BOOLEAN => LuaValue.valueOf(jv.asInstanceOf[Boolean])
    case Type.ARRAY => arrayToLuaValue(jv.asInstanceOf[org.apache.avro.generic.GenericArray[Object]], s.getElementType)
    case Type.RECORD => recordToLua(jv.asInstanceOf[GenericRecord])
    case Type.UNION => unionToLua(jv, s)
    case _ => println(s.toString(true)); throw new NotImplementedError()
  }

  // TODO: full impl
  def unionToLua(jv:Object, s:Schema): LuaValue = {
    require(s.getType == Type.UNION)
    require(s.getTypes.size == 2,"only [null,T] unions are allowed atm TODO")
    require(s.getTypes.get(0).getType == Type.NULL,"only [null,T] unions are allowed atm TODO")
    if (jv == null) {
      LuaValue.NIL
    } else {
      objectToLuaValue(jv, s.getTypes.get(1))
    }
  }

  def arrayToLuaValue(jvs:java.util.Collection[Object], s:Schema): LuaValue = {
    val t = LuaValue.tableOf
    jvs.asScala.map(objectToLuaValue(_,s)).zipWithIndex.foreach{case (v,i) => t.set(i+1,v)}
    t
  }

  def recordToLua(r:GenericRecord): LuaTable ={
    val t = new LuaTable()
    r.getSchema.getFields.asScala.foreach(f=>{
      val lv = objectToLuaValue(r.get(f.name), f.schema())
      t.set(f.name, lv)
    })
    t
  }

  // java util collection
  def luaTableToArray(lv:LuaValue, elms:Schema): util.Collection[Any] = {
    val lt = lv.checktable()
    (1 to lt.length).map(i => luaValueToObject(lt.get(i),elms)).asJavaCollection
  }

  // TODO: full impl
  def luaValueToUnion(lv:LuaValue, s:Schema): Any = {
    require(s.getType == Type.UNION)
    require(s.getTypes.size == 2,"only [null,T] unions are allowed atm TODO")
    require(s.getTypes.get(0).getType == Type.NULL,"only [null,T] unions are allowed atm TODO")
    if (lv.isnil) {
      null
    } else {
      luaValueToObject(lv,s.getTypes.get(1))
    }
  }

  def luaValueToObject(lv:LuaValue, s:Schema): Any = {
    if (lv.isnil) null
    else s.getType match {
      case Type.DOUBLE => lv.todouble()
      case Type.FLOAT => lv.tofloat()
      case Type.STRING => require(lv.isstring); lv.tojstring()
      case Type.INT => lv.toint()
      case Type.LONG => lv.tolong()
      case Type.BOOLEAN => lv.toboolean()
      case Type.ARRAY => luaTableToArray(lv, s.getElementType)
      case Type.RECORD => luaToRecord(lv.checktable, s)
      case Type.UNION => luaValueToUnion(lv, s)
      case _ => println(s.getType.getName); throw new NotImplementedError()
    }
  }

  def luaOntoRecord(l:LuaTable, r:GenericRecord): GenericRecord ={
    r.getSchema.getFields.asScala.foreach(f=>{
      val lv = l.get(f.name)
      r.put(f.name, luaValueToObject(lv, f.schema))
    })
    r
  }

  def luaToRecord(l:LuaTable, s:Schema): GenericRecord ={
    val r = new Record(s)
    luaOntoRecord(l, r)
  }

  def getLuaFunction(luaCode:String): LuaValue = {
    val globals = JsePlatform.standardGlobals()
    val chunk = globals.load(luaCode).call()
    globals.get("process")
  }

  val luaFunction = getLuaFunction(luaCode)

  def transformGenericRecord(inRawRecord:GenericRecord) = {
    val inLuaRecord = recordToLua(inRawRecord)
    val outLuaRecord = luaFunction.call(inLuaRecord).checktable
    luaToRecord(outLuaRecord, outputSchema)
  }

  def transform(in: KStream[String, GenericRecord]): KStream[String, GenericRecord] = {
    in.mapValues(transformGenericRecord)
  }
}
