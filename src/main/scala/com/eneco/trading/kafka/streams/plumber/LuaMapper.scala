package com.eneco.energy.kafka.streams.plumber

import java.util

import org.apache.avro.{Schema, UnresolvedUnionException}
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.luaj.vm2._

import scala.collection.JavaConverters._

// Translate Lua <-> GenericRecord
object LuaMapper {
  // (scala/java object, schema) -> lua
  def objectToLuaValue(jv: Object, s: Schema): LuaValue = s.getType match {
    case Type.DOUBLE => LuaValue.valueOf(jv.asInstanceOf[Double])
    case Type.FLOAT => LuaValue.valueOf(jv.asInstanceOf[Float])
    case Type.STRING => LuaValue.valueOf(jv.asInstanceOf[org.apache.avro.util.Utf8].toString)
    case Type.INT => LuaValue.valueOf(jv.asInstanceOf[Int])
    case Type.LONG => LuaValue.valueOf(jv.asInstanceOf[Long])
    case Type.BOOLEAN => LuaValue.valueOf(jv.asInstanceOf[Boolean])
    case Type.ARRAY => arrayToLuaValue(jv.asInstanceOf[org.apache.avro.generic.GenericArray[Object]], s.getElementType)
    case Type.RECORD => recordToLua(jv.asInstanceOf[GenericRecord])
    case Type.UNION => unionToLua(jv, s)
    case Type.ENUM => enumToLua(jv, s)
    case _ => println(s.toString(true)); throw new NotImplementedError()
  }

  def enumToLua(jv: Object, s: Schema): LuaValue = {
    LuaValue.valueOf(jv.asInstanceOf[GenericData.EnumSymbol].toString)
  }

  def unionToLua(jv: Object, s: Schema): LuaValue = {
    require(s.getType == Type.UNION)
    if (jv == null) {
      LuaValue.NIL
    } else if (s.getTypes.get(0).getType == Type.NULL && s.getTypes.size == 2) {
      // This is the most common use case, so short circuit it.
      objectToLuaValue(jv, s.getTypes.get(1))
    } else {
      objectToLuaValue(jv, s.getTypes.get(GenericData.get().resolveUnion(s, jv)))
    }
  }

  def arrayToLuaValue(jvs: java.util.Collection[Object], s: Schema): LuaValue = {
    val t = LuaValue.tableOf
    jvs.asScala.map(objectToLuaValue(_, s)).zipWithIndex.foreach { case (v, i) => t.set(i + 1, v) }
    t
  }

  def recordToLua(r: GenericRecord): LuaTable = {
    val t = new LuaTable()
    r.getSchema.getFields.asScala.foreach(f => {
      val lv = objectToLuaValue(r.get(f.name), f.schema())
      t.set(f.name, lv)
    })
    t
  }

  // java util collection
  def luaTableToArray(lv: LuaValue, elms: Schema): util.Collection[Any] = {
    val lt = lv.checktable()
    (1 to lt.length).map(i => luaValueToObject(lt.get(i), elms)).asJavaCollection
  }

  def luaValueToUnion(lv: LuaValue, s: Schema): Any = {
    require(s.getType == Type.UNION)
    if (lv.isnil) {
      null
    } else if (s.getTypes.get(0).getType == Type.NULL && s.getTypes.size == 2) {
      // This is the most common use case, so short circuit it.
      luaValueToObject(lv, s.getTypes.get(1))
    } else {
      val lvTypes = s.getTypes.asScala.filter(t => isLuaInstanceOf(lv, t))
      if (!lvTypes.isEmpty) luaValueToObject(lv, lvTypes.head) else throw new UnresolvedUnionException(s, lv)
    }
  }

  def isLuaInstanceOf(lv: LuaValue, s: Schema): Boolean = s.getType() match {
    case Type.DOUBLE => lv.isnumber() && !lv.isinttype()
    // There is no lua float type (only double) so
    // which is chosen depends on union ordering
    case Type.FLOAT => lv.isnumber() && !lv.isinttype()
    case Type.STRING => lv.isstring() && !lv.isnumber() // LuaNumbers can be resolved to strings...
    case Type.INT => lv.isint()
    case Type.LONG => lv.islong()
    case Type.BOOLEAN => lv.isboolean()
    case Type.RECORD => lv.istable()
    case _ => false
  }

  def luaValueToEnum(lv: LuaValue, s: Schema): Any = {
    require(s.getType == Type.ENUM)
    require(lv.isstring)
    new GenericData.EnumSymbol(s, lv.tojstring)
  }

  def luaValueToObject(lv: LuaValue, s: Schema): Any = {
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
      case Type.ENUM => luaValueToEnum(lv, s)
      case _ => println(s.getType.getName); throw new NotImplementedError()
    }
  }

  def luaOntoRecord(l: LuaTable, r: GenericRecord): GenericRecord = {
    r.getSchema.getFields.asScala.foreach(f => {
      val lv = l.get(f.name)
      r.put(f.name, luaValueToObject(lv, f.schema))
    })
    r
  }

  def luaToRecord(l: LuaTable, s: Schema): GenericRecord = {
    val r = new Record(s)
    luaOntoRecord(l, r)
  }
}
