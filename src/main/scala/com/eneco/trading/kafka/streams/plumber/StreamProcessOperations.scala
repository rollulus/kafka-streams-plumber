package com.eneco.energy.kafka.streams.plumber

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.KStream
import org.luaj.vm2.lib.jse.JsePlatform
import org.luaj.vm2.{LuaFunction, LuaValue}

import scala.collection.immutable.Seq

class LuaOperations(luaCode:String) extends Logging {
  val pbPreludeLua =
    """local pb = {plumber="awesome"} -- magic
      |
      |pb.steps = {}
      |
      |function pb.filter(f)
      |  table.insert(pb.steps, {f = f, fn = "filter"})
      |  return pb
      |end
      |
      |function pb.mapValues(f)
      |  table.insert(pb.steps, {f = f, fn = "mapValues"})
      |  return pb
      |end
      |
      |return pb
    """.stripMargin

  val plumberConfig = {
    val globals = JsePlatform.standardGlobals()
    val pbPrelude = globals.load(pbPreludeLua).call().checktable()
    globals.set("pb", pbPrelude)
    val lt = globals.load(luaCode).call().checktable()
    require(lt.get("plumber").checkstring.tojstring == "awesome") //magic
    lt
  }

  // extract the sequence of functional-style operations (e.g. filter, map) and the LuaFunction that is to be used
  def getOperations(): Seq[(String, LuaFunction)] = {
    val steps = plumberConfig.get("steps").checktable
    (1 to steps.length).map(n => {
      val s = steps.get(n).checktable()
      (s.get("fn").checkstring.tojstring, s.get("f").checkfunction)
    }).toSeq
  }
}

// Out-source the streaming operations to Lua
class StreamingOperations(luaOps:LuaOperations, outputSchema:Schema) extends Logging {
  val operations: Seq[(Option[(LuaValue, LuaValue)]) => Option[(LuaValue, LuaValue)]] = luaOps.getOperations
    .map { case (n, f) => n match {
      case "filter" => (k: LuaValue, v: LuaValue) => if (f.call(k, v).toboolean) Some(k, v) else None
      case "mapValues" => (k: LuaValue, v: LuaValue) => Some((k, f.call(v)))
    }
    }
    .map(f => (v: Option[(LuaValue, LuaValue)]) => v match {
      case Some((k, v)) => f(k, v)
      case _ => None
    })

  def transformLuaKeyvalue(keyValue0: Option[(LuaValue, LuaValue)]) = operations
    .foldLeft(keyValue0)((keyValue, operation) => operation(keyValue))

  def transformGenericRecord(in: (String, GenericRecord)): Option[(String, GenericRecord)] = {
    val ini: Option[(LuaValue, LuaValue)] = Some((if (in._1 == null) LuaValue.NIL else LuaValue.valueOf(in._1), LuaMapper.recordToLua(in._2)))
    transformLuaKeyvalue(ini)
      .map { case (k, v) => (k.tojstring, LuaMapper.luaToRecord(v.checktable, outputSchema)) }
  }

  def transform(in: KStream[String, GenericRecord]): KStream[String, GenericRecord] = {
    in
      .map[LuaValue, LuaValue]((k, v) => new KeyValue(if (k == null) LuaValue.NIL else LuaValue.valueOf(k), LuaMapper.recordToLua(v)))
      .map[String, Option[(LuaValue, LuaValue)]]((k, v) => {
      transformLuaKeyvalue(Some(k, v)) match {
        case Some(kv) => new KeyValue(null, Some(kv))
        case _ => new KeyValue(null, None)
      }
    }).filter((_, kv) => kv match {
      case Some(kv) => true
      case _ => false
    }).map((_, kv) => kv match {
      case Some((k, v)) => new KeyValue(k.tojstring, LuaMapper.luaToRecord(v.checktable, outputSchema))
    })
  }
}
