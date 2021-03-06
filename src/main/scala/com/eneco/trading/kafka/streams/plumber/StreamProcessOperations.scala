package com.eneco.energy.kafka.streams.plumber

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.KStream
import org.luaj.vm2.lib.jse.JsePlatform
import org.luaj.vm2.{LuaFunction, LuaValue}

import scala.collection.immutable.Seq
import scala.util.{Failure, Success, Try}

class LuaOperations(luaCode:String, luaTestCode:Option[String] = None) extends Logging {
  val LUA_FUNCTION_KEY = "f"
  val OPERATION_KEY = "fn"
  val PLUMBER_MAGIC_KEY = "plumber"
  val PLUMBER_MAGIC_VALUE = "awesome"
  val STEPS_KEY = "steps"
  val FILTER_OPERATION = "filter"
  val MAP_OPERATION = "map"
  val MAP_VALUES_OPERATION = "mapValues"
  val TEST_INPUTS_KEY = "test_inputs"
  val TEST_EXPECTATIONS_KEY = "test_expectations"

  val pbPreludeLua =
    s"""
      |local pb = {${PLUMBER_MAGIC_KEY}="${PLUMBER_MAGIC_VALUE}"}
      |
      |pb.${STEPS_KEY} = {}
      |pb.${TEST_INPUTS_KEY} = null
      |pb.${TEST_EXPECTATIONS_KEY} = null
      |
      |-- for testing
      |function pb.keyValue(k, v)
      |  return {key=k, value=v}
      |end
      |
      |-- for testing
      |function pb.value(v)
      |  return pb.keyValue(null, v)
      |end
      |
      |-- for testing
      |function pb.forInputs(t)
      |  pb.${TEST_INPUTS_KEY} = t
      |  return pb
      |end
      |
      |-- for testing
      |function pb.expectOutputs(t)
      |  pb.${TEST_EXPECTATIONS_KEY} = t
      |  return pb
      |end
      |
      |function pb.${FILTER_OPERATION}(f)
      |  table.insert(pb.steps, {${LUA_FUNCTION_KEY} = f, ${OPERATION_KEY} = "${FILTER_OPERATION}"})
      |  return pb
      |end
      |
      |function pb.${MAP_OPERATION}(f)
      |  table.insert(pb.steps, {${LUA_FUNCTION_KEY} = f, ${OPERATION_KEY} = "${MAP_OPERATION}"})
      |  return pb
      |end
      |
      |function pb.${MAP_VALUES_OPERATION}(f)
      |  table.insert(pb.steps, {${LUA_FUNCTION_KEY} = f, ${OPERATION_KEY} = "${MAP_VALUES_OPERATION}"})
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
    require(lt.get(PLUMBER_MAGIC_KEY).checkstring.tojstring == PLUMBER_MAGIC_VALUE) //magic
    luaTestCode.foreach(globals.load(_).call.checktable)
    lt
  }


  // extract the sequence of functional-style operations (e.g. filter, map) and the LuaFunction that is to be used
  def operations(): Seq[(String, LuaFunction)] = {
    val steps = plumberConfig.get(STEPS_KEY).checktable
    (1 to steps.length).map(n => {
      val s = steps.get(n).checktable()
      (s.get(OPERATION_KEY).checkstring.tojstring, s.get(LUA_FUNCTION_KEY).checkfunction)
    }).toSeq
  }

  def testingInputs(): Seq[(LuaValue, LuaValue)] = {
    plumberConfig.get(TEST_INPUTS_KEY) match {
      case v if v.isnil => Seq()
      case v => {
        val t = v.checktable
        (1 to t.length)
          .map(i => t.get(i).checktable)
          .map(kv => (kv.get("key"), kv.get("value")))
      }
    }
  }

  def testingExpectations(): Seq[(LuaValue, LuaValue)] = {
    plumberConfig.get(TEST_EXPECTATIONS_KEY) match {
      case v if v.isnil => Seq()
      case v => {
        val t = v.checktable
        (1 to t.length)
          .map(i => t.get(i).checktable)
          .map(kv => (kv.get("key"), kv.get("value")))
      }
    }
  }
}

object LuaUtil {
  def deepEq(a:LuaValue, b:LuaValue): Boolean = {
    if (a.`type`() != b.`type`())
      false

    if (a.istable) {
      val ta = a.checktable
      val tb = b.checktable
      ta.keys.forall(k => tb.keys.contains(k)) &&
        tb.keys.forall(k => ta.keys.contains(k)) &&
        a.checktable.keys.forall(k => deepEq(a.get(k), b.get(k)))
    } else {
      a.eq(b).toboolean
    }
  }
}

// Out-source the streaming operations to Lua
class StreamingOperations(luaOps:LuaOperations, keyValueType: KeyValueType) extends Logging {
  log.info("we output key-values of type " + keyValueType)

  val operations: Seq[(Option[(LuaValue, LuaValue)]) => Option[(LuaValue, LuaValue)]] = luaOps.operations
    .map { case (n, f) => n match {
      case "filter" => (k: LuaValue, v: LuaValue) => if (f.call(k, v).toboolean) Some(k, v) else None
      case "mapValues" => (k: LuaValue, v: LuaValue) => Some((k, f.call(v)))
      case "map" => (k: LuaValue, v: LuaValue) => {
        val retVals = f.invoke(k, v)
        require(retVals.narg == 2, "map is supposed to return 2 values")
        Some(retVals.arg(1), retVals.arg(2))
      }
    }
    }
    .map(f => (v: Option[(LuaValue, LuaValue)]) => v match {
      case Some((k, v)) => f(k, v)
      case _ => None
    })

  def verifyExpectationsForInput(in: Seq[(LuaValue, LuaValue)], exp: Seq[(LuaValue, LuaValue)]): Try[Unit] = {
    val res = in.flatMap(kv => transformLuaKeyValue(Some(kv)))
    if (res.length != exp.length) {
      Failure(new Exception(s"got ${res.length} outputs but expected ${exp.length}"))
    } else {
      if (res.zip(exp).forall { case ((lk, lv), (rk, rv)) => LuaUtil.deepEq(lk, rk) && LuaUtil.deepEq(lv, rv) }) {
        Success()
      } else {
        Failure(new Exception("output is not what was expected")) // TODO
      }
    }
  }

  def transformLuaKeyValue(keyValue0: Option[(LuaValue, LuaValue)]): Option[(LuaValue, LuaValue)] = operations
    .foldLeft(keyValue0)((keyValue, operation) => operation(keyValue))

  def transformObjects[Ko, Vo](in: (Object, Object)): Option[(Ko, Vo)] = {
    val ini: Option[(LuaValue, LuaValue)] = Some(objectToLuaValue(in._1), objectToLuaValue(in._2))
    transformLuaKeyValue(ini)
      .map { case (k, v) => (luaValueToObject(k, keyValueType.key).asInstanceOf[Ko], luaValueToObject(v, keyValueType.value).asInstanceOf[Vo]) }
  }

  def objectToLuaValue(o: Any): LuaValue = {
    if (o == null) {
      LuaValue.NIL
    } else o match {
      case g: GenericRecord => LuaMapper.recordToLua(g)
      case b: Boolean => LuaValue.valueOf(b)
      case s: String => LuaValue.valueOf(s)
      case i: Int => LuaValue.valueOf(i)
      case l: Long => LuaValue.valueOf(l)
      case _ => throw new Exception("x")
    }
  }

  def luaValueToObject(l: LuaValue, t: MappingType): Object =
    if (l.isnil) {
      null
    } else t match {
      case StringType => l.checkstring.tojstring
      case LongType => java.lang.Long.valueOf(l.checknumber.tolong)
      case AvroType(Some(schema)) => LuaMapper.luaToRecord(l.checktable, schema)
      case VoidType => null
      case _ => throw new Exception("y")
    }

  def transform(in: KStream[Any, Any]): KStream[Object, Object] = {
    in
      .map[LuaValue, LuaValue]((k, v) => {
      new KeyValue(objectToLuaValue(k), objectToLuaValue(v))
    })
      .map[String, Option[(LuaValue, LuaValue)]]((k, v) => {
      transformLuaKeyValue(Some(k, v)) match {
        case Some(kv) => new KeyValue(null, Some(kv))
        case _ => new KeyValue(null, None)
      }
    }).filter((_, kv) => kv match {
      case Some(kv) => true
      case _ => false
    }).map((_, kv) => kv match {
      case Some((k, v)) =>
        new KeyValue(luaValueToObject(k, keyValueType.key), luaValueToObject(v, keyValueType.value))
    })
  }
}
