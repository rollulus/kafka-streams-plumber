package com.eneco.energy.kafka.streams.plumber

import java.io.File
import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serdes._
import com.eneco.energy.kafka.streams.plumber.Properties._

sealed trait MappingType
object LongType extends MappingType
object StringType extends MappingType
object VoidType extends MappingType
case class AvroType(val schema:Option[Schema]) extends MappingType

case class KeyValueType(val key:MappingType,val value:MappingType)

object MappingType {
  def fromString(s: String) = {
    val s2mt = Map("long" -> LongType, "string" -> StringType, "void" -> VoidType, "avro" -> AvroType(None))
    s2mt.getOrElse(s, {
      val KvPattern = "^avro=([^,]+)$".r
      val KvPattern(f) = s
      AvroType(Some(new Parser().parse(new File(f))))
    })
  }

  def toSerde(t: MappingType, ps: Properties, isKey: Boolean) = {
    val s = t match {
      case LongType => new LongSerde
      case StringType => new StringSerde
      case AvroType(_) => new GenericAvroSerde[GenericRecord]
      case VoidType => new StringSerde //TODO: create void serializer
    }
    s.configure(ps.toHashMap(), isKey)
    s
  }
}

object KeyValueType {
  def fromString(s: String) = {
    val KvPattern = "^([^,]+),([^,]+)$".r
    val VPattern = "^([^,]+)$".r
    s match {
      case KvPattern(k, v) => KeyValueType(MappingType.fromString(k), MappingType.fromString(v))
      case VPattern(v) => KeyValueType(VoidType, MappingType.fromString(v))
    }
  }
}

