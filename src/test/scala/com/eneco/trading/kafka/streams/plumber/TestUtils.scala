package com.eneco.energy.kafka.streams.plumber

import java.io.ByteArrayOutputStream

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
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

  def process[K <: Object,V<: Object](myLua:String, in:(Object,Object), kvType:KeyValueType): Option[(K, V)] = {
    val rkv = (TestUtils.reserializeObj(in._1), TestUtils.reserializeObj(in._2))
    new StreamingOperations(new LuaOperations(myLua), kvType).transformObjects[K, V](rkv)
      .map{case (k,v) => (TestUtils.reserializeObj(k).asInstanceOf[K], TestUtils.reserializeObj(v).asInstanceOf[V])}
  }
}

object TestUtils {
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

  def reserializeObj(o:Object):Object = {
    o match {
      case g: GenericRecord => TestUtils.reserialize(g)
      case _ => o
    }
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

}