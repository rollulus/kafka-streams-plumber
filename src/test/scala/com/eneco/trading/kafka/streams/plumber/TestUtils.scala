package com.eneco.energy.kafka.streams.plumber

import java.io.ByteArrayOutputStream

import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

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