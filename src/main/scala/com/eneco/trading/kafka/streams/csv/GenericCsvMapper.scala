package com.eneco.energy.kafka.streams.plumber

import java.io.File
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import com.eneco.energy.kafka.streams.plumber.Properties._
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.KStreamBuilder

object StreamProcessor {
  lazy val SOURCE_TOPIC_CONFIG = "source.topic"
  lazy val SINK_TOPIC_CONFIG = "sink.topic"
  lazy val SCHEMA_FILE_CONFIG = "schema.file"
  lazy val SCRIPT_FILE_CONFIG = "script.file"

  def readLuaScript(f: String): String = {
    new String(Files.readAllBytes(Paths.get(f)), Charset.defaultCharset)
  }

  def main(args: Array[String]): Unit = {
    // configure
    require(args.length > 0, "at least one .properties file should be given as program argument")
    val builder = new KStreamBuilder
    val cfg = propertiesFromFiles(args) | fixedProperties
    val sourceTopic = cfg.getProperty(SOURCE_TOPIC_CONFIG)
    val sinkTopic = cfg.getProperty(SINK_TOPIC_CONFIG)
    val luaFileName = cfg.getProperty(SCRIPT_FILE_CONFIG)
    val outputSchema = new Parser().parse(new File(cfg.getProperty(SCHEMA_FILE_CONFIG)))

    // source
    val in = builder.stream[String, GenericRecord](sourceTopic)

    // transformations
    val out = new StreamingOperations(readLuaScript(luaFileName), outputSchema).transform(in)

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

