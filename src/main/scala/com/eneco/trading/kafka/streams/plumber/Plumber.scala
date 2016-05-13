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
import scopt._

import scala.util.{Success, Try}

object Plumber extends Logging {
  case class Arguments(sourceTopic: String = null,
                       sinkTopic: String= null,
                       sinkSchema: String = null,
                       scriptFile: String= null,
                       propertiesFile: String= null,
                       testFile: Option[String] = None,
                       dryRun: Boolean = false)

  def exec(a:Arguments): Try[Unit] = {
    val outputSchema = new Parser().parse(new File((a.sinkSchema)))
    val luaOps = new LuaOperations(readLuaScript(a.scriptFile),a.testFile.map(readLuaScript))
    val streamingOps = new StreamingOperations(luaOps, outputSchema)

    if (a.testFile.isDefined) {
      val v = streamingOps.verifyExpectationsForInput(luaOps.testingInputs, luaOps.testingExpectations)
      if (v.isFailure) {
        log.error(v.get.toString)
        return v
      }
    }
    if (a.dryRun) {
      return Success()
    }

    val builder = new KStreamBuilder
    val cfg = propertiesFromFiles(a.propertiesFile) | fixedProperties

    // source
    val in = builder.stream[String, GenericRecord](a.sourceTopic)

    // transformations
    val out = streamingOps.transform(in)

    // sinks
    out.to(a.sinkTopic)

    // run
    new KafkaStreams(builder, cfg).start()
    Success()
  }

  def parseProgramArgs(args: Array[String]) = {
    new OptionParser[Arguments]("plumber") {
      head("plumber", "0.0.1")
      help("help") text ("prints this usage text.")

      opt[String]('i', "source") optional() action { (x, args) =>
        args.copy(sourceTopic = x)
      } text (s"source topic.")

      opt[String]('o', "sink") optional() action { (x, args) =>
        args.copy(sinkTopic = x)
      } text (s"sink topic.")

      opt[String]('s', "schema") required() action { (x, args) =>
        args.copy(sinkSchema = x)
      } text (s"sink Avro schema file, e.g. demo.avsc.")

      opt[String]('l', "script") required() action { (x, args) =>
        args.copy(scriptFile = x)
      } text (s"lua script to provide operations, e.g. demo.lua.")

      opt[String]('p', "properties") optional() action { (x, args) =>
        args.copy(propertiesFile = x)
      } text (s"properties file, e.g. demo.properties.")

      opt[String]('t', "test") optional() action { (x, args) =>
        args.copy(testFile = Some(x))
      } text (s"lua script file for test/verification pre-pass, e.g. demo.test.lua.")

      opt[Unit]('D', "dry-run") optional() action { (x, args) =>
        args.copy(dryRun = true)
      } text (s"dry-run, do no start streaming. Only makes sense in combination with -t.")

      checkConfig { c =>
        if ((c.sourceTopic == null || c.sinkTopic == null || c.propertiesFile == null) && !c.dryRun)
          failure("source and sink topics and properties must be provided for a none-dry run")
        else
          success
      }

    }.parse(args, Arguments())
  }

  def main(args: Array[String]): Unit = {
    parseProgramArgs(args) match {
      case Some(args) =>
        if (exec(args).isFailure) sys.exit(1)
      case None =>
        sys.exit(1)
    }
  }

  def readLuaScript(f: String): String = {
    new String(Files.readAllBytes(Paths.get(f)), Charset.defaultCharset)
  }

  def propertiesFromFiles(files: String*) = files.map(Properties.fromFile).foldLeft(new java.util.Properties)(_ | _)

  def fixedProperties() = Properties.create(Map(
    StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[GenericAvroSerializer[GenericRecord]],
    StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[GenericAvroDeserializer[GenericRecord]]
  ))

}

