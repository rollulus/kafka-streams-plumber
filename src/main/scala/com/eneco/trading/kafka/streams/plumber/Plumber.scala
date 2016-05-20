package com.eneco.energy.kafka.streams.plumber

import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import com.eneco.energy.kafka.streams.plumber.Properties._
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}
import scopt._

import scala.util.{Success, Try}

object Plumber extends Logging {

  case class Arguments(sourceTopic: String = null,
                       sinkTopic: String = null,
                       inputType: KeyValueType = null,
                       outputType: KeyValueType = null,
                       scriptFile: String = null,
                       propertiesFile: String = null,
                       testFile: Option[String] = None,
                       dryRun: Boolean = false)



  def exec(a: Arguments): Try[Unit] = {
    val luaOps = new LuaOperations(readLuaScript(a.scriptFile), a.testFile.map(readLuaScript))
    val streamingOps = new StreamingOperations(luaOps, a.outputType)

    // run test?
    if (a.testFile.isDefined) {
      val v = streamingOps.verifyExpectationsForInput(luaOps.testingInputs, luaOps.testingExpectations)
      if (v.isFailure) {
        log.error(v.get.toString)
        return v
      }
    }

    // dry run only?
    if (a.dryRun) {
      return Success()
    }

    // prepare
    val builder = new KStreamBuilder
    val cfg = propertiesFromFiles(a.propertiesFile) | fixedProperties //| serializerProperties(a.inputType, a.outputType)

    // source
    val in: KStream[Any, Any] = builder.stream(
      MappingType.toDeserializer(a.inputType.key, cfg, true).asInstanceOf[Deserializer[Any]],
      MappingType.toDeserializer(a.inputType.value, cfg, false).asInstanceOf[Deserializer[Any]],
      a.sourceTopic)

    // transformations
    val out = streamingOps.transform(in)

    // sinks
    out.to(
      a.sinkTopic,
      MappingType.toSerializer(a.outputType.key, cfg, true).asInstanceOf[Serializer[Object]],
      MappingType.toSerializer(a.outputType.value, cfg, false).asInstanceOf[Serializer[Object]])

    // run
    new KafkaStreams(builder, cfg).start()
    Success()
  }

  def parseProgramArgs(args: Array[String]) = {
    new OptionParser[Arguments]("plumber") {
      head("plumber", "0.0.2")
      help("help") text "prints this usage text."

      opt[String]('i', "source") optional() valueName "<topic>" action { (x, args) =>
        args.copy(sourceTopic = x)
      } text "source topic."

      opt[String]('o', "sink") optional() valueName "<topic>" action { (x, args) =>
        args.copy(sinkTopic = x)
      } text "sink topic."

      opt[String]('d', "deserialize") optional() valueName "<types>" action { (x, args) =>
        args.copy(inputType = KeyValueType.fromString(x))
      } text "how to deserialize input messages."

      opt[String]('s', "serialize") required() valueName "<types>" action { (x, args) =>
        args.copy(outputType = KeyValueType.fromString(x))
      } text "how to serialize output messages."

      opt[String]('l', "script") required() valueName "<file>" action { (x, args) =>
        args.copy(scriptFile = x)
      } text "lua script to provide operations, e.g. demo.lua."

      opt[String]('p', "properties") optional() valueName "<file>" action { (x, args) =>
        args.copy(propertiesFile = x)
      } text "properties file, e.g. demo.properties."

      opt[String]('t', "test") optional() valueName "<file>" action { (x, args) =>
        args.copy(testFile = Some(x))
      } text "lua script file for test/verification pre-pass, e.g. demo.test.lua."

      opt[Unit]('D', "dry-run") optional() action { (x, args) =>
        args.copy(dryRun = true)
      } text "dry-run, do no start streaming. Only makes sense in combination with -t."

      note(
        """
          |<types> has the format "keytype:valuetype" or simply "valuetype", where
          |the type can be long, string, avro or void. In case of type avro, one can
          |optionally give a schema file: avro=file.avsc.
          |
          |Example:
          |
          |plumber -l toy.lua -i source -o sink -p my.properties -d string,avro -s string,avro=out.avsc
          |
        """.stripMargin)

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

  // TODO: is work around Streams API, revisit in 0.10?
  def fixedProperties() = Properties.create(Map(
    StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[LongSerializer],
    StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[LongDeserializer],
    StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[LongSerializer],
    StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[LongDeserializer]
  ))

}

