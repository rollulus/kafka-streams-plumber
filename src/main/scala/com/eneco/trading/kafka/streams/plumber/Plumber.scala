package com.eneco.energy.kafka.streams.plumber

import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
import java.util.Properties

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
    log.info(s"This is Plumber ${GitRepositoryState.describe} (${GitRepositoryState.commit}) built at ${GitRepositoryState.build}")
    log.info(s"sourceTopic: ${a.sourceTopic}")
    log.info(s"sinkTopic: ${a.sinkTopic}")
    log.info(s"inputType: ${a.inputType}")
    log.info(s"outputType: ${a.outputType}")
    log.info(s"scriptFile: ${a.scriptFile}")
    log.info(s"propertiesFile: ${a.propertiesFile}")
    log.info(s"testFile: ${a.testFile}")
    log.info(s"dryRun: ${a.dryRun}")

    val luaOps = new LuaOperations(readLuaScript(a.scriptFile), a.testFile.map(readLuaScript))
    val streamingOps = new StreamingOperations(luaOps, a.outputType)

    // run test?
    if (a.testFile.isDefined) {
      val v = streamingOps.verifyExpectationsForInput(luaOps.testingInputs, luaOps.testingExpectations)
      if (v.isFailure) {
        log.error(v.get.toString)
        return v
      } else {
        val Some(f) = a.testFile
        log.info(s"OK, expectations provided in `${f}` were met")
      }
    }

    // dry run only?
    if (a.dryRun) {
      return Success()
    }

    // prepare
    val builder = new KStreamBuilder
    val cfg = propertiesFromFiles(a.propertiesFile)

    // source
    val in: KStream[Any, Any] = builder.stream(
      MappingType.toSerde(a.inputType.key, cfg, true).asInstanceOf[Serde[Any]],
      MappingType.toSerde(a.inputType.value, cfg, false).asInstanceOf[Serde[Any]],
      a.sourceTopic)

    // transformations
    val out = streamingOps.transform(in)

    // sinks
    out.to(
      MappingType.toSerde(a.outputType.key, cfg, true).asInstanceOf[Serde[Object]],
      MappingType.toSerde(a.outputType.value, cfg, false).asInstanceOf[Serde[Object]],
      a.sinkTopic)

    // run
    log.trace("giving control to kafka streams")
    val streams = new KafkaStreams(builder, cfg)
    sys.addShutdownHook({
      log.warn("shutdownHook triggered (SIGTERM/SIGINT received?); close streams")
      streams.close()
      log.info("sent close to streams")
    })
    streams.start()
    Success()
  }

  def parseProgramArgs(args: Array[String]): Option[Arguments] = {
    new OptionParser[Arguments]("plumber") {
      head("plumber", GitRepositoryState.describe)
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
          |<types> has the format "keytype,valuetype" or simply "valuetype", where
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
}

object GitRepositoryState {
  val props = {
    val p = new Properties()
    p.load(getClass.getClassLoader.getResourceAsStream("git.properties"))
    p
  }

  def describe: String = props.getProperty("git.commit.id.describe")
  def commit: String = props.getProperty("git.commit.id")
  def build: String = props.getProperty("git.build.time")
}
