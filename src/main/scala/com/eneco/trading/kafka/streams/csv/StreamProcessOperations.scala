package com.eneco.energy.kafka.streams.plumber

import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.streams.kstream.KStream
import scala.collection.JavaConverters._
import scala.util.{Failure, Try}
import org.apache.avro.Schema

class StreamingOperations() extends Logging {
  def transform(csvRecords: KStream[String, GenericRecord]) = csvRecords.mapValues(v=>"x")
}
