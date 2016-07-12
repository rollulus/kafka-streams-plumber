package com.eneco.energy.kafka.streams.plumber

import java.util

import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.kafka.common.Configurable
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

class GenericAvroSerde[T] extends Serde[T] {
  val s = new GenericAvroSerializer[T]
  val d = new GenericAvroDeserializer[T]

  def configure(map: util.Map[String, _], isKey: Boolean) = {
    s.configure(map, isKey)
    d.configure(map, isKey)
  }

  def close = {
    s.close
    d.close
  }

  def serializer : Serializer[T] = s
  def deserializer : Deserializer[T] = d
}

// NOTE: Must have a public no-argument constructor (org.apache.kafka.common.utils.Utils.newInstance)
class GenericAvroSerializer[T]() extends Serializer[T] with Configurable {
  private val inner: KafkaAvroSerializer = new KafkaAvroSerializer()

  def this(map: util.Map[String, _]) {
    this()
    configure(map)
  }

  def configure(map: util.Map[String, _], isKey: Boolean): Unit = {
    inner.configure(map, isKey)
  }

  def serialize(t: String, v: T): Array[Byte] = {
    inner.serialize(t, v)
  }

  def close(): Unit = inner.close

  def configure(map: util.Map[String, _]): Unit = {
    inner.configure(map, false)
  }
}

// NOTE: Must have a public no-argument constructor (org.apache.kafka.common.utils.Utils.newInstance)
class GenericAvroDeserializer[T]() extends Deserializer[T] with Configurable {
  private val inner: KafkaAvroDeserializer = new KafkaAvroDeserializer()

  def this(map: util.Map[String, _]) {
    this()
    configure(map)
  }

  def configure(map: util.Map[String, _], b: Boolean): Unit = {
    inner.configure(map, b)
  }

  override def deserialize(s: String, var2: Array[Byte]): T = {
    inner.deserialize(s, var2).asInstanceOf[T]
  }

  def close(): Unit = inner.close

  def configure(map: util.Map[String, _]): Unit = {
    inner.configure(map, false)
  }
}
