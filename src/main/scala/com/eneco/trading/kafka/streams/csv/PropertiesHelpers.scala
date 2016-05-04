package com.eneco.energy.kafka.streams.plumber

import java.util

import scala.collection.JavaConverters._
import collection.JavaConversions._
import java.io.{StringReader, FileInputStream}

object Properties {
  def create(m: Map[String, _ <: AnyRef]) = {
    val ps = new java.util.Properties
    ps.putAll(m)
    ps
  }
  def lazyMerge(a: java.util.Properties, b:java.util.Properties) = {
    val ps = new java.util.Properties
    ps.putAll(b)
    ps.putAll(a)
    ps
  }
  def fromFile(filename: String) = {
    val ps = new java.util.Properties()
    ps.load(new FileInputStream(filename))
    ps
  }
  def fromString(s: String) = {
    val ps = new java.util.Properties()
    ps.load(new StringReader(s))
    ps
  }

  implicit class MyProperties(p: java.util.Properties) {
    def |(q: java.util.Properties) = Properties.lazyMerge(p, q)
    implicit def toHashMap(): java.util.HashMap[String, Any] = {
      val m = new util.HashMap[String, Any]
      p.propertyNames().asScala.map(_.toString).foreach(k => m.put(k,p.getProperty(k)))
      m
    }
  }

}

