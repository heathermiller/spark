/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.serializer

import java.io._

import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.{Utils, ByteBufferInputStream, NextIterator}

/**
 * :: DeveloperApi ::
 * A serializer. Because some serialization libraries are not thread safe, this class is used to
 * create [[org.apache.spark.serializer.SerializerInstance]] objects that do the actual
 * serialization and are guaranteed to only be called from one thread at a time.
 *
 * Implementations of this trait should implement:
 *
 * 1. a zero-arg constructor or a constructor that accepts a [[org.apache.spark.SparkConf]]
 * as parameter. If both constructors are defined, the latter takes precedence.
 *
 * 2. Java serialization interface.
 *
 * Note that serializers are not required to be wire-compatible across different versions of Spark.
 * They are intended to be used to serialize/de-serialize data within a single Spark application.
 */
@DeveloperApi
abstract class Serializer {

  /**
   * Default ClassLoader to use in deserialization. Implementations of [[Serializer]] should
   * make sure it is using this when set.
   */
  @volatile protected var defaultClassLoader: Option[ClassLoader] = None

  /**
   * Sets a class loader for the serializer to use in deserialization.
   *
   * @return this Serializer object
   */
  def setDefaultClassLoader(classLoader: ClassLoader): Serializer = {
    defaultClassLoader = Some(classLoader)
    this
  }

  /** Creates a new [[SerializerInstance]]. */
  def newInstance(): SerializerInstance
}


@DeveloperApi
object Serializer {
  def getSerializer(serializer: Serializer): Serializer = {
    if (serializer == null) SparkEnv.get.serializer else serializer
  }

  def getSerializer(serializer: Option[Serializer]): Serializer = {
    serializer.getOrElse(SparkEnv.get.serializer)
  }
}


/**
 * :: DeveloperApi ::
 * An instance of a serializer, for use by one thread at a time.
 */
@DeveloperApi
abstract class SerializerInstance {
  def serialize[T: ClassTag](t: T): ByteBuffer

  def deserialize[T: ClassTag](bytes: ByteBuffer): T

  def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T

  def serializeStream(s: OutputStream): SerializationStream

  def deserializeStream(s: InputStream): DeserializationStream
}

/**
 * :: DeveloperApi ::
 * A stream for writing serialized objects.
 */
@DeveloperApi
abstract class SerializationStream {
  def writeObject[T: ClassTag](t: T): SerializationStream
  def flush(): Unit
  def close(): Unit

  def startIteration(): Unit = {}
  def endIteration(): Unit = {}

  def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
    startIteration()
    while (iter.hasNext) {
      writeObject(iter.next())
    }
    endIteration()
    this
  }
}


/**
 * :: DeveloperApi ::
 * A stream for reading serialized objects.
 */
@DeveloperApi
abstract class DeserializationStream {
  def readObject[T: ClassTag](): T
  def close(): Unit

  def startIteration(): Unit = {}
  def endIteration(): Unit = {}

  def approxTypeOf(t: Any): String = {
    if (t == null) "null"
    else if (t.isInstanceOf[Tuple2[_, _]]) {
      val tup = t.asInstanceOf[Tuple2[Any, Any]]

      val fstTypeName = {
        val tupOne = tup._1
        if (tupOne == null) "null"
        else if (tupOne.isInstanceOf[Tuple2[_, _]]) {
          val fstTup = tupOne.asInstanceOf[Tuple2[Any, Any]]
          s"(${fstTup._1.getClass.getName},${fstTup._2.getClass.getName})"
        } else tupOne.getClass.getName
      }

      val sndTypeName = {
        val tupTwo = tup._2
        if (tupTwo == null) "null"
        else if (tupTwo.isInstanceOf[Tuple2[_, _]]) {
          val sndTup = tupTwo.asInstanceOf[Tuple2[Any, Any]]
          s"(${sndTup._1.getClass.getName},${sndTup._2.getClass.getName})"
        } else tupTwo.getClass.getName
      }

      s"($fstTypeName,$sndTypeName)"
    } else
      t.getClass.getName
  }

  /**
   * Read the elements of this stream through an iterator. This can only be called once, as
   * reading each element will consume data from the input source.
   */
  def asIterator: Iterator[Any] = new NextIterator[Any] {
    var cnt = 0
    // println(s"@@@ DeserializationStream.asIterator")
    var prev: Any = _

    override protected def getNext() = {
      try {
        if (cnt == 0) startIteration()
        cnt += 1
        prev = readObject[Any]()
        prev
      } catch {
        case eof: EOFException =>
          // if (cnt > 500)
          //   println(s"@@@@ DeserializationStream finished: $cnt, ${approxTypeOf(prev)}")
          finished = true
          endIteration()
      }
    }

    override protected def close() {
      DeserializationStream.this.close()
    }
  }
}
