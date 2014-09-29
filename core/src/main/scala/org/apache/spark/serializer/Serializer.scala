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

import scala.reflect.ClassTag

import java.io.{EOFException, InputStream, OutputStream}
import java.nio.ByteBuffer

import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream

import org.apache.spark.util.{NextIterator, ByteBufferInputStream}


/**
 * A serializer. Because some serialization libraries are not thread safe, this class is used to
 * create [[org.apache.spark.serializer.SerializerInstance]] objects that do the actual serialization and are
 * guaranteed to only be called from one thread at a time.
 *
 * Implementations of this trait should have a zero-arg constructor or a constructor that accepts a
 * [[org.apache.spark.SparkConf]] as parameter. If both constructors are defined, the latter takes precedence.
 */
trait Serializer {
  def newInstance(): SerializerInstance
}


/**
 * An instance of a serializer, for use by one thread at a time.
 */
trait SerializerInstance {
  def serialize[T: ClassTag](t: T): ByteBuffer

  def deserialize[T: ClassTag](bytes: ByteBuffer): T

  def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T

  def serializeStream(s: OutputStream): SerializationStream

  def deserializeStream(s: InputStream): DeserializationStream

  def serializeMany[T: ClassTag](iterator: Iterator[T]): ByteBuffer = {
    // Default implementation uses serializeStream
    val stream = new FastByteArrayOutputStream()
    serializeStream(stream).writeAll(iterator)
    val buffer = ByteBuffer.allocate(stream.position.toInt)
    buffer.put(stream.array, 0, stream.position.toInt)
    buffer.flip()
    buffer
  }

  def deserializeMany(buffer: ByteBuffer): Iterator[Any] = {
    // Default implementation uses deserializeStream
    buffer.rewind()
    deserializeStream(new ByteBufferInputStream(buffer)).asIterator
  }
}

/**
 * A stream for writing serialized objects.
 */
trait SerializationStream {
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
 * A stream for reading serialized objects.
 */
trait DeserializationStream {
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
