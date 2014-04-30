
package org.apache.spark.serializer

import java.io.{InputStream, OutputStream, DataInputStream, DataOutputStream, ObjectInput}
import java.nio.ByteBuffer

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.collection.concurrent.TrieMap
import scala.xml.{XML, NodeSeq}

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.ByteBufferInputStream
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.storage._
import org.apache.spark.storage.{GetBlock, GotBlock, PutBlock}


import scala.reflect.{ClassTag, classTag}
import scala.pickling._
import binary._

/*
 this is how users can register picklers:
 val ser = SparkEnv.get.serializer.asInstanceOf[PicklingSerializer] // for now, ugly, but should be made nicer later
 ser.register[Person]
*/
class PicklingSerializer extends org.apache.spark.serializer.Serializer {
  import CustomPicklersUnpicklers._

  val registry: Map[Class[_], (SPickler[_], Unpickler[_])] = new TrieMap[Class[_], (SPickler[_], Unpickler[_])]

  def newInstance(): SerializerInstance = {
    new PicklingSerializerInstance(this)
  }

  def register[T: ClassTag](implicit pickler: SPickler[T], unpickler: Unpickler[T]): Unit = {
    val ct = classTag[T]
    val clazz = ct.runtimeClass
    registry += (clazz -> (pickler -> unpickler))
  }

  // register common types
  register[StorageLevel](classTag[StorageLevel], StorageLevelPicklerUnpickler, StorageLevelPicklerUnpickler)
  // register[PutBlock]
  // register[GotBlock]
  // register[GetBlock]
  // register[MapStatus]
  // register[BlockManagerId]
  // register[Array[Byte]]
  // register[Range[Int]]
  // register[Range[Long]]
}

class PicklingSerializerInstance(serializer: PicklingSerializer) extends SerializerInstance {
  val format = implicitly[BinaryPickleFormat]

  def serialize[T: ClassTag](t: T): ByteBuffer = {
    val ct = classTag[T]
    val clazz = ct.runtimeClass
    // look up pickler in registry
    val pickler = serializer.registry.get(clazz) match {
      case Some((p, _)) => p
      case None => implicitly[SPickler[Any]]
    }

    val builder = format.createBuilder()
    pickler.asInstanceOf[SPickler[T]].pickle(t, builder)
    val binPickle = builder.result()
    val arr = binPickle.value

    // first create an array
    // then turn into ByteBuffer
    ByteBuffer.wrap(arr)
  }

  def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis)
    in.readObject[T]()
  }

  def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis) // TODO: ignore loader for now
    in.readObject[T]()
  }

  def serializeStream(s: OutputStream): SerializationStream = {
    new PicklingSerializationStream(s, format, serializer)
  }

  def deserializeStream(s: InputStream): DeserializationStream = {
    new PicklingDeserializationStream(s, format, serializer)
  }
}

// TODO: should be added to pickling
class OutputStreamOutput(out: OutputStream) extends ArrayOutput[Byte] {

  def result(): Array[Byte] =
    null

  def +=(obj: Byte) =
    out.write(obj.asInstanceOf[Int])

  def put(obj: Array[Byte]): this.type = {
    out.write(obj)
    this
  }
}

class PicklingSerializationStream(os: OutputStream, format: BinaryPickleFormat, serializer: PicklingSerializer) extends SerializationStream {
  // create an OutputStreamOutput
  val osOutput = new OutputStreamOutput(os)

  def writeObject[T: ClassTag](t: T): SerializationStream = {
    val ct = classTag[T]
    val clazz = ct.runtimeClass
    val pickler = serializer.registry.get(clazz) match {
      case Some((p, _)) => p
      case None => implicitly[SPickler[Any]]
    }
    val builder = format.createBuilder(osOutput)
    pickler.asInstanceOf[SPickler[T]].pickle(t, builder)
    this
  }

  def flush() { os.flush() }
  def close() { os.close() }
}

class PicklingDeserializationStream(is: InputStream, format: BinaryPickleFormat, serializer: PicklingSerializer) extends DeserializationStream {
  def readObject[T: ClassTag](): T = {
    val ct = classTag[T]
    val clazz = ct.runtimeClass
    val unpickler = serializer.registry.get(clazz) match {
      case Some((_, up)) => up
      case None => implicitly[Unpickler[Any]]
    }

    val pickle = BinaryPickleStream(is)
    val reader = format.createReader(pickle, scala.pickling.internal.currentMirror)
    val result = unpickler.unpickle({ scala.pickling.FastTypeTag(clazz.getCanonicalName()) }, reader)
    //implicit val fastTag = FastTypeTag(clazz.getCanonicalName()).asInstanceOf[FastTypeTag[T]]
    //reader.unpickleTopLevel[T]

    //val reader = format.createReader(isInput)
    //pickler.asInstanceOf[SPickler[T]].pickle(t, builder)
    //throw new UnsupportedOperationException()
    result.asInstanceOf[T]
  }

  def close() { is.close() }
}

// custom serializers

object CustomPicklersUnpicklers {
  import SPickler._

  class DummyObjectInput(byte1: Byte, byte2: Byte) extends ObjectInput {
    var state = 0
    def readByte(): Byte = {
      if (state == 0) { state += 1; byte1 }
      else byte2
    }

    // Members declared in java.io.DataInput
   def readBoolean(): Boolean = ???
   def readChar(): Char = ???
   def readDouble(): Double = ???
   def readFloat(): Float = ???
   def readFully(x$1: Array[Byte],x$2: Int,x$3: Int): Unit = ???
   def readFully(x$1: Array[Byte]): Unit = ???
   def readInt(): Int = ???
   def readLine(): String = ???
   def readLong(): Long = ???
   def readShort(): Short = ???
   def readUTF(): String = ???
   def readUnsignedByte(): Int = ???
   def readUnsignedShort(): Int = ???
   def skipBytes(x$1: Int): Int = ???

   // Members declared in java.io.ObjectInput
   def available(): Int = ???
   def close(): Unit = ???
   def read(x$1: Array[Byte],x$2: Int,x$3: Int): Int = ???
   def read(x$1: Array[Byte]): Int = ???
   def read(): Int = ???
   def readObject(): Object = ???
   def skip(x$1: Long): Long = ???
  }

  implicit object StorageLevelPicklerUnpickler extends SPickler[StorageLevel] with Unpickler[StorageLevel] {

    val format = null // not used
    def pickle(picklee: StorageLevel, builder: PBuilder): Unit = {
      builder.beginEntry(picklee)

      builder.putField("_1", b => {
        b.hintTag(implicitly[FastTypeTag[Int]])
        b.hintStaticallyElidedType()
        intPicklerUnpickler.pickle(picklee.toInt, b)
      })

      builder.putField("_2", b => {
        b.hintTag(implicitly[FastTypeTag[Int]])
        b.hintStaticallyElidedType()
        intPicklerUnpickler.pickle(picklee.replication, b)
      })

      builder.endEntry()
    }
    def unpickle(tag: => FastTypeTag[_], reader: PReader): Any = {
      val reader1 = reader.readField("_1")
      reader1.hintTag(implicitly[FastTypeTag[Int]])
      reader1.hintStaticallyElidedType()

      val tag1 = reader1.beginEntry()
      val result1 = intPicklerUnpickler.unpickle(tag1, reader1).asInstanceOf[Int]
      reader1.endEntry()

      val reader2 = reader1.readField("_2")
      reader2.hintTag(implicitly[FastTypeTag[Int]])
      reader2.hintStaticallyElidedType()

      val tag2 = reader2.beginEntry()
      val result2 = intPicklerUnpickler.unpickle(tag2, reader2).asInstanceOf[Int]
      reader2.endEntry()

      val sl = scala.concurrent.util.Unsafe.instance.allocateInstance(classOf[StorageLevel]).asInstanceOf[StorageLevel]
      // initialize
      val input = new DummyObjectInput(result1.asInstanceOf[Byte], result2.asInstanceOf[Byte])
      sl.readExternal(input)
      sl
    }
  }
}

