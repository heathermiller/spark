
package org.apache.spark.serializer

import java.io.{InputStream, OutputStream, DataInputStream, DataOutputStream, ObjectInput}
import java.nio.ByteBuffer

import scala.collection.mutable.{ArrayBuffer, Map, WrappedArray}
import scala.collection.concurrent.TrieMap
import scala.xml.{XML, NodeSeq}

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.ByteBufferInputStream
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.storage._
import org.apache.spark.storage.{GetBlock, GotBlock, PutBlock}

import org.apache.spark.scheduler.ShuffleMapTask
import org.apache.spark.scheduler.DirectTaskResult
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.ShuffleDependency
import org.apache.spark.scheduler.ResultTask
import org.apache.spark.rdd.FlatMappedRDD

import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap

import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.{universe => ru}
import scala.pickling._
import binary._

// for the movielens benchmark, this is what's serialized
// along with the number of times each thing is serialied
// ======================================================
// (class org.apache.spark.scheduler.ShuffleMapTask,4007)
// (class org.apache.spark.scheduler.DirectTaskResult,3951)
// (class org.apache.spark.scheduler.MapStatus,3683)
// (class org.apache.spark.ShuffleDependency,324)
// (class org.apache.spark.scheduler.ResultTask,285)
// (class org.apache.spark.rdd.FlatMappedRDD,260)


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

  // def register[T: ClassTag](implicit pickler: SPickler[T], unpickler: Unpickler[T]): Unit = {
  //   val ct = classTag[T]
  //   val clazz = ct.runtimeClass
  //   registry += (clazz -> (pickler -> unpickler))
  // }

  def register[T: ClassTag: SPickler: Unpickler](): Unit = {
   val clazz = classTag[T].runtimeClass
   val p = implicitly[SPickler[T]]
   val up = implicitly[Unpickler[T]]
   println(s"registering for ${clazz.getName()} pickler of class type ${p.getClass.getName}...")
   GlobalRegistry.picklerMap += (clazz.getName() -> p)
   GlobalRegistry.unpicklerMap += (clazz.getName() -> up)
  }

  val defOpt1: Option[AnyRef] = None
  val defOpt2: Option[AnyRef] = Some(new Object)
  GlobalRegistry.picklerMap   += (defOpt1.getClass.getName() -> anyRefOpt)
  GlobalRegistry.unpicklerMap += (defOpt1.getClass.getName() -> anyRefOpt)
  GlobalRegistry.picklerMap   += (defOpt2.getClass.getName() -> anyRefOpt)
  GlobalRegistry.unpicklerMap += (defOpt2.getClass.getName() -> anyRefOpt)

  val map = new Object2LongOpenHashMap[AnyRef]()
  GlobalRegistry.picklerMap += (map.getClass.getName() -> Object2LongOpenHashMapPickler)
  GlobalRegistry.unpicklerMap += (map.getClass.getName() -> Object2LongOpenHashMapPickler)

  GlobalRegistry.picklerMap += ("scala.collection.mutable.WrappedArray$ofRef" -> mkAnyRefWrappedArrayPickler)
  GlobalRegistry.unpicklerMap += ("scala.collection.mutable.WrappedArray.ofRef[java.lang.Object]" -> mkAnyRefWrappedArrayPickler)

  GlobalRegistry.picklerMap += ("scala.collection.immutable.$colon$colon" -> implicitly[SPickler[::[AnyRef]]])
  GlobalRegistry.unpicklerMap += ("scala.collection.immutable.$colon$colon[java.lang.Object]" -> implicitly[Unpickler[::[AnyRef]]])

  // register common types
  //register[StorageLevel]//(classTag[StorageLevel], StorageLevelPicklerUnpickler, StorageLevelPicklerUnpickler)
  register[PutBlock]
  register[GotBlock]
  register[GetBlock]
  register[BlockManagerId]//(classTag[BlockManagerId], BlockManagerIdPicklerUnpickler, BlockManagerIdPicklerUnpickler)
  register[MapStatus]
  register[Array[Byte]]
  register[Range]
  register[ShuffleMapTask]
  register[DirectTaskResult[_]]
  register[MapStatus]
  // register[ShuffleDependency[_, _]]
  register[ResultTask[_, _]]
  // register[FlatMappedRDD[_, _]]
  register[Object2LongOpenHashMap[AnyRef]]
}

class PicklingSerializerInstance(serializer: PicklingSerializer) extends SerializerInstance {
  val format = implicitly[BinaryPickleFormat]

  var checks = 0

  def serialize[T: ClassTag](t: T): ByteBuffer = {
    /*
    val ct = classTag[T]
    val clazz = ct.runtimeClass
    // look up pickler in registry
    val pickler = serializer.registry.get(clazz) match {
      case Some((p, _)) => p
      case None => implicitly[SPickler[Any]]
    }

    val builder = format.createBuilder()

    // need hintTag
    val mirror: ru.Mirror = ru.runtimeMirror(clazz.getClassLoader) // expensive: try to do only once!
    val tag = FastTypeTag.mkRaw(clazz, mirror)
    builder.hintTag(tag)

    pickler.asInstanceOf[SPickler[T]].pickle(t, builder)
    val binPickle = builder.result()
    */
    println(s"SPARK: pickling class '${t.getClass.getName}' as Any [2]")
    val binPickle = (t: Any).pickle

    // println(s"pickled: ${binPickle.value.mkString("[", ",", "]")}")

    // check whether pickling/unpickling is correct

    val ut = binPickle.unpickle[Any]

    assert(t.getClass.getName == ut.getClass.getName, "unpickled object has incorrect runtime class")
    checks += 1
    if (checks % 50 == 0)
      print("#")

    if (t.getClass.isArray) {
      val arr1 = t.asInstanceOf[Array[AnyRef]]
      val arr2 = ut.asInstanceOf[Array[AnyRef]]
      val zipped = arr1.zip(arr2)
      val wrongElem = zipped.find { case (el1, el2) => el1 != el2 }
      assert(wrongElem.isEmpty, s"!!! found unequal elem: $wrongElem")
    } else {
      (t, ut) match {
        case (Some(stuff1), Some(stuff2)) =>
          println(s"!!! contained in Option: ${stuff1.getClass.getName}")

          val m1 = stuff1.asInstanceOf[Object2LongOpenHashMap[AnyRef]]
          val m2 = stuff2.asInstanceOf[Object2LongOpenHashMap[AnyRef]]

          val values1 = m1.values().toLongArray()
          val values2 = m2.values().toLongArray()

          val vs1 = values1.mkString("[", ",", "]")
          val vs2 = values2.mkString("[", ",", "]")
          assert(values1.length == values2.length, s"values sizes unqual: ${values1.length}, ${values2.length}")

        case _ =>
          // do nothing
      }
    }

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

  var checks = 0

  def writeObject[T: ClassTag](t: T): SerializationStream = {
    def classOfElem(x: AnyRef): String =
      if (x == null) "null" else x.getClass.getName

    val className = t.getClass.getName
    // println(s"SPARK: pickling class '$className' as Any to OutputStream")
    if (className.contains("WrappedArray")) {
      val wa = t.asInstanceOf[WrappedArray.ofRef[AnyRef]]
      println(s"PICKLING: WrappedArray.ofRef[AnyRef] of size: ${wa.length}")
      val p = (t: Any).pickle
      val up = p.unpickle[Any]
      val wa2 = up.asInstanceOf[WrappedArray.ofRef[AnyRef]]
      println(s"PICKLING: Unpickled WrappedArray.ofRef[AnyRef] of size: ${wa2.length}")
    }

    // if (className.contains("Tuple2")) {
    //   val p = (t: Any).pickle
    //   val up = p.unpickle[Any]
    //   val t1 = t.asInstanceOf[(AnyRef, AnyRef)]
    //   val t2 = up.asInstanceOf[(AnyRef, AnyRef)]
    //   val s1 = classOfElem(t1._1) + "," + classOfElem(t1._2)
    //   val s2 = classOfElem(t2._1) + "," + classOfElem(t2._2)
    //   println(s1)
    //   assert(s1 == s2)
    // }
/*
    val ct = classTag[T]
    val clazz = ct.runtimeClass
    val pickler = serializer.registry.get(clazz) match {
      case Some((p, _)) => p
      case None => implicitly[SPickler[Any]]
    }
    val builder = format.createBuilder(osOutput)
    pickler.asInstanceOf[SPickler[T]].pickle(t, builder)
*/
    (t: Any).pickleTo(osOutput)

    val binPickle = (t: Any).pickle
    val ut = binPickle.unpickle[Any]

    assert(t.getClass.getName == ut.getClass.getName, "unpickled object has incorrect runtime class")
    checks += 1
    if (checks % 500 == 0) {
      // if (t.getClass.getName.contains("Tuple2")) {
      //   println("BEFORE:")
      //   println(t.toString)
      //   println("AFTER:")
      //   println(ut.toString)
      // } else
        print("o")
    }

    this
  }

  def flush() { os.flush() }
  def close() { os.close() }
}

class PicklingDeserializationStream(is: InputStream, format: BinaryPickleFormat, serializer: PicklingSerializer) extends DeserializationStream {
  var cnt = 0
  def readObject[T: ClassTag](): T = {
    /*
    val ct = classTag[T]
    val clazz = ct.runtimeClass
    val unpickler = serializer.registry.get(clazz) match {
      case Some((_, up)) => up
      case None => implicitly[Unpickler[Any]]
    }

    val pickle = BinaryPickleStream(is)
    val reader = format.createReader(pickle, scala.pickling.internal.currentMirror)
    val typeString = reader.beginEntryNoTag()
    val result = unpickler.unpickle({ scala.pickling.FastTypeTag(clazz.getCanonicalName()) }, reader)
    //implicit val fastTag = FastTypeTag(clazz.getCanonicalName()).asInstanceOf[FastTypeTag[T]]
    //reader.unpickleTopLevel[T]

    //val reader = format.createReader(isInput)
    //pickler.asInstanceOf[SPickler[T]].pickle(t, builder)
    //throw new UnsupportedOperationException()
    */

    val pickle = BinaryPickleStream(is)
    val result = pickle.unpickle[Any]
    cnt += 1
    if (cnt % 500 == 0) {
      println(s"readObject of class '${result.getClass.getName}'")
    }
    result.asInstanceOf[T]
  }

  def close() { is.close() }
}

// custom serializers

object CustomPicklersUnpicklers {
  import SPickler._

  def mkAnyRefWrappedArrayPickler(implicit pf: PickleFormat):
    SPickler[WrappedArray.ofRef[AnyRef]] with Unpickler[WrappedArray.ofRef[AnyRef]] =
      new SPickler[WrappedArray.ofRef[AnyRef]] with Unpickler[WrappedArray.ofRef[AnyRef]] {

    val format: PickleFormat = pf

    val mirror = scala.reflect.runtime.currentMirror

    def pickle(coll: WrappedArray.ofRef[AnyRef], builder: PBuilder): Unit = {
      builder.hintTag(implicitly[FastTypeTag[WrappedArray.ofRef[AnyRef]]])
      builder.beginEntry(coll)

      builder.beginCollection(coll.size)
      coll.foreach { (elem: AnyRef) =>
        builder putElement { b =>
          val elemClass = elem.getClass
          // TODO: allow passing in ClassLoader to picklers selected from registry
          val classLoader: ClassLoader = elemClass.getClassLoader
          val elemTag = FastTypeTag.mkRaw(elemClass, mirror) // slow: `mkRaw` is called for each element
          b.hintTag(elemTag)
          val pickler = SPickler.genPickler(classLoader, elemClass, elemTag).asInstanceOf[SPickler[AnyRef]]
          pickler.pickle(elem, b)
        }
      }
      builder.endCollection()

      builder.endEntry()
    }

    def unpickle(tpe: => FastTypeTag[_], preader: PReader): Any = {
      val reader = preader.beginCollection()

      val length = reader.readLength()
      val elemClass = (new Object).getClass
      val newArray = java.lang.reflect.Array.newInstance(elemClass, length).asInstanceOf[Array[AnyRef]]

      var i = 0
      while (i < length) {
        val r = reader.readElement()
        val elemTag = r.beginEntry()
        val elemUnpickler = Unpickler.genUnpickler(mirror, elemTag)
        val elem = elemUnpickler.unpickle(elemTag, r)
        r.endEntry()
        newArray(i) = elem.asInstanceOf[AnyRef]
        i = i + 1
      }

      preader.endCollection()
      new WrappedArray.ofRef(newArray)
    }
  }

  implicit val anyRefOpt = new SPickler[Option[AnyRef]] with Unpickler[Option[AnyRef]] {
    val format = implicitly[PickleFormat]
    val mirror = scala.reflect.runtime.currentMirror

    def pickle(picklee: Option[AnyRef], builder: PBuilder): Unit = {
      println(s"anyRefOpt pickler running...")
      builder.beginEntry(picklee)
      builder.putField("empty", b => {
        b.hintTag(implicitly[FastTypeTag[Int]])
        b.hintStaticallyElidedType()
        implicitly[SPickler[Int]].pickle(if (picklee.isEmpty) 0 else 1, b)
      })
      if (picklee.nonEmpty) {
        val elem        = picklee.get
        val elemClass   = elem.getClass
        println(s"element class: ${elemClass.getName}")
        val elemTag     = FastTypeTag.mkRaw(elemClass, mirror)
        val classLoader = elemClass.getClassLoader
        builder.putField("value", b => {
          b.hintTag(elemTag)
          val elemPickler = SPickler.genPickler(classLoader, elemClass, elemTag).asInstanceOf[SPickler[AnyRef]]
          elemPickler.pickle(elem, b)
        })
      }
      builder.endEntry()
    }

    def unpickle(tag: => FastTypeTag[_], reader: PReader): Any = {
      val reader1 = reader.readField("empty")
      reader1.hintTag(implicitly[FastTypeTag[Int]])
      reader1.hintStaticallyElidedType()
      val tag = reader1.beginEntry()
      val result = implicitly[Unpickler[Int]].unpickle(tag, reader1)
      reader1.endEntry()

      if (result.asInstanceOf[Int] == 1) {
        val reader2 = reader.readField("value")
        val tag2 = reader2.beginEntry()
        val elemUnpickler =
          Unpickler.genUnpickler(mirror, tag2)
        val result2 = elemUnpickler.unpickle(tag2, reader2)
        reader2.endEntry()
        Some(result2.asInstanceOf[AnyRef])
      } else {
        None
      }
    }
  }

  implicit object Object2LongOpenHashMapPickler extends SPickler[Object2LongOpenHashMap[AnyRef]] with Unpickler[Object2LongOpenHashMap[AnyRef]] {
    // println("using custom pickler...")

    val format: PickleFormat = implicitly[PickleFormat]

    val mirror      = scala.reflect.runtime.currentMirror

    val collClass   = classOf[Object2LongOpenHashMap[AnyRef]]
    val collTag     = FastTypeTag.mkRaw(collClass, mirror)
    val classLoader = collClass.getClassLoader

    val defaultElem: (AnyRef, Long) = (new Object, 0L)

    val elemTag     = //implicitly[FastTypeTag[(AnyRef, Long)]]
      FastTypeTag.mkRaw(defaultElem.getClass, mirror)
    val elemPickler = //implicitly[SPickler[(AnyRef, Long)]]
      SPickler.genPickler(classLoader, defaultElem.getClass, elemTag).asInstanceOf[SPickler[AnyRef]]
    val elemUnpickler =
      Unpickler.genUnpickler(mirror, elemTag)

    def pickle(coll: Object2LongOpenHashMap[AnyRef], builder: PBuilder): Unit = {
      println("custom Object2LongOpenHashMap pickler running...")
      builder.hintTag(collTag)
      builder.beginEntry(coll)
      builder.beginCollection(coll.size)

      val entries = coll.object2LongEntrySet()
      val iter = entries.fastIterator()

      // println("writing elements...")

      while (iter.hasNext) {
        val elem = iter.next
        val k: AnyRef = elem.getKey()
        val v: Long   = elem.getLongValue()

        builder putElement { b =>
          b.hintTag(elemTag)
          val elem = k -> v
          // print(s"$elem,")
          elemPickler.pickle(k -> v, b)
        }
      }

      // println()
      builder.endCollection()
      builder.endEntry()
    }

    def unpickle(tag: => FastTypeTag[_], preader: PReader): Any = {
      println("custom Object2LongOpenHashMap unpickler running...")
      val reader = preader.beginCollection()

      val length = reader.readLength()
      val newMap = new Object2LongOpenHashMap[AnyRef]()

      // println("reading elements...")

      var i = 0
      while (i < length) {
        val r = reader.readElement()
        r.beginEntryNoTag()
        val elem = elemUnpickler.unpickle(elemTag, r).asInstanceOf[(AnyRef, Long)]
        // print(s"$elem,")
        r.endEntry()
        newMap.put(elem._1, elem._2)
        i = i + 1
      }

      // println()
      preader.endCollection()
      newMap
    }
  }

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

