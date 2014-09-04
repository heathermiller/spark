
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

// import org.apache.spark.mllib.recommendation.Rating

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
   // println(s"registering for ${clazz.getName()} pickler of class type ${p.getClass.getName}...")
   GlobalRegistry.picklerMap += (clazz.getName() -> (x => p))
   GlobalRegistry.unpicklerMap += (clazz.getName() -> up)
  }

  @volatile var intTuplePickler: Map[String, (SPickler[_], FastTypeTag[_])] = Map()
  @volatile var unpicklerMap: Map[String, (Unpickler[Any], FastTypeTag[_])] = Map()

  @volatile var picklerMap: Map[String, (SPickler[_], FastTypeTag[_])] = Map()

  def registerIntTuplePickler(name: String, pickler: SPickler[_], unpickler: Unpickler[_], tag: FastTypeTag[_]): Unit = {
    intTuplePickler += (name -> (pickler -> tag))

    picklerMap += (s"(java.lang.Integer,$name)" -> (pickler, tag))

    unpicklerMap += (s"scala.Tuple2[scala.Int,$name]" -> (unpickler.asInstanceOf[Unpickler[Any]] -> tag))
  }

  val defOpt1: Option[AnyRef] = None
  val defOpt2: Option[AnyRef] = Some(new Object)
  GlobalRegistry.picklerMap   += (defOpt1.getClass.getName() -> (x => anyRefOpt))
  GlobalRegistry.unpicklerMap += (defOpt1.getClass.getName() -> anyRefOpt)
  GlobalRegistry.picklerMap   += (defOpt2.getClass.getName() -> (x => anyRefOpt))
  GlobalRegistry.unpicklerMap += (defOpt2.getClass.getName() -> anyRefOpt)

  val map = new Object2LongOpenHashMap[AnyRef]()
  GlobalRegistry.picklerMap += (map.getClass.getName() -> (x => Object2LongOpenHashMapPickler))
  GlobalRegistry.unpicklerMap += (map.getClass.getName() -> Object2LongOpenHashMapPickler)

  GlobalRegistry.picklerMap += ("scala.collection.mutable.WrappedArray$ofRef" -> (x => mkAnyRefWrappedArrayPickler))
  GlobalRegistry.unpicklerMap += ("scala.collection.mutable.WrappedArray.ofRef[java.lang.Object]" -> mkAnyRefWrappedArrayPickler)
  GlobalRegistry.picklerMap += ("scala.collection.mutable.WrappedArray$ofInt" -> (x => mkIntWrappedArrayPickler))
  GlobalRegistry.unpicklerMap += ("scala.collection.mutable.WrappedArray$ofInt" -> mkIntWrappedArrayPickler)

  GlobalRegistry.picklerMap += ("scala.collection.immutable.$colon$colon" -> (x => implicitly[SPickler[::[AnyRef]]]))
  GlobalRegistry.unpicklerMap += ("scala.collection.immutable.$colon$colon[java.lang.Object]" -> implicitly[Unpickler[::[AnyRef]]])

  GlobalRegistry.unpicklerMap += ("scala.Tuple2[scala.Int,scala.Int]" -> Tuple2IntIntPickler)

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


  picklerMap += ("(java.lang.Integer,java.lang.Integer)" -> (Tuple2IntIntPickler, implicitly[FastTypeTag[(Int, Int)]]))

  {
    val pickler = //implicitly[SPickler[((Int, Int), Double)]]
      Tuple2IntIntDoublePickler
    val tag = implicitly[FastTypeTag[((Int, Int), Double)]]
    picklerMap += ("((java.lang.Integer,java.lang.Integer),java.lang.Double)" -> (pickler, tag))
  }

  // {
  //   val pickler = implicitly[SPickler[(Int, Array[Double])]]
  //   val tag = implicitly[FastTypeTag[(Int, Array[Double])]]
  //   picklerMap += ("(java.lang.Integer,[D)" -> (pickler, tag))
  // }

  // {
  //   val pickler = implicitly[SPickler[(Int, (Int, Array[Double]))]]
  //   val tag = implicitly[FastTypeTag[(Int, (Int, Array[Double]))]]
  //   picklerMap += ("(java.lang.Integer,(java.lang.Integer,[D))" -> (pickler, tag))
  // }

}

class PicklingSerializerInstance(serializer: PicklingSerializer) extends SerializerInstance {
  val format = implicitly[BinaryPickleFormat]

  def serialize[T: ClassTag](t: T): ByteBuffer = {
    // println(s"SPARK: pickling class '${t.getClass.getName}' as Any")

    val binPickle = (t: Any).pickle
    val arr = binPickle.value
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

class PicklingSerializationStream(os: OutputStream, format: BinaryPickleFormat, serializer: PicklingSerializer) extends SerializationStream {
  import CustomPicklersUnpicklers._

  val osOutput = new OutputStreamOutput(os)
  val builder = format.createBuilder(osOutput)

  var cnt = 0

  def classOf(x: Any): String =
    if (x == null) "null" else x.getClass.getName

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

  /* OPT: initialize once within PicklingSerializer. Then, obtain each map using a volatile field of PicklingSerializer.
   */
  // var tuplePickler: Map[String, Map[String, (SPickler[_], FastTypeTag[_])]] = Map()

  // var intTuplePickler: Map[String, (SPickler[_], FastTypeTag[_])] = serializer.intTuplePickler
  // intTuplePickler += ("java.lang.Integer" -> (Tuple2IntIntPickler, implicitly[FastTypeTag[(Int, Int)]]))
  // // intTuplePickler += ("org.apache.spark.mllib.recommendation.Rating" -> implicitly[SPickler[(Int, Rating)]])

  // var specIntTuplePickler: Map[String, SPickler[_]] = Map()
  // specIntTuplePickler += ("java.lang.Double" -> new SpecialTuple2Pickler(implicitly[FastTypeTag[(Int, Int)]], Tuple2IntIntPickler.asInstanceOf[SPickler[Any]], FastTypeTag.Double, SPickler.doublePicklerUnpickler.asInstanceOf[SPickler[Any]]))

  // tuplePickler += ("java.lang.Integer" -> intTuplePickler)
  // tuplePickler += ("scala.Tuple2$mcII$sp" -> specIntTuplePickler)

  val picklerMap: Map[String, (SPickler[_], FastTypeTag[_])] = serializer.picklerMap

  var started = false
  var pickler: SPickler[_] = null
  var tag: FastTypeTag[_] = null

  override def startIteration(): Unit = {
    started = true
    pickler = null
    tag = null
  }

  override def endIteration(): Unit = {
    started = false
    pickler = null
    tag = null
  }

  def writeObject[T: ClassTag](t: T): SerializationStream = {
    cnt += 1
    // if (cnt % 1000 == 0) {
    //   if (classOf(t).contains("Tuple2")) {
    //     val tup = t.asInstanceOf[Tuple2[Any, Any]]
    //     println(s"writeObject with class: (${classOf(tup._1)}, ${classOf(tup._2)})")
    //   }
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

    // (t: Any).pickleTo(osOutput)

    // if (t != null && t.isInstanceOf[Tuple2[_, _]]) {
    //   val tup = t.asInstanceOf[Tuple2[Any, Any]]
    //   val fstClassName = if (tup._1 != null) tup._1.getClass.getName else "null"
    //   tuplePickler.get(fstClassName) match {
    //     case None => /* do nothing */
    //     case Some(tupMap) =>
    //       // println("WOOOOO")
    //       val sndClassName = if (tup._2 != null) tup._2.getClass.getName else "null"
    //       tupMap.get(sndClassName) match {
    //         case None => /* do nothing */
    //         case Some(both) =>
    //           // println("HAAAAAAWOOOOOO")
    //           // if (cnt % 1000 == 0)
    //           //   println(s"WOOHOO found pickler for ($fstClassName, $sndClassName)")
    //           pickler = both._1
    //           tag = both._2
    //       }
    //   }
    // }

    if (pickler == null) {
      val typeString = approxTypeOf(t)
      picklerMap.get(typeString) match {
        case None => /* do nothing */
        case Some((pickler1, tag1)) =>
          pickler = pickler1
          tag = tag1
      }
    }

    if (pickler == null) {
      // println(s"NO HIT: $typeString")
      (t: Any).pickleInto(builder)
    } else {
      // scala.pickling.internal.GRL.lock()
      builder.hintTag(tag)
      pickler.asInstanceOf[SPickler[Any]].pickle(t, builder)
      // scala.pickling.internal.GRL.unlock()
    }

    // TODO: avoid if part of same iteration
    if (!started) {
      pickler = null
      tag = null
    }

    // if (t.isInstanceOf[Tuple2[_, _]]) {
    //   val tup = t.asInstanceOf[Tuple2[Any, Any]]
    //   if (tup._2 != null && tup._2.getClass.getName == "[[D") {
    //     println("@@@ found tuple with [[D")
    //     val binPickle = (t: Any).pickle
    //     val ut = binPickle.unpickle[Any]
    //     println("@@@ verified successful unpickling")
    //   }
    // }

/*
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
*/
    this
  }

  def flush() { os.flush() }
  def close() { os.close() }
}

class PicklingDeserializationStream(is: InputStream, format: BinaryPickleFormat, serializer: PicklingSerializer) extends DeserializationStream {
  import CustomPicklersUnpicklers._
  import shareNothing._

  val pickle = BinaryPickleStream(is)
  val reader = format.createReader(pickle, scala.pickling.internal.currentMirror)
  val anyTag = implicitly[scala.pickling.FastTypeTag[Any]]

  var cnt = 0

  var lastTen: List[Any] = List()

  val debugOn = false

  def classOf(x: Any): String =
    if (x == null) "null" else x.getClass.getName

  var unpicklerMap: Map[String, (Unpickler[Any], FastTypeTag[_])] = serializer.unpicklerMap
  unpicklerMap += ("scala.Tuple2" -> ((new Tuple2RTPickler(null)).asInstanceOf[Unpickler[Any]] -> FastTypeTag("scala.Tuple2")))
  unpicklerMap += ("scala.Tuple2[scala.Int,scala.Int]" -> (Tuple2IntIntPickler.asInstanceOf[Unpickler[Any]] -> implicitly[FastTypeTag[(Int, Int)]]))
  // unpicklerMap += ("scala.Tuple2[scala.Int,scala.Tuple2[scala.Int,scala.Array[scala.Double]]]" -> ((new Tuple2RTPickler(null)).asInstanceOf[Unpickler[Any]] -> implicitly[FastTypeTag[(Int, (Int, Array[Double]))]]))

  // FIXME: for some reason, we can't generate a MapStatus Unpickler if shareNothing is imported
  GlobalRegistry.unpicklerMap.get("org.apache.spark.scheduler.MapStatus") match {
    case None => /* do nothing */
    case Some(up) =>
      unpicklerMap += ("org.apache.spark.scheduler.MapStatus" -> (up.asInstanceOf[Unpickler[Any]] -> implicitly[FastTypeTag[MapStatus]]))
  }

  {
    val unpickler = //implicitly[Unpickler[((Int, Int), Double)]]
      Tuple2IntIntDoublePickler
    val tag = implicitly[FastTypeTag[((Int, Int), Double)]]
    unpicklerMap += ("scala.Tuple2[scala.Tuple2[scala.Int,scala.Int],scala.Double]" -> (unpickler.asInstanceOf[Unpickler[Any]], tag))
  }

  var started = false // if true, will not reset unpickler/tag to null
  var unpickler: Unpickler[Any] = null
  var tag: FastTypeTag[_] = null

  override def startIteration(): Unit = {
    started = true // not reset unpickler/tag until endIteration() has been signalled
    unpickler = null
    tag = null
  }

  override def endIteration(): Unit = {
    started = false
    unpickler = null
    tag = null
  }

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

/*
    val result = pickle.unpickle[Any]
*/
    cnt += 1
    val debugOn = cnt % 1000 == 0

    reader.hintTag(anyTag)

    val typeStringOpt = try {
      Some(reader.beginEntryNoTagDebug(debugOn))
    } catch {
      case PicklingException(msg) =>
        println(s"error in PicklingDeserializationStream:\n$msg")
        None
      case _: java.lang.RuntimeException =>
        //throw new java.io.EOFException
        None
    } /*finally {
      scala.pickling.internal.GRL.unlock()
    }*/
    if (typeStringOpt.isEmpty) {
      throw new java.io.EOFException
    } else {

      if (unpickler == null) {
        val typeString = typeStringOpt.get

        // select tag and unpickler based on typeString
        unpicklerMap.get(typeString) match {
          case None =>
            // if (typeString.startsWith("scala.Tuple2"))
            //   println(s"NO HIT: last typeString: $typeString")

            scala.pickling.internal.GRL.lock()
            tag = FastTypeTag(typeString)
            unpickler = Unpickler.genUnpickler(reader.mirror, tag)(ShareNothing).asInstanceOf[Unpickler[Any]]
            scala.pickling.internal.GRL.unlock()
          case Some(pair) =>
            // if (debugOn)
            //   println(s"HIT: last typeString: $typeString")

            unpickler = pair._1
            tag = pair._2
        }
      }

      // if (debugOn)
      //   println(s"tag: ${tag.key}")
      
      // if (debugOn)
      //   println(s"unpickler: ${unpickler.getClass.getName}")

      val result = try {
        unpickler.unpickle({ FastTypeTag(typeStringOpt.get) }, reader)
      } catch {
        case e @ PicklingException(msg) =>
          println(s"error in PicklingDeserializationStream:\n$msg\nlast ten objects read: $lastTen")
          throw e
        case e: EndOfStreamException =>
          println(s"error in PicklingDeserializationStream:\nlast ten objects read: $lastTen")
          throw e
      }

      // if (debugOn) {
      //   val className = result.getClass.getName
      //   val classString = if (className.contains("Tuple2")) {
      //     val tup = result.asInstanceOf[Tuple2[Any, Any]]
      //     s"(${classOf(tup._1)}, ${classOf(tup._2)})"
      //   } else {
      //     className
      //   }
      //   println(s"readObject [#$cnt] of class '$classString'")
      // }

      // scala.pickling.internal.GRL.unlock()

      // lastTen = result :: (if (lastTen.isEmpty) List() else lastTen.init)

      if (!started) {
        unpickler = null
        tag = null
      }

      result.asInstanceOf[T]
    }
  }

  def close() { is.close() }
}

// custom serializers

object CustomPicklersUnpicklers {
  import SPickler._
  import shareNothing._

  class SpecialTuple2Pickler(tag1: FastTypeTag[_], pickler1: SPickler[Any], tag2: FastTypeTag[_], pickler2: SPickler[Any])
      extends SPickler[(Any, Any)] {
    val format = implicitly[PickleFormat]

    def pickle(picklee: (Any, Any), builder: PBuilder): Unit = {
      // println(s"@@@ using dynamic specialized ${this.getClass.getName}")
      builder.beginEntry(picklee)
      builder.putField("_1", b => {
        b.hintTag(tag1)
        pickler1.pickle(picklee._1, b)
      })
      builder.putField("_2", b => {
        b.hintTag(tag2)
        pickler2.pickle(picklee._2, b)
      })
      builder.endEntry()
    }
  }

  object Tuple2IntIntPickler extends SPickler[(Int, Int)] with Unpickler[(Int, Int)] {
    import SPickler._

    val format = implicitly[PickleFormat]

    def pickle(picklee: (Int, Int), builder: PBuilder): Unit = {
      // println(s"@@@ using static specialized ${this.getClass.getName}")
      builder.beginEntry(picklee)

      builder.pushHints()
      builder.hintTag(implicitly[FastTypeTag[Int]])
      builder.hintStaticallyElidedType()
      builder.pinHints()

      builder.putField("_1", b => {
        intPicklerUnpickler.pickle(picklee._1, b)
      })

      builder.putField("_2", b => {
        intPicklerUnpickler.pickle(picklee._2, b)
      })

      builder.popHints()
      builder.endEntry()
    }

    def unpickle(tpe: => FastTypeTag[_], reader: PReader): Any = {
      // println(s"@@@ using static specialized ${this.getClass.getName}")
      reader.hintTag(implicitly[FastTypeTag[Int]])
      reader.hintStaticallyElidedType()
      reader.pinHints()

      val reader1 = reader.readField("_1")
      val ts1 = reader1.beginEntryNoTag()
      val value1 = reader1.readPrimitive()

      val reader2 = reader.readField("_2")
      val ts2 = reader2.beginEntryNoTag()
      val value2 = reader2.readPrimitive()

      (value1, value2)
    }
  }

  object Tuple2IntIntDoublePickler extends SPickler[((Int, Int), Double)] with Unpickler[((Int, Int), Double)] {
    import SPickler._

    val format = implicitly[PickleFormat]

    def pickle(picklee: ((Int, Int), Double), builder: PBuilder): Unit = {
      // println(s"@@@ using static specialized ${this.getClass.getName}")
      builder.beginEntry(picklee)

      builder.hintTag(implicitly[FastTypeTag[(Int, Int)]])
      builder.hintStaticallyElidedType()

      builder.putField("_1", b => {
        Tuple2IntIntPickler.pickle(picklee._1, b)
      })

      builder.hintTag(implicitly[FastTypeTag[Double]])
      builder.hintStaticallyElidedType()

      builder.putField("_2", b => {
        doublePicklerUnpickler.pickle(picklee._2, b)
      })

      builder.endEntry()
    }

    def unpickle(tpe: => FastTypeTag[_], reader: PReader): Any = {
      // println(s"@@@ using static specialized ${this.getClass.getName}")
      val tag1 = implicitly[FastTypeTag[(Int, Int)]]
      reader.hintTag(tag1)
      reader.hintStaticallyElidedType()

      val reader1 = reader.readField("_1")
      val ts1 = reader1.beginEntryNoTag()
      val value1 = Tuple2IntIntPickler.unpickle(tag1, reader1)
      reader1.endEntry()

      reader.hintTag(FastTypeTag.Double)
      reader.hintStaticallyElidedType()
      val reader2 = reader.readField("_2")
      val ts2 = reader2.beginEntryNoTag()
      val value2 = reader2.readPrimitive()
      reader2.endEntry()

      (value1, value2)
    }
  }

  def mkIntWrappedArrayPickler:
    SPickler[WrappedArray[Int]] with Unpickler[WrappedArray[Int]] =
      new SPickler[WrappedArray[Int]] with Unpickler[WrappedArray[Int]] {

    val format: PickleFormat = null // unused

    val mirror = scala.reflect.runtime.currentMirror

    def pickle(coll: WrappedArray[Int], builder: PBuilder): Unit = {
      builder.beginEntry(coll)
      builder.beginCollection(coll.size)

      builder.pushHints()
      builder.hintStaticallyElidedType()
      builder.hintTag(FastTypeTag.Int)
      builder.pinHints()

      coll.foreach { (elem: Int) =>
        builder putElement { b =>
          val pickler = SPickler.intPicklerUnpickler
          pickler.pickle(elem, b)
        }
      }

      builder.popHints()
      builder.endCollection()
      builder.endEntry()
    }

    def unpickle(tpe: => FastTypeTag[_], preader: PReader): Any = {
      val reader = preader.beginCollection()

      preader.pushHints()
      reader.hintStaticallyElidedType()
      reader.hintTag(FastTypeTag.Int)
      reader.pinHints()

      val length = reader.readLength()
      val elemClass = (0).getClass
      val newArray = java.lang.reflect.Array.newInstance(elemClass, length).asInstanceOf[Array[Int]]

      var i = 0
      while (i < length) {
        val r = reader.readElement()
        r.beginEntryNoTag()
        val elemUnpickler = Unpickler.intPicklerUnpickler
        val elem = elemUnpickler.unpickle(FastTypeTag.Int, r)
        r.endEntry()
        newArray(i) = elem.asInstanceOf[Int]
        i = i + 1
      }

      preader.popHints()
      preader.endCollection()
      new WrappedArray.ofInt(newArray)
    }
  }

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
        val elemUnpickler = Unpickler.genUnpickler(mirror, elemTag)(ShareNothing)
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
      // println(s"anyRefOpt pickler running...")
      builder.beginEntry(picklee)
      builder.putField("empty", b => {
        b.hintTag(implicitly[FastTypeTag[Int]])
        b.hintStaticallyElidedType()
        implicitly[SPickler[Int]].pickle(if (picklee.isEmpty) 0 else 1, b)
      })
      if (picklee.nonEmpty) {
        val elem        = picklee.get
        val elemClass   = elem.getClass
        // println(s"element class: ${elemClass.getName}")
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
      // println("custom Object2LongOpenHashMap pickler running...")
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
      // println("custom Object2LongOpenHashMap unpickler running...")
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

  /*implicit*/ object StorageLevelPicklerUnpickler extends SPickler[StorageLevel] with Unpickler[StorageLevel] {

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

