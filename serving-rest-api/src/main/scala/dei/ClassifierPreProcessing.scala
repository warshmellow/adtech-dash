package dei

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint

import scala.math.abs
import scala.util.hashing.MurmurHash3

@SerialVersionUID(100L)
object ClassifierPreProcessing extends Serializable {
  def prefix(v: Seq[String]): Seq[String] =
    v.indices.zip(v).
    map { case(i, s) => "f" + i.toString + "_" + s }

  def cross(v: Seq[String]): Seq[String] =
    for { x <- v; y <- v } yield x + ";" + y

  def hashingTrick(v: Seq[String], numBits: Int = 24, seed: Int = 42):
    Seq[Boolean] = {
    val h = (x:String) => MurmurHash3.stringHash(x, seed)
    val x = Array.fill(numBits)(false)
    v.foreach(value => {
      val i = abs(h(value) % numBits)
      x(i) = !x(i)
    })
    x
  }

  def quadFeaturesAndHashingTrick(v: Seq[String], numBits: Int = 24, seed: Int = 42):
    Seq[Boolean] =
    hashingTrick(cross(prefix(v)), numBits, seed)

  def toVector(v: Seq[Boolean]): Vector =
    Vectors.dense(v.map(x => if (x) 1.0 else 0.0).toArray)

  def quadFeaturesAndHashingTrickVec(
                                      v: Seq[String],
                                      numBits: Int = 24,
                                      seed: Int = 42): Vector =
    toVector(quadFeaturesAndHashingTrick(v, numBits, seed))

  // Sends parsed Array of String to Labeled Point,
  // assuming 0th entry is label 0...last label
  def parsedToLabeledPoint(v: Seq[String]) =
    LabeledPoint(
      v.head.toDouble,
      quadFeaturesAndHashingTrickVec(v.drop(1)))

  def parseLineToLabeledPoint(line: String) =
    parsedToLabeledPoint(line.split("\t"))

  def parseTimestampedLineToLabeledPoint(line: String) = {
    val v = line.split("\t")
    (v.head.toLong, parsedToLabeledPoint(v.drop(1)))
  }
}