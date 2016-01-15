package dei

import scala.util.hashing.MurmurHash3
import scala.math.abs

@SerialVersionUID(100L)
object ClassifierTrain extends Serializable {
  def prefix(v: Seq[String]): Seq[String] = {
    v.indices.zip(v).
    map { case(i, s) => "f" + i.toString + "_" + s }
  }

  def cross(v: Seq[String]): Seq[String] = {
    for {
      x <- v
      y <- v
    } yield x + ";" + y
  }

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
    Seq[Boolean] = {
    hashingTrick(cross(prefix(v)), numBits, seed)
  }
}