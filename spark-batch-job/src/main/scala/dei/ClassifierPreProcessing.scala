package dei

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import com.github.nscala_time.time.Imports._

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

  def predictionAndLabels(
                           test: RDD[LabeledPoint],
                           model: LogisticRegressionModel) =
    test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

  def main(args: Array[String]) = {
    // Train classifier (Logistic Regression LBFGS and quadfeatures/hashing)
    // Create SparkContext
    val conf = new SparkConf().setAppName("Train Classifier, Compute up-to-minute auPRC")
    val sc = new SparkContext(conf)

    // Load data and hash
    val rawData = sc.textFile("s3n://warshmellow-adtech-dash/data/dac_sample_with_ms.txt")
    rawData.cache()
    val hashed = rawData.map(line => parseTimestampedLineToLabeledPoint(line))
    hashed.cache()
    val timestamps = hashed.map(x => x._1)
    val train = hashed.map(x => x._2)
    timestamps.cache()
    train.cache()

    // Train classifier on hashed data (Logistic Regression LBFGS)
    val model = new LogisticRegressionWithLBFGS().
      setIntercept(true).
      run(train)

    // Save threshold and Clear from model (default is 0.5)
    val threshold = model.getThreshold.getOrElse(0.5)
    model.clearThreshold()

    // Compute area under Precision-Recall curve (auPRC) for each minute
    val trainWithTimestampsAsDates = hashed.map {
      case (t, lp) => (new DateTime(t), lp)
    }
    trainWithTimestampsAsDates.cache()

    val firstDateTime = new DateTime(timestamps.min)
    val lastDateTime = new DateTime(timestamps.max)
    val lowestWholeMin = firstDateTime.minuteOfDay().roundCeilingCopy()
    val highestWholeMin = lastDateTime.minuteOfDay().roundCeilingCopy()

    val numBins = 1000
    val auPRByMin =
      for {
        n <- Stream(0).withFilter { case n => lowestWholeMin + n.minutes <= highestWholeMin }
        upperMin = lowestWholeMin + n.minutes
        test = trainWithTimestampsAsDates.filter {
          case (t, _) => t < upperMin
        }.map {
          case (_, lp) => lp
        }
        scoresAndLabels = predictionAndLabels(test, model)
        bcm = new BinaryClassificationMetrics(scoresAndLabels, numBins)
      } yield bcm.areaUnderPR()
  }
}