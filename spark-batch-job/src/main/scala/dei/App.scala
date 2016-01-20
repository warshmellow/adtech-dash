package dei

import com.github.nscala_time.time.Imports._
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

object App {
  def main(args: Array[String]) = {
    // Train classifier (Logistic Regression LBFGS and quadfeatures/hashing)
    // Create SparkContext
    val conf = new SparkConf().setAppName("Train Classifier, Compute up-to-minute auPRC")
    val sc = new SparkContext(conf)

    // AWS config
    val path = System.getenv("HOME") + "/.ssh/aws-hadoop-conf.xml"
    sc.hadoopConfiguration.addResource(new java.io.File(path).toURI.toURL)

    // Load data and hash
    val rawData = sc.textFile("s3n://warshmellow-adtech-dash/data/trainTenPercentWithFakeMS.txt")
    // rawData.cache()
    val hashed = rawData.map(line => ClassifierPreProcessing.
      parseTimestampedLineToLabeledPoint(line))
    hashed.cache()
    val timestamps = hashed.map(x => x._1)
    val train = hashed.map(x => x._2)
    timestamps.cache()
    train.cache()

    // Train classifier on hashed data (Logistic Regression LBFGS)
    val model = new LogisticRegressionWithLBFGS().
      setIntercept(true).
      setValidateData(false).
      run(train)

    // Get classifier retrain time
    val classifierLastRetrained = DateTime.now.getMillis

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

    // val numBins = 1000
    val auPRByMin =
      for {
        n <- Stream.from(0).takeWhile(n => lowestWholeMin + n.minutes <= highestWholeMin)
        upperMin = lowestWholeMin + n.minutes
        test = trainWithTimestampsAsDates.filter {
          case (t, _) => t < upperMin
        }.map {
          case (_, lp) => lp
        }
        scoresAndLabels = ClassifierPreProcessing.predictionAndLabels(test, model)
        // bcm = new BinaryClassificationMetrics(scoresAndLabels, numBins)
        bcm = new BinaryClassificationMetrics(scoresAndLabels)
      } yield (upperMin.getMillis, bcm.areaUnderPR())

    val eagerAuPRByMin = auPRByMin.toList

    // Load HBase config and create connection
    val hBaseConfig = HBaseConfiguration.create
    hBaseConfig.set("zookeeper.znode.parent","/hbase-unsecure")
    val connection = ConnectionFactory.createConnection(hBaseConfig)
    val admin = connection.getAdmin
    val tableName = "metrics"
    val columnFamily = "d"

    // Drop "metrics" table if exists
    if (admin.isTableAvailable(TableName.valueOf(tableName))) {
      HBaseDAO.drop(connection.getTable(TableName.valueOf(tableName)), admin)
    }

    // Create new table "metrics"
    val descriptor = new HTableDescriptor(TableName.valueOf(tableName))
    descriptor.addFamily(new HColumnDescriptor(Bytes.toBytes(columnFamily)))
    admin.createTable(descriptor)
    val table = connection.getTable(TableName.valueOf(tableName))

    // Put into table
    eagerAuPRByMin.foreach {
      case (timestamp, auPRC) =>
        val bundle = ClassifierMetricsBundle(
          timestamp, auPRC, classifierLastRetrained
        )
        HBaseDAO.put(table, bundle)
    }

    // Close connection
    connection.close()
  }
}