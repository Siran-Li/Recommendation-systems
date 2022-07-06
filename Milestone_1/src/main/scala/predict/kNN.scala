package predict

import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import ujson._

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.math
import shared.predictions._

class kNNConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default = Some("\t"))
  val num_measurements = opt[Int](default = Some(0))
  val json = opt[String]()
  verify()
}

object kNN extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession
    .builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  println("")
  println("******************************************************")

  var conf = new PersonalizedConf(args)
  println("Loading training data from: " + conf.train())
  val train = load(spark, conf.train(), conf.separator()).collect()
  println("Loading test data from: " + conf.test())
  val test = load(spark, conf.test(), conf.separator()).collect()

  // -------------- N.1 ------------------
  val knnSimilarities = computeKnnSimilarities(
    train,
    computeNormalizedDeviation(train, computeAvgRatingPerUser(train)),
    10
  )

  val predictionUser1 = knnPredictor(train, 10)(1, 1)

  // --------------- N.3 -------------------
  val measurements = (0 to conf.num_measurements()).map(x =>
    timingInMs(() => {
      MAE(test, knnPredictor(train, 300))
    })
  )

  // Save answers as JSON
  def printToFile(content: String, location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach { f =>
      try {
        f.write(content)
      } finally { f.close }
    }
  conf.json.toOption match {
    case None => ;
    case Some(jsonFile) => {
      val answers = ujson.Obj(
        "Meta" -> ujson.Obj(
          "1.Train" -> conf.train(),
          "2.Test" -> conf.test(),
          "3.Measurements" -> conf.num_measurements()
        ),
        "N.1" -> ujson.Obj(
          "1.k10u1v1" -> knnSimilarities(0)(
            0
          ), // Similarity between user 1 and user 1 (k=10)
          "2.k10u1v864" -> knnSimilarities(0)(
            863
          ), // Similarity between user 1 and user 864 (k=10)
          "3.k10u1v886" -> knnSimilarities(0)(
            885
          ), // Similarity between user 1 and user 886 (k=10)
          "4.PredUser1Item1" -> predictionUser1 // Prediction of item 1 for user 1 (k=10)
        ),
        "N.2" -> ujson.Obj(
          "1.kNN-Mae" -> List(10, 30, 50, 100, 200, 300, 400, 800, 943)
            .map(k =>
              List(
                k,
                MAE(test, knnPredictor(train, k)) // Compute MAE
              )
            )
            .toList
        ),
        "N.3" -> ujson.Obj(
          "1.kNN" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(measurements.map(t => t._2))),
            "stddev (ms)" -> ujson.Num(std(measurements.map(t => t._2)))
          )
        )
      )
      val json = write(answers, 4)

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
