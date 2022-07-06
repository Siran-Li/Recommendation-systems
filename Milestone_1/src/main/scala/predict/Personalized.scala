package predict

import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import ujson._

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.math
import shared.predictions._

class PersonalizedConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default = Some("\t"))
  val num_measurements = opt[Int](default = Some(0))
  val json = opt[String]()
  verify()
}

object Personalized extends App {
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

  // -------- P.1 --------
  val predUser11 = baselinePredictor(train)(1, 1)
  val baselineMAE = MAE(test, baselinePredictor(train))

  // -------- P.2 --------
  val simUser1User2 = computeAdjCosineSimilarities(
    train,
    computeNormalizedDeviation(train, computeAvgRatingPerUser(train))
  )(0)(1)
  val simPredUser11 = personalizedPredictor(train)(1, 1)
  val adjustedCosineMAE = MAE(test, personalizedPredictor(train))

  // -------- P.3 --------
  val jmSimUser1User2 = computeJmSimilarities(train)(0)(1)
  val jmPredictionUser11 = personalizedJmPredictor(train)(1, 1)
  val jaccardPersonalizedMAE = MAE(test, personalizedJmPredictor(train))

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
          "1.Train" -> ujson.Str(conf.train()),
          "2.Test" -> ujson.Str(conf.test()),
          "3.Measurements" -> ujson.Num(conf.num_measurements())
        ),
        "P.1" -> ujson.Obj(
          "1.PredUser1Item1" -> predUser11, // Prediction of item 1 for user 1 (similarity 1 between users)
          "2.OnesMAE" -> baselineMAE // MAE when using similarities of 1 between all users
        ),
        "P.2" -> ujson.Obj(
          "1.AdjustedCosineUser1User2" -> simUser1User2, // Similarity between user 1 and user 2 (adjusted Cosine)
          "2.PredUser1Item1" -> simPredUser11, // Prediction item 1 for user 1 (adjusted cosine)
          "3.AdjustedCosineMAE" -> adjustedCosineMAE // MAE when using adjusted cosine similarity
        ),
        "P.3" -> ujson.Obj(
          "1.JaccardUser1User2" -> jmSimUser1User2, // Similarity between user 1 and user 2 (jaccard similarity)
          "2.PredUser1Item1" -> jmPredictionUser11, // Prediction item 1 for user 1 (jaccard)
          "3.JaccardPersonalizedMAE" -> jaccardPersonalizedMAE // MAE when using jaccard similarity
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
