package distributed

import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import ujson._

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

import org.apache.log4j.Level

import scala.math
import shared.predictions._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default = Some("\t"))
  val master = opt[String](default = Some(""))
  val num_measurements = opt[Int](default = Some(0))
  val json = opt[String]()
  verify()
}

object DistributedBaseline extends App {
  var conf = new Conf(args)

  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = if (conf.master() != "") {
    SparkSession.builder().master(conf.master()).getOrCreate()
  } else {
    SparkSession.builder().getOrCreate()
  }
  spark.sparkContext.setLogLevel("ERROR")

  println("")
  println("******************************************************")

  println("Loading training data from: " + conf.train())
  val train = load(spark, conf.train(), conf.separator())
  println("Loading test data from: " + conf.test())
  val test = load(spark, conf.test(), conf.separator())

  // -------- D.1 --------
  println(s"Running Spark RDD ...")

  val globalAvgRDD = computeGlobalAvgRDD(train)
  val avgRatingUsers = computeAvgRatingPerUsersMapRDD(train)
  val avgRatingItems = computeAvgRatingPerItemMapRDD(train)
  val avgDeviationItems =
    computeGlobalDevPerItemRDD(train, avgRatingUsers)
  val prediction = baselinePredictorRDD(train)(1, 1)

  // -------- D.2 --------
  val BaselineMAE = maeRDD(test, baselinePredictorRDD(train))
  val baselineMaeTime = (0 to conf.num_measurements()).map(x =>
    timingInMs(() => maeRDD(test, baselinePredictorRDD(train)))
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
          "3.Master" -> conf.master(),
          "4.Measurements" -> conf.num_measurements()
        ),
        "D.1" -> ujson.Obj(
          "1.GlobalAvg" -> globalAvgRDD, // Datatype of answer: Double
          "2.User1Avg" -> avgRatingUsers
            .getOrElse(1, globalAvgRDD)
            .toDouble, // Datatype of answer: Double
          "3.Item1Avg" -> avgRatingItems
            .getOrElse(1, globalAvgRDD)
            .toDouble, // Datatype of answer: Double
          "4.Item1AvgDev" -> avgDeviationItems
            .getOrElse(1, 0.0)
            .toDouble, // Datatype of answer: Double
          "5.PredUser1Item1" -> prediction, // Datatype of answer: Double
          "6.Mae" -> BaselineMAE // Datatype of answer: Double
        ),
        "D.2" -> ujson.Obj(
          "1.DistributedBaseline" -> ujson.Obj(
            "average (ms)" -> ujson.Num(
              mean(baselineMaeTime.map(x => x._2))
            ), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(
              std(baselineMaeTime.map(x => x._2))
            ) // Datatype of answer: Double
          )
        )
      )

      val json = ujson.write(answers, 4)
      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json.toString, jsonFile)
    }
  }
  println("")
  spark.close()
}
