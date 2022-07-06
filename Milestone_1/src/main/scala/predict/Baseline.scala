package predict

import org.rogach.scallop._
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.math
import shared.predictions._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default = Some("\t"))
  val num_measurements = opt[Int](default = Some(0))
  val json = opt[String]()
  verify()
}

object Baseline extends App {
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

  var conf = new Conf(args)
  // For these questions, data is collected in a scala Array
  // to not depend on Spark
  println("Loading training data from: " + conf.train())
  val train = load(spark, conf.train(), conf.separator()).collect()

  println("Loading test data from: " + conf.test())
  val test = load(spark, conf.test(), conf.separator()).collect()

  // -------- B.1 --------
  val globalAverage = computeGlobalAverage(train)
  val avgRatingUser1 = computeAvgRatingPerUser(train)(1)
  val avgRatingItem1 = computeAvgRatingPerItem(train)(1)
  val avgDeviationItem1 = computeGlobalDevPerItem(
    train,
    computeAvgRatingPerUser(train)
  )(1)

  var predictionUser1Item1 = baselinePredictor(train)(1, 1)

  // -------- B.2, B.3 --------

  val GlobalAvgMAE = MAE(test, globalAveragePredictor(train))
  val globalAverage_time = (0 to conf.num_measurements()).map(x =>
    timingInMs(() => MAE(test, globalAveragePredictor(train)))
  )

  val UserAvgMAE = MAE(test, avgRatingUsersPredictor(train))
  val user_avg_MAE_time = (0 to conf.num_measurements()).map(x =>
    timingInMs(() => MAE(test, avgRatingUsersPredictor(train)))
  )

  val ItemAvgMAE = MAE(test, avgRatingItemsPredictor(train))
  val item_avg_MAE_time = (0 to conf.num_measurements()).map(x =>
    timingInMs(() => MAE(test, avgRatingItemsPredictor(train)))
  )

  val BaselineMAE = MAE(test, baselinePredictor(train))
  val baseline_MAE_time = (0 to conf.num_measurements()).map(x =>
    timingInMs(() => MAE(test, baselinePredictor(train)))
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
      var answers = ujson.Obj(
        "Meta" -> ujson.Obj(
          "1.Train" -> ujson.Str(conf.train()),
          "2.Test" -> ujson.Str(conf.test()),
          "3.Measurements" -> ujson.Num(conf.num_measurements())
        ),
        "B.1" -> ujson.Obj(
          "1.GlobalAvg" -> globalAverage, // Datatype of answer: Double
          "2.User1Avg" -> avgRatingUser1, // Datatype of answer: Double
          "3.Item1Avg" -> avgRatingItem1, // Datatype of answer: Double
          "4.Item1AvgDev" -> avgDeviationItem1, // Datatype of answer: Double
          "5.PredUser1Item1" -> predictionUser1Item1 // prediction_user_1._4 // Datatype of answer: Double
        ),
        "B.2" -> ujson.Obj(
          "1.GlobalAvgMAE" -> GlobalAvgMAE, // Datatype of answer: Double
          "2.UserAvgMAE" -> UserAvgMAE, // Datatype of answer: Double
          "3.ItemAvgMAE" -> ItemAvgMAE, // Datatype of answer: Double
          "4.BaselineMAE" -> BaselineMAE // Datatype of answer: Double
        ),
        "B.3" -> ujson.Obj(
          "1.GlobalAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(
              mean(globalAverage_time.map(x => x._2))
            ), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(
              std(globalAverage_time.map(x => x._2))
            ) // Datatype of answer: Double
          ),
          "2.UserAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(
              mean(user_avg_MAE_time.map(x => x._2))
            ), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(
              std(user_avg_MAE_time.map(x => x._2))
            ) // Datatype of answer: Double
          ),
          "3.ItemAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(
              mean(item_avg_MAE_time.map(x => x._2))
            ), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(
              std(item_avg_MAE_time.map(x => x._2))
            ) // Datatype of answer: Double
          ),
          "4.Baseline" -> ujson.Obj(
            "average (ms)" -> ujson.Num(
              mean(baseline_MAE_time.map(x => x._2))
            ), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(
              std(baseline_MAE_time.map(x => x._2))
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
