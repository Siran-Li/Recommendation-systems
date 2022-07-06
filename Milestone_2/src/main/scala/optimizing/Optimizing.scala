import org.rogach.scallop._
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import ujson._
import shared.predictions._

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

package scaling {

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val json = opt[String]()
  val users = opt[Int]()
  val movies = opt[Int]()
  val separator = opt[String](default=Some("\t"))
  val master = opt[String](default=Some("local[1]"))
  val num_measurements = opt[Int](default=Some(1))
  val k = opt[Int](default=Some(10))
  verify()
}

object Optimizing extends App {
    var conf = new Conf(args)
    // conf object is not serializable, extract values that
    // will be serialized with the parallelize implementations
    val conf_users = conf.users()
    val conf_movies = conf.movies()
    val conf_k = conf.k()
    
    // Remove these lines if encountering/debugging Spark
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = conf.master.toOption match {
      case None => SparkSession.builder().getOrCreate();
      case Some(master) => SparkSession.builder().master(master).getOrCreate();
    }
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    println("Loading training data from: " + conf.train())
    val train = loadSpark(sc, conf.train(), conf.separator(), conf.users(), conf.movies())
    val test = loadSpark(sc, conf.test(), conf.separator(), conf.users(), conf.movies())

    val measurements = (1 to conf.num_measurements()).map(x => timingInMs(() => {
        MAE(test, knnPredictor(train, conf_k))
    }))
    val timings = measurements.map(t => t._2)
    val mae = measurements(0)._1

    
    // // -------------- BR.1 ------------------

    val knnSimilarities = computeKnnSimilarities(
      computeNormalizedDeviation(train, computeAvgRatingPerUser(train, train.mapActiveValues(x => if (x != 0.0) 1.0 else 0.0))),
      conf_k
    )

    val predictor = knnPredictor(train, conf_k)
    val PredUser1Item1 = predictor(1, 1)
    val PredUser327Item2 = predictor(327, 2)

    // Save answers as JSON
    def printToFile(content: String,
                    location: String = "./answers.json") =
      Some(new java.io.PrintWriter(location)).foreach{
        f => try{
          f.write(content)
        } finally{ f.close }
    }
    conf.json.toOption match {
      case None => ;
      case Some(jsonFile) => {
        val answers = ujson.Obj(
          "Meta" -> ujson.Obj(
            "train" -> ujson.Str(conf.train()),
            "test" -> ujson.Str(conf.test()),
            "users" -> ujson.Num(conf.users()),
            "movies" -> ujson.Num(conf.movies()),
            "master" -> ujson.Str(conf.master()),
            "num_measurements" -> ujson.Num(conf.num_measurements())
          ),
          "BR.1" -> ujson.Obj(
            "1.k10u1v1" -> knnSimilarities(0,0),
            "2.k10u1v864" -> knnSimilarities(0,863),
            "3.k10u1v886" -> knnSimilarities(0,885),
            "4.PredUser1Item1" -> PredUser1Item1,
            "5.PredUser327Item2" -> PredUser327Item2,
            "6.Mae" -> mae
          ),
          "BR.2" ->  ujson.Obj( 
            "average (ms)" -> ujson.Num(mean(timings)),
            "stddev (ms)" -> ujson.Num(std(timings))
          )
        )

        val json = write(answers, 4)

        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
  }
}