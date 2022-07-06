package test.predict

import org.scalatest._
import funsuite._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import shared.predictions._
import tests.shared.helpers._
import ujson._

class kNNTests extends AnyFunSuite with BeforeAndAfterAll {

  val separator = "\t"
  var spark: org.apache.spark.sql.SparkSession = _

  val train2Path = "data/ml-100k/u2.base"
  val test2Path = "data/ml-100k/u2.test"
  var train2: Array[shared.predictions.Rating] = null
  var test2: Array[shared.predictions.Rating] = null

  var adjustedCosine: Map[Int, Map[Int, Double]] = null

  override def beforeAll: Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    spark = SparkSession
      .builder()
      .master("local[1]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // For these questions, train and test are collected in a scala Array
    // to not depend on Spark
    train2 = load(spark, train2Path, separator).collect()
    test2 = load(spark, test2Path, separator).collect()
  }

  // All the functions definitions for the tests below (and the tests in other suites)
  // should be in a single library, 'src/main/scala/shared  /predictions.scala'.

  // Provide tests to show how to call your code to do the following tasks.
  // Ensure you use the same function calls to produce the JSON outputs in
  // src/main/scala/predict/Baseline.scala.
  // Add assertions with the answer you expect from your code, up to the 4th
  // decimal after the (floating) point, on data/ml-100k/u2.base (as loaded above).
  test("kNN predictor with k=10") {
    // Create predictor on train2

    // Similarity between user 1 and itself
    assert(
      within(
        computeKnnSimilarities(
          train2,
          computeNormalizedDeviation(train2, computeAvgRatingPerUser(train2)),
          10
        )(0)(0),
        0.0,
        0.0001
      )
    )

    // Similarity between user 1 and 864
    assert(
      within(
        computeKnnSimilarities(
          train2,
          computeNormalizedDeviation(train2, computeAvgRatingPerUser(train2)),
          10
        )(0)(863),
        0.24232304952129619,
        0.0001
      )
    )

    // Similarity between user 1 and 886
    assert(
      within(
        computeKnnSimilarities(
          train2,
          computeNormalizedDeviation(train2, computeAvgRatingPerUser(train2)),
          10
        )(0)(885),
        0.0,
        0.0001
      )
    )

    // Prediction user 1 and item 1
    assert(within(knnPredictor(train2, 10)(1, 1), 4.319093503763853, 0.0001))

    // MAE on test2
    assert(
      within(MAE(test2, knnPredictor(train2, 10)), 0.8287277961963556, 0.0001)
    )
  }

  test("kNN Mae") {
    // Compute MAE for k around the baseline MAE

    // Ensure the MAEs are indeed lower/higher than baseline
    assert(
      MAE(test2, knnPredictor(train2, 10)) > MAE(
        test2,
        baselinePredictor(train2)
      )
    )
    assert(
      MAE(test2, knnPredictor(train2, 30)) > MAE(
        test2,
        baselinePredictor(train2)
      )
    )
    assert(
      MAE(test2, knnPredictor(train2, 50)) > MAE(
        test2,
        baselinePredictor(train2)
      )
    )
    assert(
      MAE(test2, knnPredictor(train2, 100)) < MAE(
        test2,
        baselinePredictor(train2)
      )
    )
    assert(
      MAE(test2, knnPredictor(train2, 200)) < MAE(
        test2,
        baselinePredictor(train2)
      )
    )
    assert(
      MAE(test2, knnPredictor(train2, 300)) < MAE(
        test2,
        baselinePredictor(train2)
      )
    )
    assert(
      MAE(test2, knnPredictor(train2, 400)) < MAE(
        test2,
        baselinePredictor(train2)
      )
    )
    assert(
      MAE(test2, knnPredictor(train2, 800)) < MAE(
        test2,
        baselinePredictor(train2)
      )
    )
    assert(
      MAE(test2, knnPredictor(train2, 943)) < MAE(
        test2,
        baselinePredictor(train2)
      )
    )
  }
}
