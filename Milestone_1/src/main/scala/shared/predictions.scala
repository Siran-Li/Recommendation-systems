package shared

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

package object predictions {
  case class Rating(user: Int, item: Int, rating: Double)

  // ----------------------- General utils ----------------------------------
  
  /** Computes the time required to run a function given
   *
   *  @param f function to be measured following the signature given
   *  @return a tuple containing the result of the method and the time to run
   * in Ms
   */
  def timingInMs(f: () => Double): (Double, Double) = {
    val start = System.nanoTime()
    val output = f()
    val end = System.nanoTime()
    return (output, (end - start) / 1000000.0)
  }

  /** Compute the mean of a given Seq
   *
   *  @param s Seq whose mean to be computed
   *  @return the mean of the Seq
   */
  def mean(s: Seq[Double]): Double =
    if (s.size > 0) s.reduce(_ + _) / s.length else 0.0

  /** Compute the mean of a given Iterable
   *
   *  @param s Iterable whose mean to be computed
   *  @return the mean of the Iterable
   */
  def mean(s: Iterable[Double]): Double =
    if (s.size > 0) s.reduce(_ + _) / s.size else 0.0
  
  /** Compute the sum of elements within a Seq
   *
   *  @param s Seq whose elements are to be summed
   *  @return the sum of the elements within Seq
   */
  def sum(s: Seq[Double]): Double = s.reduce(_ + _)
  
  /** Compute the sum of elements within a Spark RDD collection
   *
   *  @param s Spark RDD collection whose elements are to be summed
   *  @return the sum of the elements within the collection
   */
  def sum(s: RDD[Double]): Double = s.reduce(_ + _)
  
  /** Compute the sum of elements within an Iterable
   *
   *  @param s Iterable whose elements are to be summed
   *  @return the sum of the elements within Iterable
   */
  def sum(s: Iterable[Double]): Double = s.reduce(_ + _)

  
  /** Compute the stddev of elements within a Seq
   *
   *  @param s Seq given for the computation
   *  @return the stddev of the elements within Seq
   */
  def std(s: Seq[Double]): Double = {
    if (s.size == 0) 0.0
    else {
      val m = mean(s)
      scala.math.sqrt(
        s.map(x => scala.math.pow(m - x, 2)).sum / s.length.toDouble
      )
    }
  }

  /** Converts string value to Int
   * 
   */
  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  /** Loads the data from file
   * 
   * @param spark Current spark session
   * @param path path to the file to be loaded
   * @param sep separator used within the file to be loaded
   * 
   */
  def load(
      spark: org.apache.spark.sql.SparkSession,
      path: String,
      sep: String
  ): org.apache.spark.rdd.RDD[Rating] = {
    val file = spark.sparkContext.textFile(path)
    return file
      .map(l => {
        val cols = l.split(sep).map(_.trim)
        toInt(cols(0)) match {
          case Some(_) =>
            Some(Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble))
          case None => None
        }
      })
      .filter({
        case Some(_) => true
        case None    => false
      })
      .map({
        case Some(x) => x
        case None    => Rating(-1, -1, -1)
      })
  }

  // ---------------------------- B Utils -------------------------------------

  /** Scales the user by the average user rating
   * 
   * @param x the rating given by user
   * @param average the average of the ratings given by users
   * @return the scaled rating
   * 
   */
  def scale(x: Double, average: Double): Double = {
    if (x > average) return 5 - average
    if (x < average) return average - 1

    return 1.0
  }

  /** Computes the normalized deviation from the rating given by user and the
  * average of the ratings given by the user
  * 
  * @param rating the rating given by user
  * @param avgRatingPerUser map containing the average rating for each user ID
  * @return normalized deviation
  * 
  */
  def normalize(rating: Double, avgRatingPerUser: Double): Double = {
    return (rating - avgRatingPerUser) / scale(rating, avgRatingPerUser)
  }

  /** Computes the global average for an array of ratings
   * 
   * @param data: array of all ratings given
   * @return global average rating computed from all the ratings in the input
   *
   */ 
  def computeGlobalAverage(data: Array[Rating]): Double = {
    return mean(data.map({ case r: Rating => r.rating }))
  }

  /** Computes the average rating given by an user in a map
   * 
   * @param data array of all ratings given
   * @return map containing the average rating for each user ID
   *
   */ 
  def computeAvgRatingPerUser(data: Array[Rating]): Map[Int, Double] = {
    return data
      .map({ case r: Rating => (r.user, r.rating) })
      .groupBy(_._1)
      .map(x => { (x._1, mean(x._2.map(v => v._2))) })
  }

  /** Computes the average rating given for an item in a map
   * 
   * @param data array of all ratings given
   * @return a map containg the average rating given for each item ID
   *
   */
  def computeAvgRatingPerItem(data: Array[Rating]): Map[Int, Double] = {
    return data
      .map({ case r: Rating => (r.item, r.rating) })
      .groupBy(_._1)
      .map(x => { (x._1, mean(x._2.map(v => v._2))) })
  }

  /** Normalizes all the ratings for a collection of ratings
   * 
   * @param data array of all ratings given
   * @param avgRatingPerUser map containing the average rating for each user ID
   * @return array of all the ratings given where rating has been normalized
   *
   */
  def computeNormalizedDeviation(
      input: Array[Rating],
      avgRatingPerUser: Map[Int, Double]
  ): Array[(Int, Int, Double)] = {
    return input.map { r =>
      (
        r.user,
        r.item,
        normalize(r.rating, avgRatingPerUser.getOrElse(r.user, 0.0))
      )
    }
  }

  /** Computes the global average deviation for each item in the input data
   * 
   * @param data array of all ratings given
   * @param avgRatingPerUser map containing the average rating for each user ID
   * @return map containing the global average deviation for each item ID
   *
   */
  def computeGlobalDevPerItem(
      input: Array[Rating],
      avgRatingPerUser: Map[Int, Double]
  ): Map[Int, Double] = {

    val normalized_deviation =
      computeNormalizedDeviation(input, avgRatingPerUser)

    return normalized_deviation
      .map { case (u, i, avg_r) => (i, avg_r) }
      .groupBy(r => r._1)
      .mapValues(x => x.map(_._2))
      .mapValues(t => mean(t))
  }


  /** Computes the Baseline rating prediction for a given user and item.
   * 
   * @param user the ID of the user
   * @param item the ID of the item
   * @param globalAverage the global average computed from all existing ratings
   * @param avgRatingPerUser map containing the average rating for each user ID
   * @param globalAvgDevPerItem map containing the global average deviation 
   * for each item ID
   * @return prediction made by the baseline for the given user ID and item ID
   *
   */
  def computePrediction(
      user: Int,
      item: Int,
      globalAverage: Double,
      avgRatingPerUser: Map[Int, Double],
      globalAvgDevPerItem: Map[Int, Double]
  ): Double = {
    var r_u: Double = globalAverage
    var r_i: Double = 0.0

    if (avgRatingPerUser.contains(user)) {
      r_u = avgRatingPerUser(user)
    } else {
      return globalAverage //  https://moodle.epfl.ch/mod/forum/discuss.php?d=74232&parent=146476
    }

    if (globalAvgDevPerItem.contains(item)) {
      r_i = globalAvgDevPerItem(item)
    }
    return r_u + r_i * scale((r_u + r_i), r_u)
  }

  // -------- B.2, B.3 --------

  /** Creates a predictor that predicts the global average for all users and
   * items.
   * 
   * @param train the data used to train the predictor
   * @return predictor that expects two integer values, the user ID and item ID
   * and returns the prediction
   *
   */
  def globalAveragePredictor(train: Array[Rating]): ((Int, Int) => Double) = {
    val globalAvg = computeGlobalAverage(train)
    (u: Int, i: Int) => {
      globalAvg
    }
  }

  /** Computes the MAE for a given predictor
   * 
   * @param test the dataset for which we want to test the predictor's error
   * @param predictor the predictor used to estimate the ratings
   * @return the mean average error
   * 
   */
  def MAE(test: Array[Rating], predictor: (Int, Int) => Double): Double = {
    return mean(test.map({ case r: Rating =>
      scala.math.abs(r.rating - predictor(r.user, r.item))
    }))
  }

  /** Creates a predictor that predicts the average rating given by the user, or
   * global average if user has not given any rating prior to this
   * 
   * @param train the data used to train the predictor
   * @return predictor that expects two integer values, the user ID and item ID
   * and returns the prediction
   *
   */
  def avgRatingUsersPredictor(train: Array[Rating]): ((Int, Int) => Double) = {
    val globalAvg = computeGlobalAverage(train)
    val avgRatingPerUser = computeAvgRatingPerUser(train)
    (u: Int, i: Int) => {
      avgRatingPerUser.getOrElse(u, globalAvg)
    }
  }

  /** Creates a predictor that predicts the average rating given for the item, or
   * 0.0 if the item has not been rated by any user prior to this
   * 
   * @param train the data used to train the predictor
   * @return predictor that expects two integer values, the user ID and item ID
   * and returns the prediction
   *
   */
  def avgRatingItemsPredictor(train: Array[Rating]): ((Int, Int) => Double) = {
    val globalAvg = computeGlobalAverage(train)
    val avgRatingPerItemsMap = computeAvgRatingPerItem(train)
    (u: Int, i: Int) => {
      avgRatingPerItemsMap.getOrElse(i, globalAvg)
    }
  }

  /** Creates a predictor for the baseline method
   * 
   * @param train the data used to train the predictor
   * @return predictor that expects two integer values, the user ID and item ID
   * and returns the prediction
   *
   */
  def baselinePredictor(train: Array[Rating]): ((Int, Int) => Double) = {
    val globalAvg = computeGlobalAverage(train)
    val avgRatingPerUser = computeAvgRatingPerUser(train)
    val globalAvgDevPerItem =
      computeGlobalDevPerItem(train, avgRatingPerUser)
    (u: Int, i: Int) => {
      computePrediction(
        u,
        i,
        globalAvg,
        avgRatingPerUser,
        globalAvgDevPerItem
      )
    }
  }

  // ----------------- D tasks ---------------------------------


  /** Computes the global average for a Spark RDD collection of ratings
   * 
   * @param train: Spark RDD collection of all ratings given
   * @return global average rating computed from all the ratings in the input
   *
   */ 
  def computeGlobalAvgRDD(train: RDD[Rating]): Double = {
    var result = train
      .map({ case r: Rating => (r.rating, 1.0) })
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    return result._1 / result._2
  }

  /** Computes the average rating given by an user from a Spark RDD collection
   * of ratings
   * 
   * @param train: Spark RDD collection of all ratings given
   * @return map containing the average rating for each user ID
   *
   */ 
  def computeAvgRatingPerUsersMapRDD(
      train: RDD[Rating]
  ): scala.collection.Map[Int, Double] = {
    return train
      .map({ case r: Rating => (r.user, (r.rating, 1.0)) })
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map({ case (x, (y, z)) => (x, y / z) })
      .collectAsMap()
  }

  /** Computes the average rating given for an item from a Spark RDD collection
   * of ratings
   * 
   * @param train: Spark RDD collection of all ratings given
   * @return a map containg the average rating given for each item ID
   *
   */
  def computeAvgRatingPerItemMapRDD(
      train: RDD[Rating]
  ): scala.collection.Map[Int, Double] = {
    return train
      .map({ case r: Rating => (r.item, (r.rating, 1.0)) })
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map({ case (x, (y, z)) => (x, y / z) })
      .collectAsMap()
  }

  /** Computes the normalized value from a possible rating
   * 
   * @param rating: the rating given for the item
   * @param avg_r: the average rating given by the user for this item, 0 otherwise
   * @return the normalized deviation for the given rating
   *
   */
  def computeDeviationForItem(
      rating: Double,
      avg_r: Option[Double]
  ): Double = {
    avg_r match {
      case None            => 0.0
      case Some(s: Double) => normalize(rating, s)
    }
  }

  /** Computes the global average deviation for each item in a Spark RDD
   * collection of ratings.
   * 
   * @param train: Spark RDD collection of all ratings given
   * @param avgRatingPerUser map containing the average rating for each user ID
   * @return map containing the global average deviation for each item ID
   *
   */
  def computeGlobalDevPerItemRDD(
      train: RDD[Rating],
      avgRatingPerUsersMap: scala.collection.Map[Int, Double]
  ): scala.collection.Map[Int, Double] = {
    val broadcaster_avgRatingPerUsersMap =
      train.sparkContext.broadcast(avgRatingPerUsersMap)

    return train
      .map({ case r: Rating =>
        (
          r.item,
          (
            computeDeviationForItem(
              r.rating,
              broadcaster_avgRatingPerUsersMap.value.get(r.user)
            ),
            1.0
          )
        )
      })
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map({ case (x, (y, z)) => (x, y / z) })
      .collectAsMap()
  }

  /** Computes the MAE for a given predictor for a Spark RDD collection
   * of ratings
   * 
   * @param test the Spark RDD dataset for which we want to test the predictor's error
   * @param predictor the predictor used to estimate the ratings
   * @return the mean average error
   * 
   */
  def maeRDD(test: RDD[Rating], predictor: (Int, Int) => Double): Double = {
    val result = test
      .map({ case r: Rating =>
        (scala.math.abs(r.rating - predictor(r.user, r.item)), 1)
      })
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2))

    return result._1 / result._2
  }

  /** Creates a predictor that predicts the global average for all users and
   * items from a Spark RDD collection of ratings
   * 
   * @param train: Spark RDD collection of all ratings given
   * @return predictor that expects two integer values, the user ID and item ID
   * and returns the prediction
   *
   */
  def globalAveragePredictorRDD(train: RDD[Rating]): ((Int, Int) => Double) = {
    val globalAvg = computeGlobalAvgRDD(train)
    (u: Int, i: Int) => {
      globalAvg
    }
  }

  /** Creates a predictor that predicts the average rating given by the user, or
   * global average if user has not given any rating prior to this, computed
   * from a Spark RDD collection of ratings
   * 
   * @param train: Spark RDD collection of all ratings given
   * @return predictor that expects two integer values, the user ID and item ID
   * and returns the prediction
   *
   */
  def avgRatingUsersPredictorRDD(train: RDD[Rating]): ((Int, Int) => Double) = {
    val globalAvg = computeGlobalAvgRDD(train)
    val avgRatingPerUsersMap = computeAvgRatingPerUsersMapRDD(train)

    val broadcaster_avgRatingPerUsersMap =
      train.sparkContext.broadcast(avgRatingPerUsersMap)
    (u: Int, i: Int) => {
      broadcaster_avgRatingPerUsersMap.value.getOrElse(u, globalAvg)
    }
  }

  /** Creates a predictor that predicts the average rating given for the item, or
   * 0.0 if the item has not been rated by any user prior to this, computed
   * from a Spark RDD collection of ratings
   * 
   * @param train: Spark RDD collection of all ratings given
   * @return predictor that expects two integer values, the user ID and item ID
   * and returns the prediction
   *
   */
  def avgRatingItemsPredictorRDD(train: RDD[Rating]): ((Int, Int) => Double) = {
    val globalAvg = computeGlobalAvgRDD(train)
    val avg_rating_per_items = computeAvgRatingPerItemMapRDD(train)

    val broadcaster_avgRatingPerUsersMap =
      train.sparkContext.broadcast(avg_rating_per_items)
    (u: Int, i: Int) => {
      broadcaster_avgRatingPerUsersMap.value.getOrElse(i, globalAvg)
    }
  }

  /** Creates a predictor for the baseline method from a Spark RDD collection
   * 
   * @param train: Spark RDD collection of all ratings given for training
   * @return predictor that expects two integer values, the user ID and item ID
   * and returns the prediction
   *
   */
  def baselinePredictorRDD(train: RDD[Rating]): ((Int, Int) => Double) = {
    val globalAvg = computeGlobalAvgRDD(train)
    val avgRatingPerUsersMap = computeAvgRatingPerUsersMapRDD(train)
    val globalAvgDevPerItem = computeGlobalDevPerItemRDD(train, avgRatingPerUsersMap)

    val broadcaster_avgRatingPerUsersMap =
      train.sparkContext.broadcast(avgRatingPerUsersMap)
    val broadcaster_avgDevPerItem =
      train.sparkContext.broadcast(globalAvgDevPerItem)

    (u: Int, i: Int) => {
      if (broadcaster_avgRatingPerUsersMap.value.contains(u)) {
        val r_u = broadcaster_avgRatingPerUsersMap.value.getOrElse(u, globalAvg)
        val r_i = broadcaster_avgDevPerItem.value.getOrElse(i, 0.0)
        r_u + r_i * scale((r_u + r_i), r_u)
      } else {
        globalAvg  //  https://moodle.epfl.ch/mod/forum/discuss.php?d=74232&parent=146476
      }
    }
  }

  // --------------------------- P.2 ------------------------------

  /** Multiplies two matrices
   * 
   * @param m1 matrix in the shape NxB
   * @param m2 matrix in the shape BxM
   * @return the result of the convolution of the two matrices, with shape NxM
   *
   */
  def multiplication(m1: Array[Array[Double]], m2: Array[Array[Double]]) = {
    val res = Array.fill(m1.length, m2(0).length)(0.0)
    for (
      row <- (0 until m1.length).par;
      col <- (0 until m2(0).length).par;
      i <- 0 until m1(0).length
    ) {
      res(row)(col) += m1(row)(i) * m2(i)(col)
    }
    res
  }

  /** Multiplies two matrices, but considers the absolute values.
   * 
   * @param m1 matrix in the shape NxB
   * @param m2 matrix in the shape BxM
   * @return result of the the convolution of absolute values of the two matrices, 
   * with shape NxM
   *
   */
  def multiplication_abs(m1: Array[Array[Double]], m2: Array[Array[Double]]) = {
    val res = Array.fill(m1.length, m2(0).length)(0.0)
    for (
      row <- (0 until m1.length).par;
      col <- (0 until m2(0).length).par;
      i <- 0 until m1(0).length
    ) {
      res(row)(col) += scala.math.abs(m1(row)(i) * m2(i)(col))
    }
    res
  }

  /** Divides a matrix with size NxB by an array with size Nx1
   * 
   * @param m1 matrix in the shape NxB
   * @param v2 array in the shape Nx1
   * @return result of the division, with shape NxB
   *
   */
  def matrix_div_array(m1: Array[Array[Double]], v1: Array[Array[Double]]) = {
    val res = Array.fill(m1.length, m1(0).length)(0.0)
    for (
      row <- (0 until m1.length).par;
      col <- (0 until m1(0).length).par
    ) {
      res(row)(col) = m1(row)(col) / v1(row)(0)
    }
    res
  }

  /** Divides values by two matrices element wise
   * 
   * @param m1 matrix in the shape NxB
   * @param m2 matrix in the shape NxB
   * @return result of the the division, with shape NxB
   *
   */
  def matrix_div_matrix(m1: Array[Array[Double]], m2: Array[Array[Double]]) = {
    val res = Array.fill(m1.length, m1(0).length)(0.0)
    for (
      row <- (0 until m1.length).par;
      col <- (0 until m1(0).length).par
    ) {
      if (m2(row)(col) != 0.0) {
        res(row)(col) = m1(row)(col) / m2(row)(col)
      }
    }
    res
  }

  /** Preprocess the rating by dividing the square root of the summation
   * 
   * @param input array of all ratings given
   * @return array of all the ratings given where rating has been normalized
   * by the square root of the summation of the corresponding set of squared 
   * rating value
   */
  def preprocessing_rating(
      input: Array[(Int, Int, Double)]
  ): Array[(Int, Int, Double)] = {
    val std_rating = input
      .map({ case (r_user, r_item, r_rating) => (r_user, r_rating) })
      .groupBy(_._1)
      .map(x => { (x._1, sum(x._2.map(v => v._2 * v._2))) })
    return input.map {
      case (u, i, r) => {
        (u, i, r / scala.math.sqrt(std_rating.getOrElse(u, 1.0)))
      }
    }
  }

  /** Compute the Adjusted cosine similarities
   * 
   * @param train array of all ratings given
   * @param normalizedDev the result array from function preprocessing_rating
   * @return a similarity matrix with size NxN, where N is the total number of users
   */
  def computeAdjCosineSimilarities(
      train: Array[Rating],
      normalizedDev: Array[(Int, Int, Double)]
  ): Array[Array[Double]] = {
    val preprocessed_rating = preprocessing_rating(normalizedDev)

    val trainUsers = train.map({ case r: Rating => (r.user) }).toList.distinct
    val trainItems = train.map({ case r: Rating => (r.item) }).toList.distinct

    // Convert the collections to a matrix with ratings, the row idx is (user - 1), and the col idx is (item - 1)
    var prerating_matrix = Array.fill(trainUsers.max, trainItems.max)(0.0)

    preprocessed_rating.foreach {
      case (user, item, rating) => {
        prerating_matrix(user - 1)(item - 1) = rating
      }
    }
    return multiplication(prerating_matrix, prerating_matrix.transpose)
  }

/** Compute the user-specific weighted-sum deviation
   * 
   * @param train array of all ratings given
   * @param normalizedDev the result array from function preprocessing_rating
   * @param similarities the similarity matrix 
   * @return a matrix with size NxB, where N is the total number of users,
   * and B is the total number of items
   */
  def computeUserSpecDevPerItem(
      train: Array[Rating],
      normalizedDev: Array[(Int, Int, Double)],
      similarities: Array[Array[Double]]
  ): Array[Array[Double]] = {
    val trainUsers = train.map({ case r: Rating => (r.user) }).toList.distinct
    val trainItems = train.map({ case r: Rating => (r.item) }).toList.distinct

    var normalizedDevMatrix = Array.fill(trainUsers.max, trainItems.max)(0.0)
    // Build the normalizedDev matrix by replacing nonzero values to 1 
    var normalizedDevMatrixExists =
      Array.fill(trainUsers.max, trainItems.max)(0.0)
    normalizedDev.foreach {
      case (user, item, rating) => {
        normalizedDevMatrix(user - 1)(item - 1) = rating
        normalizedDevMatrixExists(user - 1)(item - 1) = 1.0
      }
    }
    // compute the numerator matrix 
    val simRating = multiplication(similarities, normalizedDevMatrix)
    // compute the denominator matrix 
    val denomSim = multiplication_abs(similarities, normalizedDevMatrixExists)
    return matrix_div_matrix(simRating, denomSim)
  }

  /** Computes the Personalized rating prediction for a given user and item.
   * 
   * @param user the ID of the user
   * @param item the ID of the item
   * @param globalAverage the global average computed from all existing ratings
   * @param avgRatingPerUser map containing the average rating for each user ID
   * @param userSpecDevPerItem array containing the user-specific weighted-sum 
  *  deviation for each user ID and item ID
   * @return prediction made by the cosine similarities for the given user ID and item ID
   *
   */
  def computePersonalizedPrediction(
      user: Int,
      item: Int,
      globalAvg: Double,
      avgRatingPerUsers: Map[Int, Double],
      userSpecDevPerItem: Array[Array[Double]]
  ): Double = {
    var r_u: Double = globalAvg
    var r_i: Double = 0.0

    if (avgRatingPerUsers.contains(user)) {
      r_u = avgRatingPerUsers(user)

      if (item < userSpecDevPerItem(0).length) {
        r_i = userSpecDevPerItem(user - 1)(item - 1)
      }
    } else {
      return globalAvg //  https://moodle.epfl.ch/mod/forum/discuss.php?d=74232&parent=146476
    }

    return r_u + r_i * scale((r_u + r_i), r_u)
  }

  /** Creates a predictor for the personalized method
   * 
   * @param train the data used to train the predictor
   * @return predictor that expects two integer values, the user ID and item ID
   * and returns the prediction
   *
   */
  def personalizedPredictor(train: Array[Rating]): ((Int, Int) => Double) = {
    val globalAvg = computeGlobalAverage(train)
    val avgRatingPerUser = computeAvgRatingPerUser(train)
    val normalizedDev = computeNormalizedDeviation(train, avgRatingPerUser)
    val similarities = computeAdjCosineSimilarities(train, normalizedDev)

    val userSpecDevPerItem =
      computeUserSpecDevPerItem(train, normalizedDev, similarities)

    (u: Int, i: Int) => {
      computePersonalizedPrediction(
        u,
        i,
        globalAvg,
        avgRatingPerUser,
        userSpecDevPerItem
      )
    }
  }

  // ------------------------------- P.3 ----------------------------

  /** Computes the denominator for computing the Jaccard similarity by matrix multiplication
   * 
   * @param m1 matrix in the shape NxB
   * @param m2 matrix in the shape BxM
   * @return result of the multiplication, with shape NxM
   *
   */
  def computeJaccardDenominator(m1: Array[Array[Double]], m2: Array[Array[Double]]) = {
    val res = Array.fill(m1.length, m2(0).length)(0.0)
    for (
      row <- (0 until m1.length).par;
      col <- (0 until m2(0).length).par;
      i <- 0 until m1(0).length
    ) {
      res(row)(col) += (m1(row)(i)).max(m2(i)(col))
    }
    res
  }

  /** Compute the Jaccard similarities
   * 
   * @param train array of all ratings given
   * @return a similarity matrix with size NxN, where N is the total number of users
   */
  def computeJmSimilarities(train: Array[Rating]): Array[Array[Double]] = {
    val trainUsers = train.map({ case r: Rating => (r.user) }).toList.distinct
    val trainItems = train.map({ case r: Rating => (r.item) }).toList.distinct
    // construct a matrix using train, where row represents (user - 1) and 
    // column represents (item - 1) and replace nonzero values to 1
    var trainMatrix = Array.ofDim[Double](trainUsers.max, trainItems.max)

    (0 to train.length - 1).foreach { x =>
      {
        val r_user = train(x).user - 1
        val r_item = train(x).item - 1
        trainMatrix(r_user)(r_item) = 1.0
      }
    }
    // transpose trainMatrix
    val trainMatrix_T = trainMatrix.transpose
    // compute the numerator matrix
    val jmNumerator = multiplication(trainMatrix, trainMatrix_T)
    // compute the denominator matrix
    var jmDenom = computeJaccardDenominator(trainMatrix, trainMatrix_T)
    return matrix_div_matrix(jmNumerator, jmDenom)
  }

  /** Creates a predictor for the personalized method using jaccard similarity
   * 
   * @param train the data used to train the predictor
   * @return predictor that expects two integer values, the user ID and item ID
   * and returns the prediction
   *
   */
  def personalizedJmPredictor(train: Array[Rating]): ((Int, Int) => Double) = {
    val globalAvg = computeGlobalAverage(train)
    val avgRatingPerUser = computeAvgRatingPerUser(train)
    val normalizedDev = computeNormalizedDeviation(train, avgRatingPerUser)
    val similarities = computeJmSimilarities(train)

    val userSpecDevPerItem =
      computeUserSpecDevPerItem(train, normalizedDev, similarities)

    (u: Int, i: Int) => {
      computePersonalizedPrediction(
        u,
        i,
        globalAvg,
        avgRatingPerUser,
        userSpecDevPerItem
      )
    }
  }

  // -------------------- N -----------------------------

  /** Computes the similiarities matrix from the prior ratings only for K
   * closest neighbours by similarity, otherwise 0.
   * 
   * @param train array containing all ratings given
   * @param normalizedDev normalized ratings for each rating given
   * @param k number of neighbours to be considered
   * @return matrix containing the adjusted cosine similarity between any two
   * users that gave ratings before
   */ 
  def computeKnnSimilarities(
      train: Array[Rating],
      normalizedDev: Array[(Int, Int, Double)],
      k: Int
  ): Array[Array[Double]] = {

    val train_users = train.map({ case r: Rating => (r.user) }).toList.distinct
    val train_items = train.map({ case r: Rating => (r.item) }).toList.distinct

    val preprocessed_rating = preprocessing_rating(normalizedDev)
    // construct a matrix using preprocessed_rating, where row represents (user - 1) and 
    // column represents (item - 1) and the value is the corresponding rating
    var prerating_matrix = Array.fill(train_users.max, train_items.max)(0.0)
    preprocessed_rating.foreach {
      case (user, item, rating) => {
        prerating_matrix(user - 1)(item - 1) = rating
      }
    }

    val similarities =
      multiplication(prerating_matrix, prerating_matrix.transpose)
    // change the similarity matrix to the collections array
    val simList = for {
      row <- 0 until similarities.length;
      col <- 0 until similarities(0).length
      if similarities(row)(col) != 0.0 && row != col
    } yield (row, col, similarities(row)(col))
    // select the highest k similarities
    val filteredSimList = simList
      .groupBy(_._1)
      .mapValues(x => x.toSeq.sortWith(_._3 > _._3).take(k))

    val knnSimilarities = Array.fill(train_users.max, train_users.max)(0.0)
    filteredSimList.foreach {
      case (u: Int, simListPerUser: Seq[(Int, Int, Double)]) => {
        simListPerUser.foreach {
          case (user1, user2, sim) => {
            knnSimilarities(user1)(user2) = sim
          }
        }
      }
    }

    return knnSimilarities
  }

  /** Creates a predictor for the KNN-based prediction method 
   * 
   * @param train the data used to train the predictor
   * @return predictor that expects two integer values, the user ID and item ID
   * and returns the prediction
   *
   */
  def knnPredictor(train: Array[Rating], k: Int): ((Int, Int) => Double) = {
    val globalAvg = computeGlobalAverage(train)
    val avgRatingPerUsers = computeAvgRatingPerUser(train)
    val normalizedDev = computeNormalizedDeviation(train, avgRatingPerUsers)
    val knnSimilarities = computeKnnSimilarities(train, normalizedDev, k)
    val userSpecDevPerItem =
      computeUserSpecDevPerItem(train, normalizedDev, knnSimilarities)

    (u: Int, i: Int) => {
      computePersonalizedPrediction(
        u,
        i,
        globalAvg,
        avgRatingPerUsers,
        userSpecDevPerItem
      )
    }
  }

  // -------------------- R -------------------------

  /** Retrieves top N recommendations for a given user by using a predictor
   * of choice
   * 
   * @param data array containing all ratings given
   * @param movieNames map containing the name of each of the movie by ID
   * @param user the ID of the user we would like to compute the recommendation
   * @param n the number of recommendations to be generated
   * @param predictor the predictor to be used to compute the recommendation
   * @return a list of recommendations containing the ID, title and the 
   * predicted score
   * 
   */
  def getTopNRecommendationsForUser(
      data: Array[Rating],
      movieNames: Map[Int, String],
      user: Int,
      n: Int,
      predictor: (Int, Int) => Double
  ): List[(Int, String, Double)] = {
    val totalUniqueItems =
      data.map({ case r: Rating => (r.item) }).toList.distinct
    val predictionForUsers = for {
      item <- (1 until totalUniqueItems.max).par
    } yield (item, predictor(user, item))

    val predictionWithName = for {
      (r_item, r_rating) <- predictionForUsers
      (n_item, n_title) <- movieNames
      if r_item == n_item
    } yield (r_item, n_title, r_rating)

    return predictionWithName.toList
      .sortWith(_._1 < _._1)
      .sortWith(_._3 > _._3)
      .take(n)
  }
}
